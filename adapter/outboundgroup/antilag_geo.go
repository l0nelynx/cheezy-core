package outboundgroup

import (
	"bytes"
	"compress/gzip"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/bart"
	"github.com/metacubex/bbolt"
	corehttp "github.com/metacubex/mihomo/component/http"
	"github.com/metacubex/mihomo/component/mmdb"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/oschwald/maxminddb-golang"
)

// Official https://ip-ranges.amazonaws.com/ip-ranges.json, compressed for offline startup.
//
//go:embed aws-ip-ranges.json.gz
var bundledAWS []byte

// The updater overwrites these files in place. Share immutable heap snapshots
// across groups so a lookup cannot pin the file or race an mmap being closed.
type localGeoSnapshot struct {
	mu     sync.Mutex
	path   string
	file   os.FileInfo
	reader *maxminddb.Reader
}

var countrySnapshot, asnSnapshot localGeoSnapshot

func (s *localGeoSnapshot) load(path string) *maxminddb.Reader {
	s.mu.Lock()
	defer s.mu.Unlock()
	stat, err := os.Stat(path)
	if err != nil {
		return nil
	}
	if s.path == path && s.file != nil && os.SameFile(stat, s.file) && stat.Size() == s.file.Size() && stat.ModTime() == s.file.ModTime() {
		return s.reader
	}
	data, err := os.ReadFile(path)
	if err == nil {
		reader, e := maxminddb.FromBytes(data)
		if e == nil {
			s.path, s.file, s.reader = path, stat, reader
			return reader
		}
	}
	if s.path == path {
		return s.reader
	} // retry after an incomplete replacement
	return nil
}

func (g *AntiLag) loadLocalGeo() {
	var country mmdb.IPReader
	if r := countrySnapshot.load(g.countryPath); r != nil {
		country = mmdb.NewIPReader(r)
	}
	asn := asnSnapshot.load(g.asnPath)
	g.mu.Lock()
	g.country, g.asn = country, asn
	g.mu.Unlock()
}

type awsPrefix struct {
	IPv4    string `json:"ip_prefix"`
	IPv6    string `json:"ipv6_prefix"`
	Service string `json:"service"`
	Region  string `json:"region"`
	Border  string `json:"network_border_group"`
}
type awsTable struct {
	regions bart.Table[string]
	date    time.Time
}

func readAWS(data []byte) (*awsTable, error) {
	var feed struct {
		Date string      `json:"createDate"`
		IPv4 []awsPrefix `json:"prefixes"`
		IPv6 []awsPrefix `json:"ipv6_prefixes"`
	}
	if err := json.Unmarshal(data, &feed); err != nil {
		return nil, err
	}
	stamp, err := time.Parse("2006-01-02-15-04-05", feed.Date)
	if err != nil || len(feed.IPv4) == 0 || len(feed.IPv6) == 0 {
		return nil, errors.New("incomplete AWS IP ranges")
	}
	t := &awsTable{date: stamp}
	unique := make(map[netip.Prefix]string)
	for _, r := range append(feed.IPv4, feed.IPv6...) {
		cidr := r.IPv4
		if cidr == "" {
			cidr = r.IPv6
		}
		p, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, err
		}
		region := r.Region
		if r.Border != "" && r.Border != r.Region {
			region = r.Border
		}
		if region == "" {
			return nil, fmt.Errorf("no AWS region for %s", p)
		}
		if r.Region == "GLOBAL" || region == "GLOBAL" || r.Service == "GLOBALACCELERATOR" || r.Service == "CLOUDFRONT" {
			region = "global"
		}
		if old, ok := unique[p]; ok && old != region {
			region = "global"
		}
		unique[p] = region
	}
	for p, r := range unique {
		t.regions.Insert(p, r)
	}
	return t, nil
}

var cloudRegions struct {
	once    sync.Once
	current atomic.Pointer[awsTable]
	refresh sync.Mutex
	next    time.Time
}

func loadBundledAWS() {
	cloudRegions.once.Do(func() {
		reader, err := gzip.NewReader(bytes.NewReader(bundledAWS))
		if err != nil {
			panic(err)
		}
		defer reader.Close()
		raw, err := io.ReadAll(reader)
		if err != nil {
			panic(err)
		}
		table, err := readAWS(raw)
		if err != nil {
			panic(err)
		}
		cloudRegions.current.Store(table)
	})
}

// Run on a background worker. Keep the last valid snapshot on all errors.
func refreshAWS(ctx context.Context, db *bbolt.DB) {
	if !cloudRegions.refresh.TryLock() {
		return
	}
	defer cloudRegions.refresh.Unlock()
	if time.Now().Before(cloudRegions.next) {
		return
	}
	const bucket = "antilag_aws_v1"
	if cloudRegions.next.IsZero() && db != nil {
		_ = db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return nil
			}
			if t, e := readAWS(b.Get([]byte("feed"))); e == nil && t.date.After(cloudRegions.current.Load().date) {
				cloudRegions.current.Store(t)
			}
			return nil
		})
	}
	cloudRegions.next = time.Now().Add(time.Hour)
	if since := time.Since(cloudRegions.current.Load().date); since >= 0 && since < 24*time.Hour {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	res, err := corehttp.HttpRequest(ctx, "https://ip-ranges.amazonaws.com/ip-ranges.json", "GET", nil, nil)
	if err != nil {
		return
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return
	}
	const maximum = 8 << 20
	raw, err := io.ReadAll(io.LimitReader(res.Body, maximum+1))
	if err != nil || len(raw) > maximum {
		return
	}
	table, err := readAWS(raw)
	if err != nil {
		return
	}
	cloudRegions.next = time.Now().Add(24 * time.Hour)
	if !table.date.After(cloudRegions.current.Load().date) {
		return
	}
	cloudRegions.current.Store(table)
	if db != nil {
		err = db.Update(func(tx *bbolt.Tx) error {
			b, e := tx.CreateBucketIfNotExists([]byte(bucket))
			if e != nil {
				return e
			}
			return b.Put([]byte("feed"), raw)
		})
		if err != nil {
			log.Warnln("[AntiLag] saving AWS ranges: %v", err)
		}
	}
}

func countryCode(value string) string {
	s := strings.ToUpper(value)
	if len(s) != 2 || s[0] < 'A' || s[0] > 'Z' || s[1] < 'A' || s[1] > 'Z' || s == "ZZ" {
		return ""
	}
	return s
}

// A nested selector may change its exit independently of this group.
func (g *AntiLag) probeExit(p C.Proxy) {
	if _, nested := p.Adapter().(ProxyGroup); !nested {
		g.exits.MaybeProbe(g.ctx, p)
	}
}

func (g *AntiLag) enrichGeo(m *C.Metadata) {
	m.DestinationRegion, m.GeoSource, m.DestinationCountry = "unknown", "unknown", ""
	ip := m.DstIP.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.DstIPASN == "" && g.asn != nil {
		asn, _ := (mmdb.ASNReader{Reader: g.asn}).LookupASN(ip.AsSlice())
		if asn != "0" {
			m.DstIPASN = asn
		}
	}
	if table := cloudRegions.current.Load(); table != nil {
		if region, ok := table.regions.Lookup(ip); ok {
			m.DestinationRegion, m.GeoSource = region, "aws"
			if region == "global" {
				return
			}
		}
	}
	var codes []string
	if g.country.Reader != nil {
		codes = g.country.LookupCode(ip.AsSlice())
	}
	for _, code := range codes {
		if cc := countryCode(code); cc != "" {
			m.DestinationCountry = cc
			if m.GeoSource == "unknown" {
				m.DestinationRegion, m.GeoSource = cc, "mmdb"
			}
			return
		}
	}
}
