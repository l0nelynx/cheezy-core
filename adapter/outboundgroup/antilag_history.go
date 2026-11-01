package outboundgroup

import (
	"encoding/json"
	"math"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/bbolt"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

const (
	antiDial = iota
	antiReply
	antiFailure
	antiTTL = 7 * 24 * time.Hour
)

type antiRecord struct {
	Stalls                   *stallHistory `json:",omitempty"`
	Seen                     time.Time
	LastFailure              time.Time
	LastReply                time.Time
	Selected                 time.Time
	Failures, Opens, Replies int
	FirstResponseMS          float64
	ReplyMass                float64
	Starts                   uint64
	CadenceWindows           int
	CadenceMS, CadenceMass   float64
	LastCadence              time.Time
}
type antiHistory struct {
	mu      sync.Mutex
	db      *bbolt.DB
	key     []byte
	records map[string]antiRecord
	dirty   bool
}
type antiHistoryEvidence struct {
	Source          string    `json:"source"`
	Opens           int       `json:"opens"`
	Replies         int       `json:"replies"`
	LastReply       time.Time `json:"lastReplyAt"`
	FirstResponseMS float64   `json:"meanFirstResponseMs"`
	Confidence      float64   `json:"weight"`
}

// Effective sample counts decay even across restarts. Earlier database
// versions only had a lifetime count; conservatively seed one observation.
func agedMass(mass float64, last, now time.Time) float64 {
	if last.IsZero() {
		return 0
	}
	if mass == 0 {
		mass = 1
	}
	age := math.Max(0, now.Sub(last).Hours())
	return math.Min(16, mass) * math.Exp2(-age/12)
}

func newAntiHistory(db *bbolt.DB, profile, name string) *antiHistory {
	h := &antiHistory{db: db, key: []byte(profile + "\x00" + name), records: map[string]antiRecord{}}
	if db != nil {
		err := db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("antilag_v1"))
			if b == nil {
				return nil
			}
			raw := b.Get(h.key)
			if raw == nil {
				return nil
			}
			return json.Unmarshal(raw, &h.records)
		})
		if err != nil || h.records == nil {
			h.records = map[string]antiRecord{}
		}
	}
	for k, r := range h.records {
		if time.Since(r.Seen) > antiTTL {
			delete(h.records, k)
			h.dirty = true
		}
	}
	return h
}
func (h *antiHistory) flush() {
	h.mu.Lock()
	if !h.dirty || h.db == nil {
		h.mu.Unlock()
		return
	}
	data, err := json.Marshal(h.records)
	h.dirty = false
	h.mu.Unlock()
	if err == nil {
		err = h.db.Update(func(tx *bbolt.Tx) error {
			b, e := tx.CreateBucketIfNotExists([]byte("antilag_v1"))
			if e != nil {
				return e
			}
			return b.Put(h.key, data)
		})
	}
	if err != nil {
		h.mu.Lock()
		h.dirty = true
		h.mu.Unlock()
		log.Warnln("[AntiLag] saving route history: %v", err)
	}
}
func antiKeys(m *C.Metadata, node string) []string {
	ip := m.DstIP.Unmap()
	family := "unknown"
	bits := 64
	if ip.Is4() {
		family = "4"
		bits = 24
	} else if ip.Is6() {
		family = "6"
	}
	prefix := m.NetWork.String() + "|" + family + "|" + node + "|"
	host := ip.String()
	if m.NetWork == C.TCP && m.Host != "" {
		host = strings.ToLower(m.Host)
	}
	keys := []string{prefix + "host:" + host + ":" + strconv.Itoa(int(m.DstPort))}
	service := m.Process
	if service == "" {
		service = m.WildcardTarget
	}
	if service == "" {
		service = m.Host
	}
	if ip.IsValid() && service != "" {
		p := netip.PrefixFrom(ip, bits).Masked()
		keys = append(keys, prefix+"net:"+service+"|"+p.String())
		if fields := strings.Fields(m.DstIPASN); len(fields) > 0 && fields[0] != "unknown" {
			keys = append(keys, prefix+"asn:"+service+"|"+strings.TrimPrefix(fields[0], "AS"))
		}
	}
	return keys
}
func (h *antiHistory) event(m *C.Metadata, node string, kind int, err error, d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	for _, k := range antiKeys(m, node) {
		r := h.records[k]
		if now.Sub(r.Seen) > antiTTL {
			r = antiRecord{}
		}
		r.Seen = now
		switch {
		case err != nil || kind == antiFailure:
			r.Failures++
			r.LastFailure = now
		case kind == antiDial:
			r.Opens++
		case kind == antiReply:
			mass := agedMass(r.ReplyMass, r.LastReply, now)
			r.Replies++
			r.LastReply = now
			r.Failures = 0
			value := float64(d) / 1e6
			r.FirstResponseMS = (mass*r.FirstResponseMS + value) / (mass + 1)
			r.ReplyMass = mass + 1
		}
		h.records[k] = r
	}
	h.trim()
	h.dirty = true
}

// Called with h.mu held after updating the records map.
func (h *antiHistory) trim() {
	for len(h.records) > 4096 {
		var oldest string
		var stamp time.Time
		for k, r := range h.records {
			if stamp.IsZero() || r.Seen.Before(stamp) {
				oldest, stamp = k, r.Seen
			}
		}
		delete(h.records, oldest)
	}
}
func (h *antiHistory) evaluate(m *C.Metadata, node string) antiCandidate {
	c := antiCandidate{reason: "health-check"}
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, k := range antiKeys(m, node) {
		r, ok := h.records[k]
		if !ok || time.Since(r.Seen) > antiTTL {
			continue
		}
		if i == 0 {
			c.stalls = r.Stalls.evidence(now)
			c.starts = r.Starts
			c.lastSelected = r.Selected
			c.failed = r.Failures >= 2 && now.Sub(r.LastFailure) < time.Minute
			if r.Failures > 0 {
				c.failurePenalty = 40 * math.Min(3, float64(r.Failures)) * math.Exp2(-math.Max(0, now.Sub(r.LastFailure).Minutes())/5)
			}
			if r.CadenceWindows > 0 && now.Sub(r.LastCadence) < antiTTL {
				mass := agedMass(r.CadenceMass, r.LastCadence, now)
				c.cadenceWeight = mass / (mass + 4)
				c.cadenceMS = r.CadenceMS
			}
		}
		if r.Replies > 0 && time.Since(r.LastReply) <= antiTTL {
			mass := agedMass(r.ReplyMass, r.LastReply, now)
			weight := mass / (mass + 4) * [...]float64{1, 0.45, 0.2}[i]
			if weight <= c.confidence {
				continue
			}
			c.confidence = weight
			c.tier = 3 - i
			c.penalty = r.FirstResponseMS
			c.reason = []string{"destination-history", "prefix-history", "asn-history"}[i]
			c.historyEvidence = &antiHistoryEvidence{c.reason, r.Opens, r.Replies, r.LastReply, r.FirstResponseMS, weight}
		}
	}
	return c
}

func (h *antiHistory) selected(m *C.Metadata, name string, initial bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, key := range antiKeys(m, name) {
		if record, ok := h.records[key]; ok {
			record.Selected = time.Now()
			if initial && i == 0 {
				record.Starts++
			}
			h.records[key] = record
			h.dirty = true
		}
	}
}
