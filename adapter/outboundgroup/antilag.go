package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"sort"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/mmdb"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
	"github.com/oschwald/maxminddb-golang"
)

type AntiLagOption struct {
	ShadowModel string          `group:"shadow-model,omitempty"`
	Targets     []AntiLagTarget `group:"probe-targets,omitempty"`
}
type AntiLagTarget struct {
	Address  string         `group:"address"`
	CIDR     string         `group:"cidr,omitempty"`
	prefix   netip.Prefix   `group:"-"`
	endpoint netip.AddrPort `group:"-"`
}
type AntiLag struct {
	*GroupBase
	shadow               *antiShadowModel
	shadowStatus         string
	countryPath, asnPath string
	url                  string
	disableUDP           bool
	targets              []AntiLagTarget
	ctx                  context.Context
	cancel               context.CancelFunc
	mu                   sync.Mutex
	flows                map[string]*antiFlow
	parents              map[*antiSocket]bool
	history              *antiHistory
	asn                  *maxminddb.Reader
	country              mmdb.IPReader
	exits                *smart.ExitWatcher
	measurements         map[netip.AddrPort]*antiRegion
	once                 sync.Once
	workers              sync.WaitGroup
	regionalBusy         bool
}

func NewAntiLag(o GroupCommonOption, opt AntiLagOption, empty C.Proxy, providers []P.ProxyProvider) (*AntiLag, error) {
	seen := map[string]bool{}
	for i := range opt.Targets {
		t := &opt.Targets[i]
		var err error
		t.endpoint, err = netip.ParseAddrPort(t.Address)
		if err != nil || t.endpoint.Port() == 0 {
			return nil, fmt.Errorf("antilag: invalid STUN IP:port %q", t.Address)
		}
		if t.CIDR != "" {
			t.prefix, err = netip.ParsePrefix(t.CIDR)
			if err != nil {
				return nil, err
			}
			t.prefix = t.prefix.Masked()
			t.CIDR = t.prefix.String()
		}
		if seen[t.CIDR] {
			return nil, fmt.Errorf("antilag: duplicate probe-target cidr %q", t.CIDR)
		}
		seen[t.CIDR] = true
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &AntiLag{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name: o.Name, Type: C.AntiLag, Hidden: o.Hidden, Icon: o.Icon,
			Filter: o.Filter, ExcludeFilter: o.ExcludeFilter, ExcludeType: o.ExcludeType,
			TestTimeout: o.TestTimeout, MaxFailedTimes: o.MaxFailedTimes,
			EmptyFallback: empty, Providers: providers,
		}),
		url: o.URL, disableUDP: o.DisableUDP, targets: opt.Targets,
		ctx: ctx, cancel: cancel, parents: make(map[*antiSocket]bool),
		flows: make(map[string]*antiFlow), measurements: make(map[netip.AddrPort]*antiRegion),
	}
	g.history = newAntiHistory(cachefile.Cache().DB, C.Path.Config(), o.Name)
	g.shadowStatus = "disabled"
	if opt.ShadowModel != "" {
		model, err := loadAntiShadow(opt.ShadowModel)
		if err != nil {
			g.shadowStatus = "unavailable"
			log.Warnln("[AntiLag] %s: shadow model unavailable: %v", o.Name, err)
		} else {
			g.shadow = model
			g.shadowStatus = "ready"
		}
	}
	loadBundledAWS()
	g.exits = smart.NewExitWatcher(smart.ExitWatcherOptions{Name: o.Name})
	g.countryPath, g.asnPath = C.Path.MMDB(), C.Path.ASN()
	g.loadLocalGeo()
	g.loadRegions()
	return g, nil
}

func (g *AntiLag) start() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ctx.Err() != nil {
		return
	}
	g.once.Do(func() {
		// Config parsing may precede the previous group's final checkpoint.
		g.history.journal.once.Do(g.history.loadJournal)
		if g.exits != nil {
			g.workers.Add(1)
			go func() {
				defer g.workers.Done()
				timer := time.NewTicker(time.Hour)
				defer timer.Stop()
				for {
					refreshAWS(g.ctx, g.history.db)
					select {
					case <-g.ctx.Done():
						return
					case <-timer.C:
						g.loadLocalGeo()
					}
				}
			}()
		}
		g.workers.Add(1)
		go func() { defer g.workers.Done(); g.monitor() }()
	})
}
func (g *AntiLag) Providers() []P.ProxyProvider { return g.providers }
func (g *AntiLag) Proxies() []C.Proxy           { return g.GetProxies(false) }
func (g *AntiLag) Now() string {
	p := g.Unwrap(nil, false)
	if p == nil {
		return ""
	}
	return p.Name()
}
func (g *AntiLag) SupportUDP() bool {
	if g.disableUDP {
		return false
	}
	for _, p := range g.GetProxies(false) {
		if p.SupportUDP() {
			return true
		}
	}
	return false
}
func (g *AntiLag) IsL3Protocol(m *C.Metadata) bool {
	p := g.Unwrap(m, false)
	return p != nil && p.IsL3Protocol(m)
}
func (g *AntiLag) MarshalJSON() ([]byte, error) {
	names := []string{}
	for _, p := range g.Proxies() {
		names = append(names, p.Name())
	}
	info := map[string]any{"type": g.Type().String(), "all": names, "now": g.Now(), "hidden": g.Hidden(), "icon": g.Icon(), "testUrl": g.url, "shadowStatus": g.shadowStatus}
	if g.shadow != nil {
		info["shadowModelId"] = g.shadow.id
		info["shadowProcess"] = g.shadow.process
	}
	return json.Marshal(info)
}
func (g *AntiLag) target(ip netip.Addr) netip.AddrPort {
	best := -2
	var target netip.AddrPort
	for _, t := range g.targets {
		bits := t.prefix.Bits()
		if bits > best && (!t.prefix.IsValid() || t.prefix.Contains(ip)) {
			best = bits
			target = t.endpoint
		}
	}
	return target
}

type antiCandidate struct {
	regionStallCost                                             float64
	prior                                                       float64
	exitCountry                                                 string
	geoCredit                                                   float64
	stalls                                                      *antiStallEvidence
	outer                                                       *C.OuterQUICQuality
	score, confidence, failurePenalty, cadenceWeight, cadenceMS float64
	starts                                                      uint64
	proxy                                                       C.Proxy
	failed                                                      bool
	tier                                                        int
	penalty                                                     float64
	lastSelected                                                time.Time
	historyEvidence                                             *antiHistoryEvidence
	regional                                                    bool
	delay                                                       uint16
	reason                                                      string
}

func (g *AntiLag) candidates(m *C.Metadata, touch bool) []antiCandidate {
	if m == nil {
		m = &C.Metadata{}
	}
	proxies := g.GetProxies(touch)
	target := g.target(m.DstIP)
	grades := make(map[string]probeGrade)
	g.mu.Lock()
	regional := ""
	if r := g.measurements[target]; r != nil && r.grade(r.winner).count >= 20 {
		regional = r.winner
		if m.NetWork == C.UDP {
			for _, p := range proxies {
				grades[p.Name()] = r.grade(p.Name())
			}
		}
	}
	g.mu.Unlock()
	var list []antiCandidate
	for _, p := range proxies {
		if m.NetWork == C.UDP && (!p.SupportUDP() || g.disableUDP) || !p.AliveForTestUrl(g.url) {
			continue
		}
		c := g.history.evaluate(m, p.Name())
		c.proxy = p
		if g.exits != nil {
			info := g.exits.Info(p.Name())
			if time.Since(time.Unix(info.Updated, 0)) < 6*time.Hour {
				c.exitCountry = countryCode(info.Region)
			}
			if touch && m.DstIP.IsGlobalUnicast() && !m.DstIP.IsPrivate() {
				g.probeExit(p)
			}
		}
		if reporter, ok := p.Adapter().(C.OuterQUICReporter); ok {
			c.outer = reporter.OuterQUIC()
		}
		c.delay = p.LastDelayForTestUrl(g.url)
		c.regional = m.NetWork == C.UDP && regional == p.Name()
		if c.exitCountry != "" && c.exitCountry == m.DestinationCountry && c.tier == 0 && len(grades) == 0 && !c.failed && c.failurePenalty == 0 {
			c.geoCredit = 2
		}
		if c.regional && c.tier == 0 {
			c.reason = "regional-probe"
		}
		list = append(list, c)
	}
	rankAntiCandidates(list, grades, regional)
	return list
}

// An unknown path starts at the health-check prior. Exact game observations
// gradually replace that prior. Scores are costs, not estimates of game RTT.
func rankAntiCandidates(list []antiCandidate, grades map[string]probeGrade, leader string) {
	var known []int
	var gapSum, gapWeight float64
	for _, c := range list {
		if c.delay > 0 && c.delay < 65535 {
			known = append(known, int(c.delay))
		}
		gapSum += c.cadenceWeight * c.cadenceMS
		gapWeight += c.cadenceWeight
	}
	sort.Ints(known)
	fallback := 100.0
	if len(known) > 0 {
		fallback = float64(known[len(known)/2])
	}
	for i := range list {
		c := &list[i]
		prior := float64(c.delay)
		if c.delay == 0 || c.delay == 65535 {
			prior = fallback
		}
		if grade := grades[c.proxy.Name()]; grade.count >= 20 && grades[leader].count >= 20 {
			cost := func(v probeGrade) float64 { return float64(v.p95)/1e6 + 500*v.loss }
			prior = math.Max(cost(grade), cost(grades[leader]))
			if c.confidence == 0 {
				c.reason = "regional-probe"
			}
		}
		c.prior = prior
		c.score = (1-c.confidence)*prior + c.confidence*c.penalty + c.failurePenalty + c.regionStallCost - c.geoCredit
		if c.stalls != nil {
			c.score += c.stalls.Penalty
		}
		if c.outer != nil {
			c.score += c.outer.Penalty
		}
		if gapWeight > 0 {
			c.score += c.cadenceWeight * (c.cadenceMS - gapSum/gapWeight)
		}
	}
	sort.SliceStable(list, func(i, j int) bool {
		a, b := list[i], list[j]
		if a.failed != b.failed {
			return !a.failed
		}
		if a.score != b.score {
			return a.score < b.score
		}
		if a.regional != b.regional {
			return a.regional
		}
		if !a.lastSelected.Equal(b.lastSelected) {
			return a.lastSelected.After(b.lastSelected)
		}
		return a.delay < b.delay
	})
}

// Sample only on a new connection to this destination. A warm standby, API
// lookup or manual switch must neither consume the budget nor move a flow.
func exploreAntiCandidate(list []antiCandidate) {
	if len(list) < 2 {
		return
	}
	var starts uint64
	for _, c := range list {
		starts += c.starts
	}
	if starts%8 != 7 {
		return
	}
	best := 0
	limit := list[0].score + math.Max(25, math.Abs(list[0].score)*0.25)
	for i := 1; i < len(list); i++ {
		if !list[i].failed && list[i].score <= limit && list[i].starts < list[best].starts {
			best = i
		}
	}
	if best > 0 {
		trial := list[best]
		copy(list[1:best+1], list[:best])
		list[0] = trial
		list[0].reason = "destination-trial"
	}
}
func (g *AntiLag) Unwrap(m *C.Metadata, touch bool) C.Proxy {
	list := g.candidates(m, touch)
	if len(list) > 0 {
		return list[0].proxy
	}
	return nil
}
func (g *AntiLag) DialContext(ctx context.Context, m *C.Metadata) (C.Conn, error) {
	g.start()
	if g.ctx.Err() != nil {
		return nil, net.ErrClosed
	}
	var last error
	m = m.Clone()
	g.enrichGeo(m)
	options := g.candidates(m, true)
	exploreAntiCandidate(options)
	for _, c := range options {
		start := time.Now()
		child, cancel := context.WithTimeout(ctx, C.DefaultTCPTimeout)
		conn, err := c.proxy.DialContext(child, m.Clone())
		cancel()
		if ctx.Err() != nil {
			if conn != nil {
				conn.Close()
			}
			return nil, ctx.Err()
		}
		g.history.event(m, c.proxy.Name(), antiDial, err, 0)
		if err != nil {
			last = err
			continue
		}
		conn.AppendToChains(g)
		g.history.selected(m, c.proxy.Name(), true)
		meta := m.Clone()
		name := c.proxy.Name()
		return callback.NewFirstReadCallBackConn(conn, func(err error) {
			if err == nil {
				g.history.event(meta, name, antiReply, nil, time.Since(start))
			}
		}), nil
	}
	if last == nil {
		last = errors.New("antilag: no available TCP path")
	}
	return nil, last
}
func (g *AntiLag) ListenPacketContext(ctx context.Context, m *C.Metadata) (C.PacketConn, error) {
	if g.disableUDP {
		return nil, C.ErrNotSupport
	}
	meta := m.Clone()
	if err := g.ResolveUDP(ctx, meta); err != nil {
		return nil, err
	}
	g.start()
	s := newAntiSocket(g, meta)
	g.mu.Lock()
	if g.ctx.Err() != nil {
		g.mu.Unlock()
		s.cancel()
		return nil, net.ErrClosed
	}
	g.parents[s] = true
	g.mu.Unlock()
	initial, err := s.binding(ctx, meta.AddrPort())
	if err != nil {
		s.Close()
		return nil, err
	}
	initial.mu.Lock()
	if initial.active != nil {
		if p, ok := N.FindUpstream[C.UDPQueuePolicy](initial.active.PacketConn, func(p C.UDPQueuePolicy) bool { return p.UDPMaxQueueDelay() > 0 }); ok {
			s.initialQueueDelay = p.UDPMaxQueueDelay()
		}
	}
	initial.mu.Unlock()
	return outbound.NewPacketConn(s, g), nil
}
func (g *AntiLag) Bindings() []antiBindingInfo {
	g.mu.Lock()
	flows := make([]*antiFlow, 0, len(g.flows))
	for _, f := range g.flows {
		flows = append(flows, f)
	}
	g.mu.Unlock()
	out := make([]antiBindingInfo, 0, len(flows))
	for _, f := range flows {
		out = append(out, f.info())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (g *AntiLag) Failover(ctx context.Context, id, name string) error {
	g.mu.Lock()
	f := g.flows[id]
	g.mu.Unlock()
	if f == nil {
		return fmt.Errorf("unknown antilag binding %q", id)
	}
	return f.change(ctx, name, nil, "manual", 0)
}
func (g *AntiLag) Close() error {
	g.mu.Lock()
	g.cancel()
	var parents []*antiSocket
	for s := range g.parents {
		parents = append(parents, s)
	}
	g.mu.Unlock()
	for _, s := range parents {
		s.Close()
	}
	g.workers.Wait()
	g.history.flush()
	g.saveRegions()
	return nil
}
