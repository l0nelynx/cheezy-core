package outboundgroup

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"net"
	"net/netip"
	"sort"
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

type probeSample struct {
	at       time.Time
	duration time.Duration
	ok       bool
}
type antiRegion struct {
	samples            map[string][]probeSample
	winner, challenger string
	votes              int
	rotation           int
	revision, lastVote uint64
}
type probeGrade struct {
	count int
	total int
	p95   time.Duration
	loss  float64
}

func (r *antiRegion) grade(name string) probeGrade {
	window := r.samples[name]
	var values []time.Duration
	n := 0
	for _, v := range window {
		if time.Since(v.at) > 5*time.Minute {
			continue
		}
		n++
		if v.ok {
			values = append(values, v.duration)
		}
	}
	g := probeGrade{count: len(values), total: n}
	if n > 0 {
		g.loss = float64(n-len(values)) / float64(n)
	}
	if len(values) > 0 {
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		g.p95 = values[(95*len(values)+99)/100-1]
	}
	return g
}

type antiEvidence struct {
	Samples   int        `json:"samples"`
	Replies   int        `json:"replies"`
	P95MS     *float64   `json:"p95Ms"`
	LastProbe *time.Time `json:"lastProbeAt"`
}

func (r *antiRegion) evidence(name string) antiEvidence {
	g := r.grade(name)
	out := antiEvidence{Samples: g.total, Replies: g.count}
	if g.count >= 20 {
		ms := float64(g.p95) / 1e6
		out.P95MS = &ms
	}
	if v := r.samples[name]; len(v) > 0 {
		at := v[len(v)-1].at
		out.LastProbe = &at
	}
	return out
}
func (r *antiRegion) record(name string, s probeSample) {
	r.revision++
	v := append(r.samples[name], s)
	if len(v) > 64 {
		v = append([]probeSample(nil), v[len(v)-64:]...)
	}
	r.samples[name] = v
}
func (r *antiRegion) compare(a, b string) {
	x, y := r.grade(a), r.grade(b)
	if x.count < 20 || y.count < 20 {
		return
	}
	if r.winner == "" || r.grade(r.winner).count < 20 {
		r.winner = a
	}
	if r.winner == b {
		a, b = b, a
		x, y = y, x
	}
	if r.winner != a {
		a = r.winner
		x = r.grade(a)
	}
	margin := x.p95 * 15 / 100
	if margin < 10*time.Millisecond {
		margin = 10 * time.Millisecond
	}
	if !(y.loss+0.05 < x.loss || y.loss <= x.loss+0.05 && y.p95+margin <= x.p95) {
		if r.challenger != b {
			return
		}
		r.challenger = ""
		r.votes = 0
		return
	}
	if r.challenger != b {
		r.challenger = b
		r.votes = 0
	} else {
		if r.revision == r.lastVote {
			return
		}
	}
	r.lastVote = r.revision
	r.votes++
	if r.votes >= 2 {
		r.winner = b
		r.votes = 0
		r.challenger = ""
	}
}

// One STUN Binding transaction. Only a success response for our transaction
// from the configured IP:port is a positive observation.
func stunProbe(parent context.Context, p C.Proxy, endpoint netip.AddrPort) probeSample {
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	sample := probeSample{at: time.Now()}
	pc, err := p.ListenPacketContext(ctx, &C.Metadata{Type: C.INNER, NetWork: C.UDP, DstIP: endpoint.Addr(), DstPort: endpoint.Port()})
	if err != nil {
		return sample
	}
	done := make(chan struct{})
	defer close(done)
	var closeOnce sync.Once
	closeSocket := func() { closeOnce.Do(func() { pc.Close() }) }
	defer closeSocket()
	go func() {
		select {
		case <-ctx.Done():
			closeSocket()
		case <-done:
		}
	}()
	cutoff, _ := ctx.Deadline()
	pc.SetDeadline(cutoff)
	var request [20]byte
	request[1] = 1
	binary.BigEndian.PutUint32(request[4:], 0x2112a442)
	if _, err = rand.Read(request[8:]); err != nil {
		return sample
	}
	start := time.Now()
	if _, err = pc.WriteTo(request[:], net.UDPAddrFromAddrPort(endpoint)); err != nil {
		return sample
	}
	var reply [2048]byte
	for ctx.Err() == nil {
		n, addr, e := pc.ReadFrom(reply[:])
		if e != nil {
			return sample
		}
		src, ok := addr.(*net.UDPAddr)
		if !ok || src == nil || src.AddrPort().Addr().Unmap() != endpoint.Addr().Unmap() || src.Port != int(endpoint.Port()) {
			continue
		}
		if n >= 20 && n%4 == 0 && int(binary.BigEndian.Uint16(reply[2:4])) == n-20 && binary.BigEndian.Uint16(reply[:2]) == 0x101 && bytes.Equal(request[4:], reply[4:20]) {
			sample.ok = true
			sample.duration = time.Since(start)
			break
		}
	}
	return sample
}

type probePair struct {
	target netip.AddrPort
	a, b   string
}
type antiMiss struct {
	count int
	first time.Time
}

func (m *antiMiss) update(a, b bool, at time.Time) bool {
	if a || !b {
		*m = antiMiss{}
		return false
	}
	if m.count == 0 {
		m.first = at
	}
	m.count++
	return m.count >= 3
}
func (g *AntiLag) monitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	misses := map[probePair]antiMiss{}
	tick := 0
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
		}
		tick++
		if tick%30 == 0 {
			g.history.flush()
			g.saveRegions()
		}
		g.mu.Lock()
		flows := make([]*antiFlow, 0, len(g.flows))
		for _, f := range g.flows {
			flows = append(flows, f)
		}
		g.mu.Unlock()
		pairs := map[probePair][]*antiFlow{}
		for _, f := range flows {
			f.mu.Lock()
			active, spare, last := f.active, f.reserve, f.lastUse
			f.mu.Unlock()
			if time.Since(last) > 2*time.Minute {
				s := f.socket
				s.mu.Lock()
				f.mu.Lock()
				idle := time.Since(f.lastUse) > 2*time.Minute
				f.mu.Unlock()
				if idle && s.flows[f.dest] == f {
					delete(s.flows, f.dest)
					f.close()
				}
				s.mu.Unlock()
				continue
			}
			if active == nil {
				continue
			}
			if spare == nil {
				if tick%5 == 0 {
					go f.warm(active)
				}
				continue
			}
			target := g.target(f.dest.Addr())
			if tick%30 == 0 {
				go f.warm(active)
			}
			if target.IsValid() {
				k := probePair{target, active.name, spare.name}
				pairs[k] = append(pairs[k], f)
			}
		}
		for k := range misses {
			if len(pairs[k]) == 0 {
				delete(misses, k)
			}
		}
		if len(pairs) == 0 {
			continue
		}
		members := map[string]C.Proxy{}
		for _, p := range g.Proxies() {
			if p.SupportUDP() {
				members[p.Name()] = p
			}
		}
		// Share a probe between all bindings using the same node and reflector.
		type key struct {
			target netip.AddrPort
			name   string
		}
		keys := map[key]bool{}
		for k := range pairs {
			keys[key{k.target, k.a}] = true
			keys[key{k.target, k.b}] = true
		}
		results := make(map[key]probeSample)
		var lock sync.Mutex
		var wg sync.WaitGroup
		slots := make(chan struct{}, 2)
		for k := range keys {
			p := members[k.name]
			if p == nil {
				continue
			}
			slots <- struct{}{}
			wg.Add(1)
			go func(k key, p C.Proxy) {
				defer wg.Done()
				defer func() { <-slots }()
				v := stunProbe(g.ctx, p, k.target)
				lock.Lock()
				results[k] = v
				lock.Unlock()
			}(k, p)
		}
		wg.Wait()
		g.mu.Lock()
		for k, s := range results {
			r := g.measurements[k.target]
			if r == nil {
				r = &antiRegion{samples: map[string][]probeSample{}}
				g.measurements[k.target] = r
			}
			r.record(k.name, s)
		}
		g.mu.Unlock()
		regions := map[netip.AddrPort]string{}
		for k, bindings := range pairs {
			a, okA := results[key{k.target, k.a}]
			b, okB := results[key{k.target, k.b}]
			if !okA || !okB {
				delete(misses, k)
				continue
			}
			regions[k.target] = k.a
			g.mu.Lock()
			if a.ok || b.ok {
				g.measurements[k.target].compare(k.a, k.b)
			}
			g.mu.Unlock()
			miss := misses[k]
			if miss.update(a.ok, b.ok, a.at) {
				for _, f := range bindings {
					f.mu.Lock()
					p := f.active
					changed := f.changed
					f.mu.Unlock()
					if p != nil && p.name == k.a && !changed.After(miss.first) {
						f.change(g.ctx, k.b, p, "probe-timeout", time.Since(miss.first))
					}
				}
				miss = antiMiss{}
			}
			misses[k] = miss
		}
		if tick%30 == 0 && len(regions) > 0 {
			var targets []netip.AddrPort
			for target := range regions {
				targets = append(targets, target)
			}
			sort.Slice(targets, func(i, j int) bool { return targets[i].String() < targets[j].String() })
			target := targets[(tick/30-1)%len(targets)]
			g.scheduleRegionalRound(target, regions[target], members)
		}
	}
}
func (g *AntiLag) scheduleRegionalRound(target netip.AddrPort, active string, members map[string]C.Proxy) {
	g.mu.Lock()
	if g.regionalBusy || g.ctx.Err() != nil {
		g.mu.Unlock()
		return
	}
	g.regionalBusy = true
	g.workers.Add(1)
	g.mu.Unlock()
	go func() {
		defer g.workers.Done()
		g.regionalRound(target, active, members)
		g.mu.Lock()
		g.regionalBusy = false
		g.mu.Unlock()
	}()
}
func (g *AntiLag) regionalRound(target netip.AddrPort, active string, members map[string]C.Proxy) {
	var names []string
	for name := range members {
		if name != active {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return
	}
	g.mu.Lock()
	r := g.measurements[target]
	other := names[r.rotation%len(names)]
	r.rotation++
	g.mu.Unlock()
	var wg sync.WaitGroup
	var received bool
	for _, name := range []string{active, other} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			for i := 0; i < 3 && g.ctx.Err() == nil; i++ {
				s := stunProbe(g.ctx, members[name], target)
				g.mu.Lock()
				r.record(name, s)
				received = received || s.ok
				g.mu.Unlock()
			}
		}(name)
	}
	wg.Wait()
	g.mu.Lock()
	if received {
		r.compare(active, other)
	}
	g.mu.Unlock()
}
