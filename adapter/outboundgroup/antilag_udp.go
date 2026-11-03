package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/net/deadline"
	"github.com/metacubex/mihomo/common/pool"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	M "github.com/metacubex/sing/common/metadata"
)

type antiPath struct {
	decision *antiDecision
	C.PacketConn
	name        string
	member      C.Proxy
	firstWrite  time.Time
	lastWrite   time.Time
	cadence     cadenceWindow
	stalls      stallMeter
	observed    bool
	retired     chan struct{}
	once        sync.Once
	failureOnce sync.Once
	preparation time.Duration
	writes      chan antiWrite
}

type antiWrite struct {
	payload []byte
	addr    netip.AddrPort
	result  chan antiWriteResult
}
type antiWriteResult struct {
	n   int
	err error
}

// One writer owns each generation. Retiring A must wake its caller even if
// the transport is still blocked inside WriteTo. Only the old writer retains
// the owned payload until that call returns.
func (f *antiFlow) writeLoop(p *antiPath) {
	for {
		select {
		case <-p.retired:
			return
		case w := <-p.writes:
			n, err := p.WriteTo(w.payload, net.UDPAddrFromAddrPort(w.addr))
			if err == nil && n == len(w.payload) {
				f.mu.Lock()
				if f.active == p {
					f.observeStalls(p, time.Now(), false)
				}
				f.mu.Unlock()
			}
			pool.Put(w.payload)
			w.result <- antiWriteResult{n, err}
		}
	}
}

func (p *antiPath) done() <-chan struct{} { return p.retired }
func (p *antiPath) Close() error {
	p.once.Do(func() { close(p.retired); p.PacketConn.Close() })
	return nil
}

type antiPacket struct {
	path     *antiPath
	flow     *antiFlow
	data     []byte
	addr     net.Addr
	put      func()
	received time.Time
}
type antiSocket struct {
	group             *AntiLag
	meta              *C.Metadata
	ctx               context.Context
	cancel            context.CancelFunc
	mu                sync.Mutex
	flows             map[netip.AddrPort]*antiFlow
	incoming          chan antiPacket
	failures          chan antiReadFailure
	readDeadline      deadline.PipeDeadline
	writeClock        deadline.PipeDeadline
	writeDeadline     time.Time
	initialQueueDelay time.Duration
}

// The tunnel binds this policy once when it starts its per-source sender.
func (s *antiSocket) UDPMaxQueueDelay() time.Duration { return s.initialQueueDelay }

func (s *antiSocket) UDPRoutes() []C.UDPRoute {
	s.mu.Lock()
	routes := make([]C.UDPRoute, 0, len(s.flows))
	for _, f := range s.flows {
		f.mu.Lock()
		if p := f.active; p != nil && !f.closed {
			routes = append(routes, C.UDPRoute{ID: f.id, Destination: f.dest.String(),
				Chains: append(C.Chain(nil), p.Chains()...), ProviderChains: append(C.Chain(nil), p.ProviderChains()...)})
		}
		f.mu.Unlock()
	}
	s.mu.Unlock()
	sort.Slice(routes, func(i, j int) bool { return routes[i].Destination < routes[j].Destination })
	return routes
}

type antiReadFailure struct {
	flow *antiFlow
	err  error
}
type antiFlow struct {
	socket                          *antiSocket
	id                              string
	dest                            netip.AddrPort
	meta                            *C.Metadata
	ready                           chan struct{}
	initErr                         error
	switchGate                      chan struct{}
	writeMu                         sync.Mutex
	mu                              sync.Mutex
	closed, warming                 bool
	active, reserve                 *antiPath
	reason, selection               string
	lastUse, lastRead, previousRead time.Time
	gap, prepare, detect            time.Duration
	changed                         time.Time
	switchTime                      time.Duration
}
type antiBindingInfo struct {
	ShadowWinner       string                  `json:"shadowWinner,omitempty"`
	DestinationRegion  string                  `json:"destinationRegion"`
	GeoSource          string                  `json:"geoSource"`
	DestinationCountry string                  `json:"destinationCountry,omitempty"`
	Candidates         []antiCandidateInfo     `json:"candidates"`
	History            *antiHistoryEvidence    `json:"history,omitempty"`
	ID                 string                  `json:"id"`
	Destination        string                  `json:"destination"`
	Source             string                  `json:"source,omitempty"`
	Process            string                  `json:"process,omitempty"`
	Active             string                  `json:"active"`
	Reserve            string                  `json:"reserve"`
	Selection          string                  `json:"selection"`
	Reason             string                  `json:"reason"`
	Changed            time.Time               `json:"changedAt"`
	PrepareMS          float64                 `json:"prepareMs"`
	SwitchMS           float64                 `json:"switchMs"`
	DetectMS           float64                 `json:"detectMs"`
	GapMS              *float64                `json:"receiveGapMs"`
	Control            string                  `json:"control,omitempty"`
	Evidence           map[string]antiEvidence `json:"evidence,omitempty"`
}

type antiCandidateInfo struct {
	PredictedStallSeconds *float64             `json:"predictedStallSecondsPerActiveMinute,omitempty"`
	Prior                 float64              `json:"priorCost"`
	RegionalStallCost     float64              `json:"regionalStallCost"`
	ExitCountry           string               `json:"exitCountry,omitempty"`
	GeoCredit             float64              `json:"geoCredit"`
	HealthDelay           uint16               `json:"healthDelayMs"`
	FailurePenalty        float64              `json:"failurePenalty"`
	Stalls                *antiStallEvidence   `json:"stalls,omitempty"`
	Outer                 *C.OuterQUICQuality  `json:"outerQuic,omitempty"`
	Node                  string               `json:"node"`
	Cost                  float64              `json:"cost"`
	Source                string               `json:"source"`
	Cooldown              bool                 `json:"cooldown"`
	Starts                uint64               `json:"starts"`
	History               *antiHistoryEvidence `json:"history,omitempty"`
	GapSpreadMS           float64              `json:"gapSpreadMs"`
	GapWeight             float64              `json:"gapWeight"`
}

func candidateInfo(c antiCandidate) antiCandidateInfo {
	return antiCandidateInfo{Node: c.proxy.Name(), Cost: c.score, Source: c.reason, Cooldown: c.failed, Starts: c.starts,
		History: c.historyEvidence, GapSpreadMS: c.cadenceMS, GapWeight: c.cadenceWeight,
		Stalls: c.stalls, Outer: c.outer, ExitCountry: c.exitCountry, GeoCredit: c.geoCredit, HealthDelay: c.delay, FailurePenalty: c.failurePenalty,
		Prior: c.prior, RegionalStallCost: c.regionStallCost}
}

func (f *antiFlow) info() antiBindingInfo {
	f.mu.Lock()
	v := antiBindingInfo{ID: f.id, Destination: f.dest.String(), Selection: f.selection, Reason: f.reason, Changed: f.changed, PrepareMS: float64(f.prepare) / 1e6, DetectMS: float64(f.detect) / 1e6}
	v.SwitchMS = float64(f.switchTime) / 1e6
	v.Process = f.meta.Process
	v.DestinationRegion, v.GeoSource, v.DestinationCountry = f.meta.DestinationRegion, f.meta.GeoSource, f.meta.DestinationCountry
	if f.meta.SrcIP.IsValid() {
		v.Source = f.meta.SourceAddrPort().String()
	}
	if f.active != nil {
		v.Active = f.active.name
	}
	if f.reserve != nil {
		v.Reserve = f.reserve.name
	}
	if f.gap > 0 {
		ms := float64(f.gap) / 1e6
		v.GapMS = &ms
	}
	f.mu.Unlock()
	g := f.socket.group
	v.Candidates, v.ShadowWinner = g.describeCandidates(f.meta, g.candidates(f.meta, false))
	if v.Active != "" {
		v.History = g.history.evaluate(f.meta, v.Active).historyEvidence
	}
	if target := g.target(f.dest.Addr()); target.IsValid() {
		v.Control = target.String()
		g.mu.Lock()
		if r := g.measurements[target]; r != nil {
			v.Evidence = map[string]antiEvidence{}
			for _, candidate := range v.Candidates {
				name := candidate.Node
				if name != "" {
					v.Evidence[name] = r.evidence(name)
				}
			}
		}
		g.mu.Unlock()
	}
	return v
}
func newAntiSocket(g *AntiLag, m *C.Metadata) *antiSocket {
	ctx, cancel := context.WithCancel(g.ctx)
	return &antiSocket{
		group: g, meta: m, ctx: ctx, cancel: cancel,
		flows: make(map[netip.AddrPort]*antiFlow), incoming: make(chan antiPacket),
		failures:     make(chan antiReadFailure, 1),
		readDeadline: deadline.MakePipeDeadline(), writeClock: deadline.MakePipeDeadline(),
	}
}
func (s *antiSocket) binding(ctx context.Context, dest netip.AddrPort) (*antiFlow, error) {
	dest = netip.AddrPortFrom(dest.Addr().Unmap(), dest.Port())
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil, net.ErrClosed
	}
	if f := s.flows[dest]; f != nil {
		s.mu.Unlock()
		select {
		case <-f.ready:
			return f, f.initErr
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.ctx.Done():
			return nil, net.ErrClosed
		}
	}
	if len(s.flows) >= 256 {
		s.mu.Unlock()
		return nil, errors.New("antilag: destination limit reached")
	}
	m := s.meta.Clone()
	if m.AddrPort() != dest {
		m.Host = ""
		m.DstIPASN = ""
		m.SniffHost = ""
		m.DstGeoIP = nil
		m.RawDstAddr = nil
		m.WildcardTarget = ""
	}
	m.DstIP = dest.Addr()
	m.DstPort = dest.Port()
	m.NetWork = C.UDP
	s.group.enrichGeo(m)
	f := &antiFlow{socket: s, id: utils.NewUUIDV4().String(), dest: dest, meta: m, ready: make(chan struct{}), switchGate: make(chan struct{}, 1), lastUse: time.Now()}
	s.flows[dest] = f
	s.mu.Unlock()
	err := f.change(ctx, "", nil, "initial", 0)
	s.mu.Lock()
	if err == nil && s.ctx.Err() == nil {
		s.group.mu.Lock()
		s.group.flows[f.id] = f
		s.group.mu.Unlock()
	} else {
		if err == nil {
			err = net.ErrClosed
		}
		delete(s.flows, dest)
	}
	f.initErr = err
	close(f.ready)
	s.mu.Unlock()
	if err != nil {
		f.close()
		return nil, err
	}
	return f, nil
}
func (f *antiFlow) dial(ctx context.Context, p C.Proxy) (*antiPath, error) {
	child, cancel := context.WithTimeout(ctx, C.DefaultUDPTimeout)
	defer cancel()
	start := time.Now()
	pc, err := p.ListenPacketContext(child, f.meta.Clone())
	if ctx.Err() == nil && f.socket.ctx.Err() == nil {
		f.socket.group.history.event(f.meta, p.Name(), antiDial, err, 0)
	}
	if err != nil {
		return nil, err
	}
	return &antiPath{PacketConn: pc, name: p.Name(), member: p, retired: make(chan struct{}), preparation: time.Since(start), writes: make(chan antiWrite)}, nil
}

// expected guards asynchronous failure reports from retired generations.
func (f *antiFlow) change(ctx context.Context, name string, expected *antiPath, reason string, detected time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, C.DefaultUDPTimeout)
	defer cancel()
	select {
	case f.switchGate <- struct{}{}:
		defer func() { <-f.switchGate }()
	case <-ctx.Done():
		return ctx.Err()
	case <-f.socket.ctx.Done():
		return net.ErrClosed
	}
	start := time.Now()
	f.mu.Lock()
	if f.closed || f.socket.ctx.Err() != nil {
		f.mu.Unlock()
		return net.ErrClosed
	}
	old := f.active
	if expected != nil && old != expected || old != nil && old.name == name {
		f.mu.Unlock()
		return nil
	}
	spare := f.reserve
	f.mu.Unlock()
	if expected != nil {
		expected.failureOnce.Do(func() { f.socket.group.history.event(f.meta, expected.name, antiFailure, nil, 0) })
	}
	options := f.socket.group.candidates(f.meta, true)
	journal := &f.socket.group.history.journal
	row := &antiDecision{Binding: f.id, Started: start, Destination: f.dest.String(), DestinationRegion: f.meta.DestinationRegion,
		DestinationCountry: f.meta.DestinationCountry, GeoSource: f.meta.GeoSource, ASN: f.meta.DstIPASN, Process: f.meta.Process, Trigger: reason}
	row.Candidates, row.ShadowWinner = f.socket.group.describeCandidates(f.meta, options)
	if row.ShadowWinner != "" {
		row.ShadowModelID = f.socket.group.shadow.id
	}
	if reason == "initial" {
		exploreAntiCandidate(options)
	}
	if name == "" && expected != nil && spare != nil {
		for i, c := range options {
			if c.proxy.Name() == spare.name {
				options[0], options[i] = options[i], options[0]
				break
			}
		}
	}
	var next *antiPath
	var last error
	var selection string
	for _, c := range options {
		if c.failed && expected != nil {
			continue
		}
		if name != "" && c.proxy.Name() != name || old != nil && old.name == c.proxy.Name() {
			continue
		}
		if spare != nil && c.proxy == spare.member {
			next = spare
		} else {
			next, last = f.dial(ctx, c.proxy)
		}
		if ctx.Err() != nil {
			if next != nil && next != spare {
				next.Close()
			}
			return ctx.Err()
		}
		if next != nil {
			selection = c.reason
			break
		}
		row.DialFailures = append(row.DialFailures, c.proxy.Name())
	}
	if next == nil {
		now := time.Now()
		row.Ended = &now
		row.Status = "unavailable"
		journal.add(row)
		if last == nil {
			last = errors.New("antilag: no available replacement path")
		}
		return last
	}
	f.socket.mu.Lock()
	if !f.socket.writeDeadline.IsZero() {
		next.SetWriteDeadline(f.socket.writeDeadline)
	}
	f.mu.Lock()
	f.socket.mu.Unlock()
	if f.closed {
		f.mu.Unlock()
		if next != spare {
			next.Close()
		}
		return net.ErrClosed
	}
	stale := f.reserve
	f.flushStalls(old, true)
	journal.end(old, reason, expected != nil)
	row.Selected, row.Selection, row.Status = next.name, selection, "active"
	journal.add(row)
	next.decision = row
	f.active = next
	f.reserve = nil
	f.changed = time.Now()
	f.prepare = next.preparation
	f.switchTime = time.Since(start)
	f.detect = detected
	f.reason = reason
	f.selection = selection
	f.previousRead = f.lastRead
	f.lastRead = time.Time{}
	f.gap = 0
	f.mu.Unlock()
	if old != nil {
		old.Close()
	}
	if stale != nil && stale != next {
		stale.Close()
	}
	f.socket.group.history.selected(f.meta, next.name, reason == "initial")
	go f.receive(next)
	go f.writeLoop(next)
	go f.warm(next)
	return nil
}
func (f *antiFlow) warm(active *antiPath) {
	f.mu.Lock()
	if f.closed || f.warming || f.active != active || active == nil {
		f.mu.Unlock()
		return
	}
	f.warming = true
	old := f.reserve
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.warming = false; f.mu.Unlock() }()
	options := f.socket.group.candidates(f.meta, false)
	for _, c := range options {
		if c.proxy.Name() == active.name || c.failed {
			continue
		}
		if old != nil {
			if old.member == c.proxy {
				return
			}
			for _, previous := range options {
				if previous.proxy == old.member && !previous.failed && previous.score-c.score < math.Max(10, 0.15*math.Abs(previous.score)) {
					return
				}
			}
		}
		p, err := f.dial(f.socket.ctx, c.proxy)
		if err != nil {
			continue
		}
		// A switch may have borrowed the standby while the new child was
		// being prepared. Serialize the swap with promotion of that child.
		if old != nil {
			select {
			case f.switchGate <- struct{}{}:
			default:
				p.Close()
				return
			}
		}
		f.mu.Lock()
		if f.active == active && !f.closed && f.reserve == old {
			f.reserve = p
			p = nil
		}
		f.mu.Unlock()
		if old != nil {
			<-f.switchGate
		}
		if p != nil {
			p.Close()
		} else if old != nil {
			old.Close()
		}
		return
	}
}
func (f *antiFlow) fail(path *antiPath, reason string, elapsed time.Duration) error {
	f.mu.Lock()
	valid := !f.closed && path == f.active
	f.mu.Unlock()
	if f.socket.ctx.Err() != nil {
		return net.ErrClosed
	}
	if !valid {
		return nil
	}
	err := f.change(f.socket.ctx, "", path, reason, elapsed)
	if err != nil {
		f.mu.Lock()
		if f.active == path {
			f.flushStalls(path, true)
			f.socket.group.history.journal.end(path, reason, true)
			f.active = nil
		}
		f.mu.Unlock()
		path.Close()
		select {
		case f.socket.failures <- antiReadFailure{f, err}:
		default:
		}
	}
	return err
}
func (f *antiFlow) receive(p *antiPath) {
	for {
		data, put, addr, err := p.WaitReadFrom()
		if err != nil {
			if put != nil {
				put()
			}
			f.fail(p, "read-failure", 0)
			return
		}
		packet := antiPacket{p, f, data, addr, put, time.Now()}
		if addr != nil && M.SocksaddrFromNet(addr).Unwrap().AddrPort() == f.dest {
			f.mu.Lock()
			if f.active == p {
				f.observeStalls(p, packet.received, true)
			}
			f.mu.Unlock()
		}
		select {
		case f.socket.incoming <- packet:
		case <-f.socket.ctx.Done():
			if put != nil {
				put()
			}
			return
		case <-p.done():
			if put != nil {
				put()
			}
			return
		}
	}
}
func (s *antiSocket) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	for {
		select {
		case <-s.ctx.Done():
			return nil, nil, nil, net.ErrClosed
		case <-s.readDeadline.Wait():
			return nil, nil, nil, os.ErrDeadlineExceeded
		case failure := <-s.failures:
			s.mu.Lock()
			sole := len(s.flows) == 1 && s.flows[failure.flow.dest] == failure.flow
			failure.flow.mu.Lock()
			unavailable := failure.flow.active == nil
			failure.flow.mu.Unlock()
			s.mu.Unlock()
			if sole && unavailable {
				return nil, nil, nil, failure.err
			}
		case packet := <-s.incoming:
			f := packet.flow
			p := packet.path
			now := packet.received
			f.mu.Lock()
			if f.closed || f.active != p {
				f.mu.Unlock()
				if packet.put != nil {
					packet.put()
				}
				continue
			}
			if f.lastRead.IsZero() && !f.previousRead.IsZero() {
				f.gap = now.Sub(f.previousRead)
			}
			spread, completed := p.cadence.observe(now, f.lastRead, p.lastWrite)
			f.lastRead = now
			f.lastUse = now
			first := !p.observed && !p.firstWrite.IsZero()
			start := p.firstWrite
			if first {
				p.observed = true
				s.group.history.journal.reply(p, now.Sub(start))
			}
			f.mu.Unlock()
			if completed {
				s.group.history.cadence(f.meta, p.name, spread, now)
			}
			if first {
				s.group.history.event(f.meta, p.name, antiReply, nil, now.Sub(start))
			}
			return packet.data, packet.put, packet.addr, nil
		}
	}
}
func (s *antiSocket) ReadFrom(b []byte) (int, net.Addr, error) {
	data, put, addr, err := s.WaitReadFrom()
	if put != nil {
		defer put()
	}
	return copy(b, data), addr, err
}
func (s *antiSocket) WriteTo(b []byte, addr net.Addr) (int, error) {
	ua, ok := addr.(*net.UDPAddr)
	if !ok || ua == nil || !ua.AddrPort().IsValid() {
		return 0, fmt.Errorf("antilag: invalid destination %v", addr)
	}
	s.mu.Lock()
	d := s.writeDeadline
	s.mu.Unlock()
	ctx := s.ctx
	if !d.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f, err := s.binding(ctx, ua.AddrPort())
	if err != nil {
		return 0, err
	}
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	select {
	case <-s.writeClock.Wait():
		return 0, os.ErrDeadlineExceeded
	case <-s.ctx.Done():
		return 0, net.ErrClosed
	default:
	}
	f.mu.Lock()
	p := f.active
	f.mu.Unlock()
	if p == nil {
		if err = f.change(ctx, "", nil, "reconnect", 0); err != nil {
			return 0, err
		}
	}
	f.mu.Lock()
	p = f.active
	if f.closed || p == nil {
		f.mu.Unlock()
		return 0, net.ErrClosed
	}
	f.lastUse = time.Now()
	p.lastWrite = f.lastUse
	if p.firstWrite.IsZero() {
		p.firstWrite = f.lastUse
	}
	f.mu.Unlock()
	payload := pool.Get(len(b))[:len(b)]
	copy(payload, b)
	w := antiWrite{payload, ua.AddrPort(), make(chan antiWriteResult, 1)}
	select {
	case p.writes <- w:
	case <-p.retired:
		pool.Put(payload)
		return f.retiredWrite(len(b))
	case <-s.writeClock.Wait():
		pool.Put(payload)
		return 0, os.ErrDeadlineExceeded
	case <-s.ctx.Done():
		pool.Put(payload)
		return 0, net.ErrClosed
	}
	var n int
	select {
	case r := <-w.result:
		n, err = r.n, r.err
	case <-p.retired:
		return f.retiredWrite(len(b))
	case <-s.writeClock.Wait():
		return 0, os.ErrDeadlineExceeded
	case <-s.ctx.Done():
		return 0, net.ErrClosed
	}
	// Per-datagram failures (oversize, deadline, bad destination) do not
	// establish that the transport has died. A terminal read also drives failover.
	if (errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.EOF)) && s.ctx.Err() == nil {
		if recovery := f.fail(p, "write-failure", 0); recovery == nil {
			return len(b), nil
		}
	}
	return n, err
}
func (f *antiFlow) retiredWrite(size int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.socket.ctx.Err() != nil {
		return 0, net.ErrClosed
	}
	if f.active == nil {
		return 0, errors.New("antilag: all paths failed")
	}
	// Delivery on the retired path is uncertain. Consume this datagram so
	// the caller can send the next one on the replacement without replay.
	return size, nil
}
func (f *antiFlow) close() {
	f.mu.Lock()
	f.closed = true
	a, b := f.active, f.reserve
	f.flushStalls(a, true)
	f.socket.group.history.journal.end(a, "closed", false)
	f.active = nil
	f.reserve = nil
	f.mu.Unlock()
	if a != nil {
		a.Close()
	}
	if b != nil {
		b.Close()
	}
	f.socket.group.mu.Lock()
	delete(f.socket.group.flows, f.id)
	f.socket.group.mu.Unlock()
}
func (s *antiSocket) Close() error {
	s.cancel()
	s.mu.Lock()
	for _, f := range s.flows {
		f.close()
	}
	s.flows = map[netip.AddrPort]*antiFlow{}
	s.mu.Unlock()
	s.group.mu.Lock()
	delete(s.group.parents, s)
	s.group.mu.Unlock()
	return nil
}
func (s *antiSocket) LocalAddr() net.Addr               { return net.UDPAddrFromAddrPort(s.meta.SourceAddrPort()) }
func (s *antiSocket) SetReadDeadline(t time.Time) error { s.readDeadline.Set(t); return nil }
func (s *antiSocket) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeDeadline = t
	s.writeClock.Set(t)
	for _, f := range s.flows {
		f.mu.Lock()
		if f.active != nil {
			f.active.SetWriteDeadline(t)
		}
		f.mu.Unlock()
	}
	return nil
}
func (s *antiSocket) SetDeadline(t time.Time) error {
	s.SetReadDeadline(t)
	return s.SetWriteDeadline(t)
}
