package outboundgroup

import (
	"context"
	"encoding/binary"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/stretchr/testify/require"
)

type labNode struct {
	blocked   chan struct{}
	release   chan struct{}
	blockOnce sync.Once
	C.Proxy
	base      *outbound.Direct
	blackhole atomic.Bool
	mu        sync.Mutex
	sockets   []*net.UDPConn
}
type labSocket struct {
	*net.UDPConn
	node *labNode
}

func (c *labSocket) WriteTo(b []byte, a net.Addr) (int, error) {
	if c.node.blocked != nil {
		c.node.blockOnce.Do(func() { close(c.node.blocked) })
		<-c.node.release
		return 0, net.ErrClosed
	}
	if c.node.blackhole.Load() {
		return len(b), nil
	}
	return c.UDPConn.WriteTo(b, a)
}

func TestAntiLagRetiresBlockedWrite(t *testing.T) {
	g, a, _ := labGroup(t, netip.AddrPort{})
	a.blocked = make(chan struct{})
	a.release = make(chan struct{})
	defer close(a.release)
	dest := labServer(t, false)
	ctx := context.Background()
	c, err := g.ListenPacketContext(ctx, labMeta(dest))
	require.NoError(t, err)
	defer c.Close()
	finished := make(chan error, 1)
	go func() { _, e := c.WriteTo([]byte("old"), net.UDPAddrFromAddrPort(dest)); finished <- e }()
	select {
	case <-a.blocked:
	case <-time.After(time.Second):
		t.Fatal("A did not enter WriteTo")
	}
	require.NoError(t, g.Failover(ctx, g.Bindings()[0].ID, "B"))
	select {
	case e := <-finished:
		require.NoError(t, e)
	case <-time.After(time.Second):
		t.Fatal("retired transport still blocks the sender")
	}
	require.Contains(t, exchange(t, c, dest, "new"), " new")
}
func (p *labNode) ListenPacketContext(ctx context.Context, m *C.Metadata) (C.PacketConn, error) {
	raw, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.sockets = append(p.sockets, raw)
	p.mu.Unlock()
	return outbound.NewPacketConn(&labSocket{raw, p}, p.base), nil
}

type labProvider struct {
	P.ProxyProvider
	nodes []C.Proxy
}

func TestAntiLagNewEvidenceChangesReserveOnly(t *testing.T) {
	g, a, b := labGroup(t, netip.AddrPort{})
	d := outbound.NewDirectWithOption(outbound.DirectOption{Name: "C"})
	third := &labNode{Proxy: adapter.NewProxy(d), base: d}
	g.providers[0] = labProvider{nodes: []C.Proxy{a, b, third}}
	dest := labServer(t, false)
	m := labMeta(dest)
	pc, err := g.ListenPacketContext(context.Background(), m)
	require.NoError(t, err)
	defer pc.Close()
	require.Eventually(t, func() bool { return g.Bindings()[0].Reserve == "B" }, 2*time.Second, 5*time.Millisecond)
	for i := 0; i < 12; i++ {
		g.history.event(m, "C", antiReply, nil, 15*time.Millisecond)
	}
	g.mu.Lock()
	var f *antiFlow
	for _, route := range g.flows {
		f = route
	}
	g.mu.Unlock()
	f.mu.Lock()
	active := f.active
	f.mu.Unlock()
	f.warm(active)
	binding := g.Bindings()[0]
	require.Equal(t, "A", binding.Active)
	require.Equal(t, "C", binding.Reserve)
	require.Equal(t, "C", binding.Candidates[0].Node)
	require.NoError(t, f.fail(active, "read-failure", 0))
	require.Equal(t, "C", g.Bindings()[0].Active)
	require.Contains(t, exchange(t, pc, dest, "game"), " game")
}

func (p labProvider) Version() uint32    { return 1 }
func (p labProvider) Proxies() []C.Proxy { return p.nodes }
func (p labProvider) Touch()             {}
func labGroup(t *testing.T, target netip.AddrPort) (*AntiLag, *labNode, *labNode) {
	t.Helper()
	node := func(name string) *labNode {
		d := outbound.NewDirectWithOption(outbound.DirectOption{Name: name})
		return &labNode{Proxy: adapter.NewProxy(d), base: d}
	}
	a, b := node("A"), node("B")
	ctx, cancel := context.WithCancel(context.Background())
	g := &AntiLag{GroupBase: NewGroupBase(GroupBaseOption{Name: "experiment", Type: C.AntiLag, EmptyFallback: a, Providers: []P.ProxyProvider{labProvider{nodes: []C.Proxy{a, b}}}}), ctx: ctx, cancel: cancel, url: C.DefaultTestURL, flows: map[string]*antiFlow{}, parents: map[*antiSocket]bool{}, measurements: map[netip.AddrPort]*antiRegion{}, history: newAntiHistory(nil, "lab", "experiment")}
	if target.IsValid() {
		g.targets = []AntiLagTarget{{endpoint: target}}
	}
	t.Cleanup(func() { g.Close() })
	return g, a, b
}
func labServer(t *testing.T, stun bool) netip.AddrPort {
	t.Helper()
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	go func() {
		buf := make([]byte, 65535)
		for {
			n, from, e := c.ReadFromUDPAddrPort(buf)
			if e != nil {
				return
			}
			if stun {
				if n != 20 {
					continue
				}
				binary.BigEndian.PutUint16(buf, 0x101)
				c.WriteToUDPAddrPort(buf[:n], from)
			} else {
				c.WriteToUDPAddrPort(append([]byte(from.String()+" "), buf[:n]...), from)
			}
		}
	}()
	return c.LocalAddr().(*net.UDPAddr).AddrPort()
}
func labMeta(ap netip.AddrPort) *C.Metadata {
	return &C.Metadata{NetWork: C.UDP, DstIP: ap.Addr(), DstPort: ap.Port()}
}
func exchange(t *testing.T, c net.PacketConn, to netip.AddrPort, msg string) string {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(4*time.Second)))
	_, err := c.WriteTo([]byte(msg), net.UDPAddrFromAddrPort(to))
	require.NoError(t, err)
	var buf [8192]byte
	n, _, err := c.ReadFrom(buf[:])
	require.NoError(t, err)
	return string(buf[:n])
}
func TestAntiLagSwitch(t *testing.T) {
	g, a, _ := labGroup(t, netip.AddrPort{})
	x, y := labServer(t, false), labServer(t, false)
	ctx := context.Background()
	c, err := g.ListenPacketContext(ctx, labMeta(x))
	require.NoError(t, err)
	defer c.Close()
	before := exchange(t, c, x, "x")
	other := exchange(t, c, y, "y")
	var id string
	for _, v := range g.Bindings() {
		if v.Destination == x.String() {
			id = v.ID
		}
	}
	require.NotEmpty(t, id)
	require.Error(t, g.Failover(ctx, id, "missing"))
	require.Equal(t, before, exchange(t, c, x, "x"))
	require.NoError(t, g.Failover(ctx, id, "B"))
	after := exchange(t, c, x, "x")
	require.NotEqual(t, before, after)
	require.NoError(t, g.Failover(ctx, id, "B"))
	require.Equal(t, after, exchange(t, c, x, "x"))
	require.Equal(t, other, exchange(t, c, y, "y"))
	require.Len(t, g.Bindings(), 2)
	a.mu.Lock()
	all := append([]*net.UDPConn(nil), a.sockets...)
	a.mu.Unlock()
	for _, raw := range all {
		raw.Close()
	}
	require.Eventually(t, func() bool {
		for _, v := range g.Bindings() {
			if v.Destination == y.String() {
				return v.Active == "B"
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond)
	require.NotEqual(t, other, exchange(t, c, y, "y"))
	require.NoError(t, c.Close())
	require.Empty(t, g.Bindings())
}
func TestAntiLagSilentFailure(t *testing.T) {
	target := labServer(t, true)
	g, a, b := labGroup(t, target)
	dest := labServer(t, false)
	c, e := g.ListenPacketContext(context.Background(), labMeta(dest))
	require.NoError(t, e)
	defer c.Close()
	a.blackhole.Store(true)
	b.blackhole.Store(true)
	time.Sleep(4500 * time.Millisecond)
	require.Equal(t, "A", g.Bindings()[0].Active)
	b.blackhole.Store(false)
	require.Eventually(t, func() bool { return g.Bindings()[0].Active == "B" }, 8*time.Second, 20*time.Millisecond)
	require.Contains(t, exchange(t, c, dest, "ok"), " ok")
	require.Equal(t, "probe-timeout", g.Bindings()[0].Reason)
}
