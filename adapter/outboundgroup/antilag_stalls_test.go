package outboundgroup

import (
	"context"
	"net"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	"github.com/stretchr/testify/require"
)

type labCarrier struct {
	*outbound.Direct
	quality *C.OuterQUICQuality
}

func (c *labCarrier) OuterQUIC() *C.OuterQUICQuality { return c.quality }

func TestAntiLagOuterQUICSharedAcrossDestinations(t *testing.T) {
	g, a, _ := labGroup(t, netip.AddrPort{})
	carrier := &labCarrier{Direct: a.base, quality: &C.OuterQUICQuality{Penalty: 50}}
	a.Proxy = adapter.NewProxy(outbound.NewAutoCloseProxyAdapter(carrier))
	for _, target := range []string{"192.0.2.1:1000", "192.0.2.2:2000"} {
		m := labMeta(netip.MustParseAddrPort(target))
		candidates := g.candidates(m, false)
		require.Equal(t, "B", candidates[0].proxy.Name())
		require.Equal(t, 50.0, candidates[1].score-candidates[0].score)
	}
	carrier.quality = nil
	require.Equal(t, "A", g.candidates(labMeta(netip.MustParseAddrPort("192.0.2.1:1000")), false)[0].proxy.Name())
}

func warmStallMeter(s *stallMeter, now time.Time, period time.Duration) time.Time {
	for i := 0; i < 20; i++ {
		now = now.Add(period)
		s.observe(now, false)
		s.observe(now.Add(time.Millisecond), true)
	}
	return now
}

func TestAntiLagStallsDuringActiveTraffic(t *testing.T) {
	var s stallMeter
	step := 20 * time.Millisecond
	now := warmStallMeter(&s, time.Now(), step)
	start := s.pending.Active
	for episode := 1; episode <= 2; episode++ {
		for i := 0; i < 50; i++ {
			now = now.Add(step)
			s.observe(now, false)
		}
		require.EqualValues(t, episode, s.pending.Events, "count the outage even before a reply arrives")
		s.observe(now.Add(time.Millisecond), true)
		require.Equal(t, time.Duration(episode)*850*time.Millisecond, s.pending.Excess)
		require.Equal(t, time.Duration(episode)*time.Second, s.pending.Active-start)
	}
}

func TestAntiLagStallsExcludeInactivity(t *testing.T) {
	var s stallMeter
	now := warmStallMeter(&s, time.Now(), 20*time.Millisecond)
	before := s.pending
	now = now.Add(750 * time.Millisecond)
	s.observe(now, false)
	s.observe(now.Add(time.Millisecond), true)
	require.Equal(t, before, s.pending)
	// A server with a regular 100ms tick tolerates an occasional 300ms gap.
	s = stallMeter{}
	now = warmStallMeter(&s, now, 100*time.Millisecond)
	for i := 0; i < 3; i++ {
		now = now.Add(100 * time.Millisecond)
		s.observe(now, false)
	}
	s.observe(now.Add(time.Millisecond), true)
	require.Zero(t, s.pending.Events)
	s = stallMeter{}
	for i := 0; i < 100; i++ {
		s.observe(now.Add(time.Duration(i)*time.Second), false)
	}
	require.Zero(t, s.pending)
}

func TestAntiLagStallsPersistAndRankOnlyExactPair(t *testing.T) {
	g, _, _ := labGroup(t, netip.AddrPort{})
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "quality.db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { g.Close(); db.Close() })
	g.history = newAntiHistory(db, "lab", "quality")
	dest := netip.MustParseAddrPort("192.0.2.7:7000")
	m := labMeta(dest)
	m.Process = "game"
	m.DstIPASN = "64500"
	pc, err := g.ListenPacketContext(context.Background(), m)
	require.NoError(t, err)
	defer pc.Close()
	now := time.Now()
	g.history.addStalls(m, "A", stallDelta{Active: time.Minute, Events: 4, Excess: time.Second}, now)
	g.history.flush()
	restored := newAntiHistory(db, "lab", "quality")
	require.Equal(t, 4.0, restored.evaluate(m, "A").stalls.PerMinute)
	require.Nil(t, restored.evaluate(m, "B").stalls)
	other := m.Clone()
	other.DstPort++
	require.Nil(t, restored.evaluate(other, "A").stalls)
	binding := g.Bindings()[0]
	require.Equal(t, "A", binding.Active)
	require.Equal(t, "B", binding.Candidates[0].Node)
	require.Positive(t, binding.Candidates[1].Stalls.Penalty)
	var window stallHistory
	window.add(stallDelta{Events: 1, Active: time.Second}, now)
	require.Zero(t, window.evidence(now).Penalty)
	require.Nil(t, window.evidence(now.Add(30*time.Minute)))
}

func TestAntiLagStallsFollowLivePath(t *testing.T) {
	g, a, _ := labGroup(t, netip.AddrPort{})
	dest := labServer(t, false)
	ctx := context.Background()
	pc, err := g.ListenPacketContext(ctx, labMeta(dest))
	require.NoError(t, err)
	defer pc.Close()
	for i := 0; i < 20; i++ {
		exchange(t, pc, dest, "play")
		time.Sleep(10 * time.Millisecond)
	}
	a.blackhole.Store(true)
	until := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(until) {
		_, err = pc.WriteTo([]byte("play"), net.UDPAddrFromAddrPort(dest))
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}
	id := g.Bindings()[0].ID
	require.NoError(t, g.Failover(ctx, id, "B"))
	exchange(t, pc, dest, "recovered")
	binding := g.Bindings()[0]
	require.Equal(t, "B", binding.Active)
	require.Len(t, binding.Candidates, 2)
	for _, candidate := range binding.Candidates {
		if candidate.Node == "A" {
			require.NotNil(t, candidate.Stalls)
			require.EqualValues(t, 1, candidate.Stalls.Events)
			require.Positive(t, candidate.Stalls.ExcessSeconds)
		} else {
			require.Nil(t, candidate.Stalls)
		}
	}
}
