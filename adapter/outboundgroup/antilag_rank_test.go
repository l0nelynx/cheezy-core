package outboundgroup

import (
	"context"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/metacubex/bbolt"
	C "github.com/metacubex/mihomo/constant"
	"github.com/stretchr/testify/require"
)

func TestAntiLagHistoryIsolation(t *testing.T) {
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "cache.db"), 0600, nil)
	require.NoError(t, err)
	defer db.Close()
	h := newAntiHistory(db, "first", "game")
	m := labMeta(netip.MustParseAddrPort("192.0.2.9:1000"))
	m.Process = "game"
	m.DstIPASN = "64500"
	m.DestinationRegion, m.GeoSource = "eu-central-1", "aws"
	h.event(m, "B", antiDial, nil, 0)
	require.Zero(t, h.evaluate(m, "B").tier, "opening a UDP socket is not a reply")
	h.event(m, "B", antiReply, nil, 30*time.Millisecond)
	h.flush()
	h = newAntiHistory(db, "first", "game")
	require.Equal(t, 3, h.evaluate(m, "B").tier)
	require.Zero(t, newAntiHistory(db, "second", "game").evaluate(m, "B").tier)
	nearby := m.Clone()
	nearby.DstIP = netip.MustParseAddr("192.0.2.10")
	require.Equal(t, 2, h.evaluate(nearby, "B").tier)
	nearby.DstIP = netip.MustParseAddr("198.51.100.1")
	require.Equal(t, 1, h.evaluate(nearby, "B").tier)
	nearby.NetWork = C.TCP
	require.Zero(t, h.evaluate(nearby, "B").tier)
	h.event(m, "B", antiFailure, nil, 0)
	h.event(m, "B", antiFailure, nil, 0)
	require.True(t, h.evaluate(m, "B").failed)
	for k, r := range h.records {
		r.Seen = time.Now().Add(-8 * 24 * time.Hour)
		h.records[k] = r
	}
	h.flush()
	require.Empty(t, newAntiHistory(db, "first", "game").records)
}
func TestAntiLagProbeConfidenceAndRestore(t *testing.T) {
	target := netip.MustParseAddrPort("127.0.0.1:3478")
	g, _, _ := labGroup(t, target)
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "probes.db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { g.Close(); g.history.db = nil; db.Close() })
	g.history = newAntiHistory(db, "lab", "routes")
	r := &antiRegion{samples: map[string][]probeSample{}}
	g.measurements[target] = r
	for i := 0; i < 19; i++ {
		r.record("A", probeSample{time.Now(), 80 * time.Millisecond, true})
		r.record("B", probeSample{time.Now(), 40 * time.Millisecond, true})
	}
	r.compare("A", "B")
	require.Empty(t, r.winner)
	r.record("A", probeSample{time.Now(), 80 * time.Millisecond, true})
	r.record("B", probeSample{time.Now(), 40 * time.Millisecond, true})
	r.compare("A", "B")
	require.Equal(t, "A", r.winner)
	r.compare("B", "A")
	require.Equal(t, "A", r.winner, "the same samples cannot count as two evaluations")
	r.record("A", probeSample{time.Now(), 80 * time.Millisecond, true})
	r.record("B", probeSample{time.Now(), 40 * time.Millisecond, true})
	r.compare("A", "B")
	require.Equal(t, "B", r.winner)
	require.Equal(t, "B", g.candidates(labMeta(target), false)[0].proxy.Name())
	g.saveRegions()
	g.measurements = map[netip.AddrPort]*antiRegion{}
	g.loadRegions()
	require.Equal(t, "B", g.measurements[target].winner)
	require.Equal(t, 21, g.measurements[target].evidence("A").Replies)
}
func TestAntiLagFailedPreparationPreservesSocket(t *testing.T) {
	g, _, _ := labGroup(t, netip.AddrPort{})
	dest := labServer(t, false)
	c, err := g.ListenPacketContext(context.Background(), labMeta(dest))
	require.NoError(t, err)
	defer c.Close()
	before := exchange(t, c, dest, "still-live")
	id := g.Bindings()[0].ID
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, g.Failover(ctx, id, "B"))
	require.Equal(t, before, exchange(t, c, dest, "still-live"))
}

func TestAntiLagLearnsDifferentExitsAndReopensHistory(t *testing.T) {
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "routes"), 0600, nil)
	require.NoError(t, err)
	defer db.Close()
	_, a, b := labGroup(t, netip.AddrPort{})
	h := newAntiHistory(db, "lab", "routes")
	x := labMeta(netip.MustParseAddrPort("192.0.2.10:1234"))
	y := labMeta(netip.MustParseAddrPort("192.0.2.20:1234"))
	rank := func(m *C.Metadata) []antiCandidate {
		ca, cb := h.evaluate(m, "A"), h.evaluate(m, "B")
		ca.proxy, cb.proxy = a, b
		ca.delay, cb.delay = 80, 40
		cs := []antiCandidate{ca, cb}
		rankAntiCandidates(cs, nil, "")
		return cs
	}
	h.event(x, "A", antiReply, nil, time.Millisecond)
	require.Equal(t, "B", rank(x)[0].proxy.Name(), "one response must not lock in A")
	for i := 0; i < 12; i++ {
		h.event(x, "A", antiReply, nil, 20*time.Millisecond)
		h.event(x, "B", antiReply, nil, 100*time.Millisecond)
		h.event(y, "A", antiReply, nil, 100*time.Millisecond)
		h.event(y, "B", antiReply, nil, 20*time.Millisecond)
	}
	h.cadence(x, "B", 90, time.Now())
	h.flush()
	h = newAntiHistory(db, "lab", "routes")
	require.Equal(t, "A", rank(x)[0].proxy.Name())
	require.Equal(t, "B", rank(y)[0].proxy.Name())
	require.Positive(t, h.evaluate(x, "B").cadenceWeight)
	for k, r := range h.records {
		r.LastReply = r.LastReply.Add(-5 * 24 * time.Hour)
		r.LastCadence = r.LastCadence.Add(-5 * 24 * time.Hour)
		h.records[k] = r
	}
	require.Equal(t, "B", rank(x)[0].proxy.Name(), "stale evidence must yield to a fresh prior")
}

func TestAntiLagCadenceMeasuresActiveWindows(t *testing.T) {
	var w cadenceWindow
	now := time.Now()
	var spread float64
	var complete bool
	for i := 0; !complete && i < 600; i++ {
		gap := 16 * time.Millisecond
		if i%10 == 0 {
			gap = 75 * time.Millisecond
		}
		next := now.Add(gap)
		spread, complete = w.observe(next, now, now)
		now = next
	}
	require.True(t, complete)
	require.Equal(t, float64(59), spread)
	_, complete = w.observe(now.Add(2*time.Second), now, now)
	require.False(t, complete)
	require.Zero(t, w.count)
	_, complete = w.observe(now.Add(time.Millisecond), now, now.Add(-time.Minute))
	require.False(t, complete)
	require.Zero(t, w.count)
}

func TestAntiLagExplorationBudget(t *testing.T) {
	_, a, b := labGroup(t, netip.AddrPort{})
	list := []antiCandidate{{proxy: a, score: 50, starts: 6}, {proxy: b, score: 62}}
	exploreAntiCandidate(list)
	require.Equal(t, a, list[0].proxy)
	list[0].starts++
	list[1].failed = true
	exploreAntiCandidate(list)
	require.Equal(t, a, list[0].proxy)
	list[1].failed = false
	list[1].score = 180
	exploreAntiCandidate(list)
	require.Equal(t, a, list[0].proxy)
	list[1].score = 62
	exploreAntiCandidate(list)
	require.Equal(t, b, list[0].proxy)
	require.Equal(t, "destination-trial", list[0].reason)
}
