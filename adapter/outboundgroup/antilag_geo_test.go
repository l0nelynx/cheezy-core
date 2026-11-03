package outboundgroup

import (
	"context"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/stretchr/testify/require"
)

func TestAntiLagBundledRegions(t *testing.T) {
	loadBundledAWS()
	g, _, _ := labGroup(t, netip.AddrPort{})
	for _, tc := range []struct{ addr, region, source string }{
		{"18.197.183.182:35530", "eu-central-1", "aws"},
		{"43.198.248.30:35570", "ap-east-1", "aws"},
		{"75.2.9.96:41536", "global", "aws"},
		{"127.0.0.1:1234", "unknown", "unknown"},
	} {
		m := labMeta(netip.MustParseAddrPort(tc.addr))
		g.enrichGeo(m)
		require.Equal(t, tc.region, m.DestinationRegion)
		require.Equal(t, tc.source, m.GeoSource)
		if tc.region == "global" {
			require.Empty(t, m.DestinationCountry)
		}
	}
}

func TestAntiLagRegionTransfer(t *testing.T) {
	h := newAntiHistory(nil, "lab", "game")
	m := labMeta(netip.MustParseAddrPort("18.197.183.182:35530"))
	m.Process = "game"
	m.DstIPASN = "16509"
	m.GeoSource = "aws"
	m.DestinationRegion = "eu-central-1"
	h.event(m, "A", antiReply, nil, 40*time.Millisecond)
	h.addStalls(m, "A", stallDelta{Active: time.Minute, Excess: time.Second, Events: 3}, time.Now())
	other := m.Clone()
	other.DstIP = netip.MustParseAddr("18.198.1.1")
	other.DstPort++
	c := h.evaluate(other, "A")
	require.Equal(t, "regional-asn-history", c.reason)
	require.Positive(t, c.regionStallCost)
	require.Nil(t, c.stalls)
	other.DstIPASN = "different"
	require.Equal(t, "region-history", h.evaluate(other, "A").reason)
	other.DstIPASN = m.DstIPASN
	for _, region := range []string{"ap-east-1", "global", "unknown"} {
		other.DestinationRegion = region
		c = h.evaluate(other, "A")
		require.Nil(t, c.historyEvidence)
		require.Zero(t, c.regionStallCost)
	}
}

func TestAntiLagClassificationPerBinding(t *testing.T) {
	loadBundledAWS()
	g, _, _ := labGroup(t, netip.AddrPort{})
	eu := netip.MustParseAddrPort("18.197.183.182:35530")
	hk := netip.MustParseAddrPort("43.198.248.30:35570")
	anycast := netip.MustParseAddrPort("75.2.9.96:41536")
	s := newAntiSocket(g, labMeta(eu))
	defer s.Close()
	for _, dest := range []netip.AddrPort{eu, hk, anycast} {
		_, err := s.binding(context.Background(), dest)
		require.NoError(t, err)
	}
	want := map[string]string{eu.String(): "eu-central-1", hk.String(): "ap-east-1", anycast.String(): "global"}
	require.Len(t, g.Bindings(), len(want))
	for _, b := range g.Bindings() {
		require.Equal(t, want[b.Destination], b.DestinationRegion)
	}
	rows := g.Decisions()
	require.Len(t, rows, len(want))
	for _, d := range rows {
		require.Equal(t, want[d.Destination], d.DestinationRegion)
		require.Equal(t, "aws", d.GeoSource)
	}
}

func TestAntiLagGeoTiebreak(t *testing.T) {
	g, _, _ := labGroup(t, netip.AddrPort{})
	g.exits = smart.NewExitWatcher(smart.ExitWatcherOptions{})
	g.exits.Store("B", smart.ExitInfo{Region: "hk"}, time.Now())
	m := labMeta(netip.MustParseAddrPort("43.198.248.30:35570"))
	m.DestinationCountry = "HK"
	c := g.candidates(m, false)
	require.Equal(t, "B", c[0].proxy.Name())
	require.Equal(t, 2.0, c[0].geoCredit)
	require.Equal(t, 2.0, c[1].score-c[0].score)
	g.history.event(m, "B", antiReply, nil, time.Second)
	c = g.candidates(m, false)
	require.Equal(t, "A", c[0].proxy.Name())
	require.Zero(t, c[1].geoCredit)
	m.DestinationCountry = ""
	require.Zero(t, g.candidates(m, false)[0].geoCredit)
}

func TestAntiLagJournalSnapshotsAndRestore(t *testing.T) {
	g, _, _ := labGroup(t, netip.AddrPort{})
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "journal"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { g.Close(); db.Close() })
	g.history = newAntiHistory(db, "first", "routes")
	dest := labServer(t, false)
	ctx := context.Background()
	sock, err := g.ListenPacketContext(ctx, labMeta(dest))
	require.NoError(t, err)
	defer sock.Close()
	exchange(t, sock, dest, "A")
	require.NoError(t, g.Failover(ctx, g.Bindings()[0].ID, "B"))
	exchange(t, sock, dest, "B")
	decisions := g.Decisions()
	require.Len(t, decisions, 2)
	require.Equal(t, "manual", decisions[1].Status)
	require.False(t, decisions[1].ConfirmedFailure)
	require.NotNil(t, decisions[1].FirstResponseMS)
	require.Equal(t, "active", decisions[0].Status)
	require.Len(t, decisions[1].Candidates, 2)
	require.Zero(t, decisions[1].Candidates[0].Starts)
	require.Nil(t, decisions[1].Candidates[0].History)
	require.Equal(t, "unknown", decisions[1].DestinationRegion)
	g.history.flush()
	restored := newAntiHistory(db, "first", "routes")
	restored.loadJournal()
	require.Len(t, restored.journal.rows, 2)
	require.Equal(t, "interrupted", restored.journal.rows[1].Status)
	other := newAntiHistory(db, "other", "routes")
	other.loadJournal()
	require.Empty(t, other.journal.rows)
	for i := 0; i < antiJournalLimit+2; i++ {
		g.history.journal.add(&antiDecision{Status: "unavailable"})
	}
	require.Len(t, g.Decisions(), antiJournalLimit)
	require.Empty(t, g.Decisions()[antiJournalLimit-1].Binding)
}
