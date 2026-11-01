package outbound

import (
	"testing"
	"time"

	"github.com/metacubex/quic-go"
	"github.com/stretchr/testify/require"
)

func TestHysteria2OuterQUICWindow(t *testing.T) {
	now := time.Now()
	prev := quic.ConnectionStats{PacketsSent: 100, PacketsReceived: 200, PacketsLost: 5}
	next := quic.ConnectionStats{PacketsSent: 200, PacketsReceived: 280, PacketsLost: 8, MinRTT: 30 * time.Millisecond, SmoothedRTT: 80 * time.Millisecond}
	sample := func() float64 { return hyQUICWindow(prev, next, now, now.Add(5*time.Second), 3).Penalty }
	q := hyQUICWindow(prev, next, now, now.Add(5*time.Second), 3)
	require.EqualValues(t, 3, q.Generation)
	require.Equal(t, 50.0, q.InflationMS)
	require.True(t, q.RTTUsed)
	require.Equal(t, 0.03, *q.NetLossFraction)
	require.Equal(t, 65.0, q.Penalty)
	next.PacketsLost = 3
	require.Equal(t, 50.0, sample(), "spurious loss correction")
	next.PacketsReceived = prev.PacketsReceived
	require.Zero(t, sample(), "no traffic received to corroborate the RTT gauge")
	next.PacketsLost = 5 + 1000
	require.Equal(t, 100.0, sample())
	require.Zero(t, hyQUICWindow(prev, next, now, now.Add(20*time.Second), 3).Penalty)
	next.PacketsSent = prev.PacketsSent + 7
	require.Nil(t, hyQUICWindow(prev, next, now, now.Add(time.Second), 3).NetLossFraction)
	next.PacketsSent = 1
	require.Zero(t, sample())
}
