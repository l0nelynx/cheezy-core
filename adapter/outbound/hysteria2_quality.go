package outbound

import (
	"math"
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/quic-go"
)

// Reads use the atomic counters / RTT gauges in the pinned quic-go fork.
// Sampling is shared between all observers, independent of the number of flows.
type hyQUICMonitor struct {
	mu         sync.Mutex
	conn       *quic.Conn
	generation uint64
	previous   quic.ConnectionStats
	at         time.Time
	quality    C.OuterQUICQuality
}

func (m *hyQUICMonitor) attach(c *quic.Conn) {
	m.mu.Lock()
	m.conn = c
	m.generation++
	m.previous = c.ConnectionStats()
	m.at = time.Now()
	m.quality = hyQUICWindow(m.previous, m.previous, m.at, m.at, m.generation)
	m.mu.Unlock()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-c.Context().Done():
				m.mu.Lock()
				if m.conn == c {
					m.conn = nil
				}
				m.mu.Unlock()
				return
			case <-ticker.C:
				m.mu.Lock()
				if m.conn != c {
					m.mu.Unlock()
					return
				}
				now := time.Now()
				current := c.ConnectionStats()
				m.quality = hyQUICWindow(m.previous, current, m.at, now, m.generation)
				m.previous = current
				m.at = now
				m.mu.Unlock()
			}
		}
	}()
}

func (h *Hysteria2) OuterQUIC() *C.OuterQUICQuality {
	m := &h.quicMonitor
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn == nil || m.conn.Context().Err() != nil {
		return nil
	}
	if time.Since(m.at) > 15*time.Second {
		return nil
	}
	snapshot := m.quality
	return &snapshot
}

func hyQUICWindow(before, after quic.ConnectionStats, start, end time.Time, gen uint64) C.OuterQUICQuality {
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	elapsed := end.Sub(start)
	q := C.OuterQUICQuality{Generation: gen, SampledAt: end, IntervalMS: ms(elapsed), MinRTTMS: ms(after.MinRTT), SmoothedRTTMS: ms(after.SmoothedRTT), DeviationMS: ms(after.MeanDeviation)}
	if elapsed <= 0 || elapsed > 15*time.Second || after.PacketsSent < before.PacketsSent || after.PacketsReceived < before.PacketsReceived {
		return q
	}
	q.Sent = after.PacketsSent - before.PacketsSent
	q.Received = after.PacketsReceived - before.PacketsReceived
	if q.Sent < 8 {
		return q
	}
	if q.Received > 0 && after.MinRTT > 0 && after.SmoothedRTT > 0 {
		q.RTTUsed = true
		q.InflationMS = math.Max(0, q.SmoothedRTTMS-q.MinRTTMS)
		q.Penalty = math.Min(150, q.InflationMS)
	}
	// A spurious-loss correction can reduce PacketsLost. Attribute only
	// positive net growth and bound it when detections straddle windows.
	fraction := 0.0
	if after.PacketsLost > before.PacketsLost {
		fraction = math.Min(1, float64(after.PacketsLost-before.PacketsLost)/float64(q.Sent))
	}
	q.NetLossFraction = &fraction
	q.Penalty += math.Min(100, 500*fraction)
	q.Penalty *= math.Min(1, float64(q.Sent)/32)
	return q
}
