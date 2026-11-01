package outboundgroup

import (
	"math"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// Receive cadence is application-dependent. Compare it only across routes to
// the same endpoint during bidirectional activity. Silence is not packet loss.
var gapBoundsMS = [...]int{1, 2, 4, 8, 12, 16, 20, 25, 33, 50, 75, 100, 150, 250, 500, 1000}

type cadenceWindow struct {
	start time.Time
	bins  [len(gapBoundsMS)]uint32
	count uint32
}

func (w *cadenceWindow) observe(now, previous, lastWrite time.Time) (float64, bool) {
	gap := now.Sub(previous)
	if previous.IsZero() || lastWrite.IsZero() || now.Sub(lastWrite) > time.Second || gap < 0 || gap > time.Second {
		*w = cadenceWindow{}
		return 0, false
	}
	if w.start.IsZero() {
		w.start = previous
	}
	for i, bound := range gapBoundsMS {
		if gap <= time.Duration(bound)*time.Millisecond {
			w.bins[i]++
			break
		}
	}
	w.count++
	if now.Sub(w.start) < 10*time.Second {
		return 0, false
	}
	defer func() { *w = cadenceWindow{} }()
	if w.count < 32 {
		return 0, false
	}
	percentile := func(p uint64) int {
		target := (uint64(w.count)*p + 99) / 100
		var sum uint64
		for i, n := range w.bins {
			sum += uint64(n)
			if sum >= target {
				return gapBoundsMS[i]
			}
		}
		return gapBoundsMS[len(gapBoundsMS)-1]
	}
	return math.Min(100, float64(percentile(95)-percentile(50))), true
}

func (h *antiHistory) cadence(m *C.Metadata, node string, spread float64, now time.Time) {
	key := antiKeys(m, node)[0]
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.records[key]
	if !ok {
		return
	}
	mass := agedMass(r.CadenceMass, r.LastCadence, now)
	r.CadenceMS = (r.CadenceMS*mass + spread) / (1 + mass)
	r.CadenceMass = mass + 1
	r.CadenceWindows++
	r.LastCadence = now
	r.Seen = now
	h.records[key] = r
	h.dirty = true
}
