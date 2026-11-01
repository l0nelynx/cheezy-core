package outboundgroup

import (
	"math"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

type stallDelta struct {
	Active time.Duration
	Excess time.Duration
	Events uint64
}

// One meter per path generation. A send continues an open gap even if no
// reply ever arrives; a switch starts the new path with no inherited gap.
type stallMeter struct {
	event, rx, tx time.Time
	typical       time.Duration
	intervals     int
	inEpisode     bool
	pending       stallDelta
}

func (s *stallMeter) observe(now time.Time, receive bool) {
	if now.Before(s.event) {
		now = s.event
	}
	gap := now.Sub(s.rx)
	threshold := 4 * s.typical
	if threshold < 150*time.Millisecond {
		threshold = 150 * time.Millisecond
	}
	limit := threshold
	if s.intervals < 8 {
		limit = time.Second
	}
	continuous := !s.tx.IsZero() && now.Sub(s.tx) <= limit
	if !continuous {
		s.rx = time.Time{}
		s.typical = 0
		s.intervals = 0
		s.inEpisode = false
	} else if s.intervals >= 8 && !s.rx.IsZero() {
		s.pending.Active += now.Sub(s.event)
		if gap > threshold && s.tx.After(s.rx) {
			if !s.inEpisode {
				s.pending.Events++
				s.inEpisode = true
			}
			start := s.rx.Add(threshold)
			if start.Before(s.event) {
				start = s.event
			}
			s.pending.Excess += now.Sub(start)
		}
	}
	if receive {
		if s.intervals < 8 && gap > 250*time.Millisecond {
			s.intervals = 0
			s.typical = 0
		}
		if !s.rx.IsZero() && gap > 0 && gap <= 250*time.Millisecond && !s.inEpisode {
			if s.intervals == 0 {
				s.typical = gap
			} else {
				s.typical += (gap - s.typical) / 8
			}
			if s.intervals < 8 {
				s.intervals++
			}
		}
		s.rx = now
		s.inEpisode = false
	} else {
		s.tx = now
	}
	s.event = now
}

// Time buckets bound the age and size of the persisted exact-destination data.
const stallBucketSpan = 5 * time.Minute

type stallBucket struct {
	Slot int64
	stallDelta
}
type stallHistory [6]stallBucket
type antiStallEvidence struct {
	ActiveSeconds float64 `json:"activeSeconds"`
	Events        uint64  `json:"events"`
	ExcessSeconds float64 `json:"excessSeconds"`
	PerMinute     float64 `json:"perActiveMinute"`
	Penalty       float64 `json:"penalty"`
}

func (w *stallHistory) add(d stallDelta, now time.Time) {
	slot := now.Unix() / int64(stallBucketSpan/time.Second)
	b := &w[slot%int64(len(w))]
	if b.Slot != slot {
		if b.Slot > slot {
			return
		}
		*b = stallBucket{Slot: slot}
	}
	b.Active += d.Active
	b.Excess += d.Excess
	b.Events += d.Events
}
func (w *stallHistory) evidence(now time.Time) *antiStallEvidence {
	if w == nil {
		return nil
	}
	slot := now.Unix() / int64(stallBucketSpan/time.Second)
	var total stallDelta
	for _, b := range w {
		if b.Slot <= slot && slot-b.Slot < int64(len(w)) {
			total.Active += b.Active
			total.Events += b.Events
			total.Excess += b.Excess
		}
	}
	if total.Active <= 0 {
		return nil
	}
	q := &antiStallEvidence{ActiveSeconds: total.Active.Seconds(), Events: total.Events, ExcessSeconds: total.Excess.Seconds(), PerMinute: float64(total.Events) / total.Active.Minutes()}
	if q.ActiveSeconds >= 20 {
		q.Penalty = math.Min(80, 3*q.PerMinute+200*q.ExcessSeconds/q.ActiveSeconds) * q.ActiveSeconds / (q.ActiveSeconds + 60)
	}
	return q
}

// Both methods run under f.mu. Merge into memory at coarse intervals and
// before retirement so late packets cannot add evidence to a new generation.
func (f *antiFlow) observeStalls(p *antiPath, now time.Time, receive bool) {
	if p.stalls.pending.Active > 0 && now.Sub(p.stalls.event) > time.Second {
		f.flushStalls(p, true)
	}
	p.stalls.observe(now, receive)
	f.flushStalls(p, false)
}

func (f *antiFlow) flushStalls(p *antiPath, force bool) {
	if p == nil || p.stalls.pending.Active <= 0 || !force && p.stalls.pending.Active < 5*time.Second {
		return
	}
	f.socket.group.history.addStalls(f.meta, p.name, p.stalls.pending, p.stalls.event)
	p.stalls.pending = stallDelta{}
}
func (h *antiHistory) addStalls(m *C.Metadata, node string, d stallDelta, now time.Time) {
	key := antiKeys(m, node)[0]
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.records[key]
	if r.Stalls == nil {
		r.Stalls = &stallHistory{}
	}
	r.Stalls.add(d, now)
	if now.After(r.Seen) {
		r.Seen = now
	}
	h.records[key] = r
	h.trim()
	h.dirty = true
}
