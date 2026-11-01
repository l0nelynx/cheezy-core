package statistic

import (
	"sync"
	"sync/atomic"
	"time"
)

// UDPDropReason describes a local discard, never an inferred network loss.
type UDPDropReason int

const (
	UDPIngressQueueFull UDPDropReason = iota
	UDPSenderQueueFull
	UDPQueueExpired
	UDPSenderClosed
	UDPResolveError
	UDPWriteError
	UDPWriteBackError
	udpDropReasonCount
)

var udpDropNames = [...]string{
	"ingressQueueFull", "senderQueueFull", "queueExpired", "senderClosed",
	"resolveError", "writeError", "writeBackError",
}

var udpDurationBounds = [...]time.Duration{
	100 * time.Microsecond, 500 * time.Microsecond, time.Millisecond,
	2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond,
	20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond,
	250 * time.Millisecond, time.Second,
}

type udpDurationHistogram struct {
	buckets [len(udpDurationBounds) + 1]atomic.Uint64
	totalUS atomic.Uint64
	maxUS   atomic.Uint64
}

// UDPDurationBucket counts observations up to UpperBoundUS, cumulatively.
// A nil bound is the final, unbounded bucket.
type UDPDurationBucket struct {
	UpperBoundUS *int64 `json:"leUs"`
	Count        uint64 `json:"count"`
}

type UDPDurationSnapshot struct {
	Count   uint64              `json:"count"`
	TotalUS uint64              `json:"totalUs"`
	MaxUS   uint64              `json:"maxUs"`
	Buckets []UDPDurationBucket `json:"buckets"`
}

func (h *udpDurationHistogram) observe(elapsed time.Duration) {
	if elapsed < 0 {
		elapsed = 0
	}
	index := len(udpDurationBounds)
	for i, bound := range udpDurationBounds {
		if elapsed <= bound {
			index = i
			break
		}
	}
	us := uint64(elapsed / time.Microsecond)
	h.totalUS.Add(us)
	for old := h.maxUS.Load(); us > old; old = h.maxUS.Load() {
		if h.maxUS.CompareAndSwap(old, us) {
			break
		}
	}
	h.buckets[index].Add(1)
}

func (h *udpDurationHistogram) snapshot() UDPDurationSnapshot {
	snapshot := UDPDurationSnapshot{
		TotalUS: h.totalUS.Load(), MaxUS: h.maxUS.Load(),
		Buckets: make([]UDPDurationBucket, len(h.buckets)),
	}
	for i := range h.buckets {
		snapshot.Count += h.buckets[i].Load()
		snapshot.Buckets[i].Count = snapshot.Count
		if i < len(udpDurationBounds) {
			us := int64(udpDurationBounds[i] / time.Microsecond)
			snapshot.Buckets[i].UpperBoundUS = &us
		}
	}
	return snapshot
}

// UDPDiagnostics is one process-wide measurement session. It retains no
// addresses, payloads or per-flow state. In-flight observations may finish
// after disabling a session, but cannot enter a later session.
type UDPDiagnostics struct {
	enabled       atomic.Bool
	startedAt     time.Time
	drops         [udpDropReasonCount]atomic.Uint64
	ingressQueue  udpDurationHistogram
	senderQueue   udpDurationHistogram
	outboundWrite udpDurationHistogram
}

type UDPDiagnosticsSnapshot struct {
	Enabled       bool                `json:"enabled"`
	StartedAt     *time.Time          `json:"startedAt"`
	Drops         map[string]uint64   `json:"drops"`
	IngressQueue  UDPDurationSnapshot `json:"ingressQueue"`
	SenderQueue   UDPDurationSnapshot `json:"senderQueue"`
	OutboundWrite UDPDurationSnapshot `json:"outboundWrite"`
}

var (
	udpDiagnosticsMu sync.Mutex
	udpDiagnostics   atomic.Pointer[UDPDiagnostics]
)

// SetUDPDiagnosticsEnabled starts a fresh session on a false -> true
// transition. Repeated enable requests preserve the running session.
func SetUDPDiagnosticsEnabled(enabled bool) {
	udpDiagnosticsMu.Lock()
	defer udpDiagnosticsMu.Unlock()
	current := udpDiagnostics.Load()
	if enabled {
		if current == nil || !current.enabled.Load() {
			current = &UDPDiagnostics{startedAt: time.Now()}
			current.enabled.Store(true)
			udpDiagnostics.Store(current)
		}
	} else if current != nil {
		current.enabled.Store(false)
	}
}

func ActiveUDPDiagnostics() *UDPDiagnostics {
	current := udpDiagnostics.Load()
	if current != nil && current.enabled.Load() {
		return current
	}
	return nil
}

func SnapshotUDPDiagnostics() UDPDiagnosticsSnapshot {
	current := udpDiagnostics.Load()
	if current == nil {
		current = &UDPDiagnostics{}
	}
	snapshot := UDPDiagnosticsSnapshot{
		Enabled:       current.enabled.Load(),
		Drops:         make(map[string]uint64, udpDropReasonCount),
		IngressQueue:  current.ingressQueue.snapshot(),
		SenderQueue:   current.senderQueue.snapshot(),
		OutboundWrite: current.outboundWrite.snapshot(),
	}
	if !current.startedAt.IsZero() {
		startedAt := current.startedAt
		snapshot.StartedAt = &startedAt
	}
	for i, name := range udpDropNames {
		snapshot.Drops[name] = current.drops[i].Load()
	}
	return snapshot
}

func (d *UDPDiagnostics) Drop(reason UDPDropReason) {
	if d != nil {
		d.drops[reason].Add(1)
	}
}

func (d *UDPDiagnostics) ObserveIngressQueue(elapsed time.Duration) {
	d.ingressQueue.observe(elapsed)
}

func (d *UDPDiagnostics) ObserveSenderQueue(elapsed time.Duration) {
	d.senderQueue.observe(elapsed)
}

func (d *UDPDiagnostics) ObserveOutboundWrite(elapsed time.Duration) {
	d.outboundWrite.observe(elapsed)
}
