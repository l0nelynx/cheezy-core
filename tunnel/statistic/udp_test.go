package statistic

import (
	"sync"
	"testing"
	"time"
)

func TestUDPDurationHistogram(t *testing.T) {
	var histogram udpDurationHistogram
	for _, elapsed := range []time.Duration{-1, 100 * time.Microsecond, 101 * time.Microsecond, 2 * time.Second} {
		histogram.observe(elapsed)
	}
	snapshot := histogram.snapshot()
	if snapshot.Count != 4 || snapshot.TotalUS != 2000201 || snapshot.MaxUS != 2000000 {
		t.Fatalf("unexpected histogram: %+v", snapshot)
	}
	if snapshot.Buckets[0].Count != 2 || snapshot.Buckets[1].Count != 3 {
		t.Fatalf("incorrect inclusive bounds: %+v", snapshot.Buckets)
	}
	last := snapshot.Buckets[len(snapshot.Buckets)-1]
	if last.UpperBoundUS != nil || last.Count != 4 {
		t.Fatalf("incorrect overflow bucket: %+v", last)
	}
}

func TestUDPDiagnosticsConcurrentObservations(t *testing.T) {
	var diagnostics UDPDiagnostics
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for n := 0; n < 1000; n++ {
				diagnostics.Drop(UDPSenderQueueFull)
				diagnostics.ObserveSenderQueue(time.Millisecond)
				_ = diagnostics.senderQueue.snapshot()
			}
		}()
	}
	workers.Wait()
	snapshot := diagnostics.senderQueue.snapshot()
	if diagnostics.drops[UDPSenderQueueFull].Load() != 8000 || snapshot.Count != 8000 || snapshot.TotalUS != 8000000 {
		t.Fatalf("lost concurrent observations: %+v", snapshot)
	}
}

func TestUDPDiagnosticsSessions(t *testing.T) {
	SetUDPDiagnosticsEnabled(false)
	defer SetUDPDiagnosticsEnabled(false)
	if ActiveUDPDiagnostics() != nil {
		t.Fatal("diagnostics should be disabled")
	}
	SetUDPDiagnosticsEnabled(true)
	first := ActiveUDPDiagnostics()
	first.Drop(UDPQueueExpired)
	SetUDPDiagnosticsEnabled(true)
	if ActiveUDPDiagnostics() != first {
		t.Fatal("repeated enable reset the running session")
	}
	SetUDPDiagnosticsEnabled(false)
	if snapshot := SnapshotUDPDiagnostics(); snapshot.Enabled || snapshot.StartedAt == nil || snapshot.Drops["queueExpired"] != 1 {
		t.Fatalf("disabled session was not retained: %+v", snapshot)
	}
	SetUDPDiagnosticsEnabled(true)
	first.Drop(UDPQueueExpired) // an operation from the previous session finishes
	if ActiveUDPDiagnostics() == first || SnapshotUDPDiagnostics().Drops["queueExpired"] != 0 {
		t.Fatal("previous session contaminated the new session")
	}
}
