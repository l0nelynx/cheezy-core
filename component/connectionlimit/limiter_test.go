package connectionlimit

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLimiterQueuesAndReleases(t *testing.T) {
	limiter, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	releaseFirst, err := limiter.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	acquired := make(chan func(), 1)
	go func() {
		release, acquireErr := limiter.Acquire(context.Background())
		if acquireErr != nil {
			return
		}
		acquired <- release
	}()

	time.Sleep(20 * time.Millisecond)
	if snapshot := limiter.Snapshot(); snapshot.Active != 1 || snapshot.Waiting != 1 {
		t.Fatalf("unexpected snapshot while queued: %+v", snapshot)
	}
	releaseFirst()

	select {
	case releaseSecond := <-acquired:
		releaseSecond()
	case <-time.After(time.Second):
		t.Fatal("queued acquisition did not resume")
	}
	if snapshot := limiter.Snapshot(); snapshot.Active != 0 || snapshot.Waiting != 0 {
		t.Fatalf("unexpected final snapshot: %+v", snapshot)
	}
}

func TestLimiterCancellation(t *testing.T) {
	limiter, _ := New(1)
	release, _ := limiter.Acquire(context.Background())
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := limiter.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestRegistrySharesLimiterAndRejectsLimitChange(t *testing.T) {
	registry := NewRegistry()
	first, err := registry.Get("wap", 7)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Get("wap", 7)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("expected the limiter to be shared")
	}
	if _, err := registry.Get("wap", 8); err == nil {
		t.Fatal("expected a changed limit to be rejected")
	}
}
