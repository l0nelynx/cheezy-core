//go:build with_powersave

package statistic

import (
	"context"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/powersave"
)

func TestBackgroundTrafficWakesStatistics(t *testing.T) {
	defer powersave.Set(powersave.Active)
	for _, mode := range []powersave.Mode{powersave.Background, powersave.Idle} {
		t.Run(mode.String(), func(t *testing.T) {
			powersave.Set(mode)
			m := &Manager{trafficWake: make(chan struct{}, 1)}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				m.handle(ctx, 20*time.Millisecond)
			}()
			defer func() { cancel(); <-done }()
			wait := func(f func() bool) {
				t.Helper()
				deadline := time.Now().Add(3 * time.Second)
				for !f() {
					if time.Now().After(deadline) {
						t.Fatal("statistics did not reach the expected state")
					}
					time.Sleep(time.Millisecond)
				}
			}
			wait(m.paused.Load)
			m.PushUploaded(123)
			wait(func() bool { up, _ := m.Now(); return up == 123 })
			wait(m.paused.Load)
			m.PushDownloaded(456)
			wait(func() bool { _, down := m.Now(); return down == 456 })
			wait(m.paused.Load)
			// Waking via the mode change must not discard pending bytes either.
			m.PushUploaded(77)
			powersave.Set(powersave.Active)
			wait(func() bool { up, _ := m.Now(); return up == 77 })
			if up, down := m.Total(); up != 200 || down != 456 {
				t.Fatalf("totals = %d/%d, want 200/456", up, down)
			}
		})
	}
}
