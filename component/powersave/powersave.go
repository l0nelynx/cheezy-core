// Package powersave tells the core's periodic work what the device is doing,
// so that it does less when nobody is looking: health checks and idle QUIC
// sessions wake the radio on their own, which on a phone costs more than the
// work itself.
//
// The host sets the mode (a phone app from screen and doze broadcasts); a host
// that never does stays Active and nothing changes. What each mode means is up
// to the code reading it; see adapter/provider/healthcheck.go.
//
// Built without the with_powersave tag, Set does nothing: the core stays
// Active whatever the host says.
package powersave

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/metacubex/mihomo/constant/features"
	"github.com/metacubex/mihomo/log"
)

type Mode int32

const (
	// Active: someone uses the device. Everything runs as configured.
	Active Mode = iota
	// Background: the screen is off. Periodic work slows down.
	Background
	// Idle: the device is idle (Android doze). Periodic work waits.
	Idle
)

func (m Mode) String() string {
	switch m {
	case Active:
		return "active"
	case Background:
		return "background"
	case Idle:
		return "idle"
	default:
		return "unknown"
	}
}

func (m *Mode) UnmarshalText(data []byte) error {
	switch string(data) {
	case "active":
		*m = Active
	case "background":
		*m = Background
	case "idle":
		*m = Idle
	default:
		return errors.New("power-save mode must be active, background or idle")
	}
	return nil
}

var (
	mode    atomic.Int32
	mu      sync.Mutex
	changed = make(chan struct{})
)

// Set switches the mode; readers waiting on Changed wake up.
func Set(m Mode) {
	if !features.WithPowerSave || m < Active || m > Idle {
		return
	}

	mu.Lock()
	if Mode(mode.Swap(int32(m))) == m {
		mu.Unlock()
		return
	}

	close(changed)
	changed = make(chan struct{})
	mu.Unlock()

	log.Debugln("[PowerSave] %s", m)
}

// Current is the mode now.
func Current() Mode {
	return Mode(mode.Load())
}

// Changed is closed on the next change of mode: take it before reading
// Current, then wait on it.
func Changed() <-chan struct{} {
	mu.Lock()
	defer mu.Unlock()

	return changed
}
