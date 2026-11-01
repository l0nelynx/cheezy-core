//go:build with_powersave

package log

import (
	"testing"
	"time"
)

func TestGate(t *testing.T) {
	defer SetLevel(Level())
	defer SetGated(false)

	SetLevel(WARNING)

	SetGated(false)
	if !enabled(DEBUG) {
		t.Fatal("ungated: every event is produced")
	}

	SetGated(true)
	if enabled(DEBUG) || enabled(INFO) {
		t.Fatal("gated: below the level nothing is produced")
	}
	if !enabled(WARNING) || !enabled(ERROR) {
		t.Fatal("gated: from the level up everything is produced")
	}

	release := Want(INFO)
	if !enabled(INFO) || enabled(DEBUG) {
		t.Fatal("wanted info: info is produced, debug is not")
	}
	release()
	release()
	if enabled(INFO) {
		t.Fatal("released: info is not produced, and a second release changes nothing")
	}
}

type countedLogValue struct{ calls int }

func (v *countedLogValue) String() string {
	v.calls++
	return "requested debug event"
}

func TestGatingSkipsFormattingAndHonorsReader(t *testing.T) {
	defer SetLevel(Level())
	defer SetGated(false)
	SetLevel(WARNING)
	SetGated(true)
	value := &countedLogValue{}
	Debugln("%s", value)
	if value.calls != 0 {
		t.Fatal("suppressed events must not be formatted")
	}
	sub := Subscribe()
	defer UnSubscribe(sub)
	release := Want(DEBUG)
	defer release()
	Debugln("%s", value)
	select {
	case event := <-sub:
		if value.calls != 1 || event.LogLevel != DEBUG || event.Payload != "requested debug event" {
			t.Fatalf("requested event missing or formatted more than once: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("an active reader must receive its requested level")
	}
}
