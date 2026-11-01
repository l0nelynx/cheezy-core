//go:build with_powersave

package powersave

import "testing"

func TestChanged(t *testing.T) {
	defer Set(Active)

	ch := Changed()
	Set(Active)
	select {
	case <-ch:
		t.Fatal("the same mode again is no change")
	default:
	}

	Set(Idle)
	select {
	case <-ch:
	default:
		t.Fatal("a new mode closes the channel")
	}
	if Current() != Idle {
		t.Fatal("Current follows Set")
	}
}

func TestInvalidModeDoesNotChangeState(t *testing.T) {
	defer Set(Active)
	Set(Background)
	changed := Changed()
	Set(Mode(-1))
	Set(Mode(3))
	if Current() != Background {
		t.Fatal("invalid modes must not replace a valid mode")
	}
	select {
	case <-changed:
		t.Fatal("invalid modes must not notify listeners")
	default:
	}
}
