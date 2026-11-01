//go:build !with_powersave

package powersave

import "testing"

func TestDisabledPowerSaveStaysActive(t *testing.T) {
	changed := Changed()
	Set(Background)
	Set(Idle)
	if Current() != Active {
		t.Fatal("a build without the tag must stay active")
	}
	select {
	case <-changed:
		t.Fatal("disabled power save must not notify listeners")
	default:
	}
}
