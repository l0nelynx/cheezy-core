//go:build !with_powersave

package log

import "testing"

func TestGatingDisabledWithoutBuildTag(t *testing.T) {
	defer SetLevel(Level())
	defer SetGated(false)
	SetLevel(SILENT)
	SetGated(true)
	if !enabled(DEBUG) {
		t.Fatal("a build without with_powersave must preserve subscriber events")
	}
}
