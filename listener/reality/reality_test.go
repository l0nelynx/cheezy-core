package reality

import (
	"bytes"
	"encoding/base64"
	"testing"
	"time"

	utls "github.com/metacubex/utls"
)

func TestRealityMldsa65Seed(t *testing.T) {
	privateKey := bytes.Repeat([]byte{1}, 32)
	seed := bytes.Repeat([]byte{2}, 32)
	for _, tc := range []struct {
		name, seed string
		valid      bool
	}{
		{"disabled", "", true},
		{"valid", base64.RawURLEncoding.EncodeToString(seed), true},
		{"bad encoding", "!", false},
		{"short seed", base64.RawURLEncoding.EncodeToString(seed[:31]), false},
		{"long seed", base64.RawURLEncoding.EncodeToString(append(seed, 0)), false},
		{"reused private key", base64.RawURLEncoding.EncodeToString(privateKey), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, err := (Config{Dest: "example.com:443", PrivateKey: base64.RawURLEncoding.EncodeToString(privateKey), ShortID: []string{""}, ServerNames: []string{"example.com"}, Mldsa65Seed: tc.seed, MaxTimeDifference: 1500}).Build(nil)
			if (err == nil) != tc.valid {
				t.Fatalf("Build error = %v, valid = %v", err, tc.valid)
			}
			if !tc.valid {
				return
			}
			if builder.realityConfig.MaxTimeDiff != 1500*time.Millisecond {
				t.Fatal("max-time-difference must use milliseconds")
			}
			if tc.seed == "" {
				if len(builder.realityConfig.Mldsa65Key) != 0 {
					t.Fatal("unexpected signing key")
				}
			} else {
				want, _, err := utls.RealityMldsa65KeyFromSeed(seed)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(builder.realityConfig.Mldsa65Key, want) {
					t.Fatal("incorrect derived signing key")
				}
			}
		})
	}
}
