package outbound

import (
	"encoding/base64"
	"testing"

	utls "github.com/metacubex/utls"
)

func TestRealityOptionsMldsa65(t *testing.T) {
	_, verify, err := utls.RealityMldsa65KeyFromSeed(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	publicKey := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	for _, tc := range []struct {
		name, key, verify string
		valid             bool
	}{
		{"disabled", publicKey, "", true},
		{"valid", publicKey, base64.RawURLEncoding.EncodeToString(verify), true},
		{"bad encoding", publicKey, "!", false},
		{"short key", publicKey, base64.RawURLEncoding.EncodeToString(verify[:1951]), false},
		{"long key", publicKey, base64.RawURLEncoding.EncodeToString(append(verify, 0)), false},
		{"missing reality key", "", base64.RawURLEncoding.EncodeToString(verify), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := (RealityOptions{PublicKey: tc.key, Mldsa65Verify: tc.verify}).Parse()
			if (err == nil) != tc.valid {
				t.Fatalf("Parse error = %v, valid = %v", err, tc.valid)
			}
			if tc.valid && tc.verify != "" && len(config.Mldsa65Verify) != 1952 {
				t.Fatal("verification key was not passed through")
			}
		})
	}
}
