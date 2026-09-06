package provider

import (
	"testing"

	"github.com/metacubex/mihomo/common/structure"
	"github.com/stretchr/testify/require"
)

func TestXrayMuxOverrideOnlyAppliesToRawTCPVless(t *testing.T) {
	decoder := structure.NewDecoder(structure.Option{TagName: "provider", WeaklyTypedInput: true})
	var override overrideSchema
	require.NoError(t, decoder.Decode(map[string]any{
		"xray-mux": map[string]any{
			"enabled":         true,
			"concurrency":     32,
			"max-connections": 3,
		},
	}, &override))

	tests := []struct {
		name    string
		mapping map[string]any
		wantMux bool
	}{
		{name: "implicit tcp", mapping: map[string]any{"type": "vless"}, wantMux: true},
		{name: "explicit tcp", mapping: map[string]any{"type": "VLESS", "network": "TCP"}, wantMux: true},
		{name: "vision", mapping: map[string]any{"type": "vless", "flow": "xtls-rprx-vision"}},
		{name: "xhttp", mapping: map[string]any{"type": "vless", "network": "xhttp"}},
		{name: "grpc", mapping: map[string]any{"type": "vless", "network": "grpc"}},
		{name: "ws", mapping: map[string]any{"type": "vless", "network": "ws"}},
		{name: "hysteria", mapping: map[string]any{"type": "vless", "network": "hysteria"}},
		{name: "non vless", mapping: map[string]any{"type": "vmess"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.mapping["xray-mux"] = map[string]any{"enabled": true, "concurrency": 99}
			require.NoError(t, override.Apply(test.mapping))
			mux, exists := test.mapping["xray-mux"]
			require.Equal(t, test.wantMux, exists)
			if test.wantMux {
				require.Equal(t, override.XrayMux, mux)
			}
		})
	}
}
