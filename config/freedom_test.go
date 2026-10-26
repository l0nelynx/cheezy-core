package config

import (
	"os"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// Keep the documented host-scoped YAML executable, including the important
// distinction between TCP HTTPS, QUIC and unrelated hosts.
func TestFreedomHostExample(t *testing.T) {
	data, err := os.ReadFile("../docs/examples/freedom-host.yaml")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := UnmarshalRawConfig(data)
	if err != nil {
		t.Fatal(err)
	}
	proxies := map[string]C.Proxy{"DIRECT": adapter.NewProxy(outbound.NewDirect())}
	for _, mapping := range raw.Proxy {
		proxy, err := adapter.ParseProxy(mapping)
		if err != nil {
			t.Fatal(err)
		}
		defer proxy.Close()
		proxies[proxy.Name()] = proxy
	}
	rules, err := parseRules(raw.Rule, proxies, nil, nil, "rules")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, host string
		network    C.NetWork
		port       uint16
		want       string
	}{
		{"target HTTPS", "example.com", C.TCP, 443, "FRAGMENT"},
		{"other host", "other.example", C.TCP, 443, "DIRECT"},
		{"subdomain", "www.example.com", C.TCP, 443, "DIRECT"},
		{"plain HTTP", "example.com", C.TCP, 80, "DIRECT"},
		{"QUIC", "example.com", C.UDP, 443, "DIRECT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &C.Metadata{Host: tc.host, NetWork: tc.network, DstPort: tc.port}
			for _, rule := range rules {
				if matches, target := rule.Match(metadata, C.RuleMatchHelper{}); matches {
					if target != tc.want {
						t.Fatalf("route = %s, want %s", target, tc.want)
					}
					return
				}
			}
			t.Fatal("no matching rule")
		})
	}
}
