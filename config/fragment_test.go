package config

import (
	"os"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func TestInlineFragmentSubscription(t *testing.T) {
	data, err := os.ReadFile("../docs/examples/vless-inline-fragment.yaml")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := UnmarshalRawConfig(data)
	if err != nil {
		t.Fatal(err)
	}
	// Add the redacted user's REALITY options. This public key is a parse-only
	// fixture, not a usable server key. Unknown legacy fields remain tolerated.
	raw.Proxy[0]["reality-opts"] = map[string]any{
		"public-key":             "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"short-id":               "0123456789abcdef",
		"support-x25519mlkem768": true,
	}
	proxies, _, err := parseProxies(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, proxy := range proxies {
		defer proxy.Close()
	}
	if len(raw.Proxy) != 1 || proxies["VLESS-FRAGMENT"].ProxyInfo().DialerProxy != "" {
		t.Fatal("shared subscription needs another outbound")
	}
	rules, err := parseRules(raw.Rule, proxies, nil, nil, "rules")
	if err != nil {
		t.Fatal(err)
	}
	if ok, target := rules[0].Match(&C.Metadata{NetWork: C.TCP, Host: "example.com", DstPort: 443}, C.RuleMatchHelper{}); !ok || target != "VLESS-FRAGMENT" {
		t.Fatal("the node was not selected")
	}
}
