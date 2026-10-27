package adapter

import "testing"

func TestParseInlineProxyFragment(t *testing.T) {
	for _, protocol := range []string{"vless", "trojan", "vmess"} {
		t.Run(protocol, func(t *testing.T) {
			mapping := map[string]any{
				"name": "test", "type": protocol, "server": "127.0.0.1", "port": 443,
				"uuid": "00000000-0000-0000-0000-000000000001", "alterId": 0, "cipher": "auto", "password": "test",
				"tls": true, "client-fingerprint": "firefox",
				"fragment": map[string]any{"split": "sni", "length": "100-200", "packets": "tlshello", "interval": "1-3"},
				"smux":     map[string]any{"enabled": true, "padding": true, "protocol": "h2mux", "max-connections": 3},
			}
			proxy, err := ParseProxy(mapping)
			if err != nil {
				t.Fatal(err)
			}
			defer proxy.Close()
			if proxy.ProxyInfo().DialerProxy != "" || !proxy.ProxyInfo().SMUX {
				t.Fatal("inline fragment introduced a proxy dependency or lost smux")
			}
			for _, fragment := range []map[string]any{
				{"split": "invalid"}, {"packets": "tlshello", "length": "0"},
				{"split": "sni", "interval": "invalid"},
			} {
				mapping["fragment"] = fragment
				if _, err := ParseProxy(mapping); err == nil {
					t.Fatal("invalid inline fragment was ignored", fragment)
				}
			}
		})
	}
}
