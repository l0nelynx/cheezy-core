package adapter

import "testing"

func TestParseHysteria2UDPQueueDelay(t *testing.T) {
	for _, test := range []struct {
		value any
		valid bool
	}{
		{"50ms", true}, {0, true}, {"0s", true},
		{"-1ms", false}, {50, false}, {"invalid", false},
	} {
		proxy, err := ParseProxy(map[string]any{
			"name": "realtime", "type": "hysteria2", "server": "127.0.0.1", "port": 443,
			"udp-max-queue-delay": test.value,
		})
		if proxy != nil {
			proxy.Close()
		}
		if (err == nil) != test.valid {
			t.Fatalf("udp-max-queue-delay=%v: error=%v, want valid=%t", test.value, err, test.valid)
		}
	}
}
