package outbound

import (
	"net"
	"testing"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

func TestHysteria2UDPQueueDelayValidation(t *testing.T) {
	for _, test := range []struct {
		value   string
		want    time.Duration
		invalid bool
	}{
		{value: ""}, {value: "0"}, {value: "0s"},
		{value: "50ms", want: 50 * time.Millisecond},
		{value: "1.5s", want: 1500 * time.Millisecond},
		{value: "-1ms", invalid: true}, {value: "50", invalid: true},
		{value: "bad", invalid: true}, {value: "999999999999999999999h", invalid: true},
	} {
		t.Run(test.value, func(t *testing.T) {
			out, err := NewHysteria2(Hysteria2Option{Server: "127.0.0.1", Port: 443, UDPMaxQueueDelay: test.value})
			if test.invalid {
				if err == nil {
					out.Close()
					t.Fatal("invalid delay accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer out.Close()
			if out.UDPMaxQueueDelay() != test.want {
				t.Fatalf("delay = %s, want %s", out.UDPMaxQueueDelay(), test.want)
			}
		})
	}
}

func TestHysteria2UDPQueuePolicyThroughWrappers(t *testing.T) {
	out, err := NewHysteria2(Hysteria2Option{Name: "realtime", Server: "127.0.0.1", Port: 443, UDPMaxQueueDelay: "50ms"})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	raw, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pc := NewPacketConn(raw, out)
	pc.(AddRef).AddRef(out)
	tracked := statistic.NewUDPTracker(pc, statistic.DefaultManager, &C.Metadata{NetWork: C.UDP}, nil, 0, 0, false)
	defer tracked.Close()
	policy, ok := N.FindUpstream[C.UDPQueuePolicy](tracked, nil)
	if !ok || policy.UDPMaxQueueDelay() != 50*time.Millisecond {
		t.Fatal("queue policy lost through tracker / reference wrappers")
	}
}
