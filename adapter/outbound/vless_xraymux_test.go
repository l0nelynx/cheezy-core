package outbound

import (
	"context"
	"errors"
	"net/netip"
	"testing"

	"github.com/metacubex/mihomo/common/structure"
	C "github.com/metacubex/mihomo/constant"
)

func baseXrayMuxVlessOption() VlessOption {
	return VlessOption{
		Name:   "test",
		Server: "127.0.0.1",
		Port:   443,
		UUID:   "00000000-0000-0000-0000-000000000001",
		XrayMux: XrayMuxOption{
			Enabled:     true,
			Concurrency: 32,
		},
	}
}

func TestVlessXrayMuxFlowPrecedenceIsSilent(t *testing.T) {
	option := baseXrayMuxVlessOption()
	option.Flow = "  xtls-rprx-vision  "
	option.XrayMux.Concurrency = -1 // ignored together with Xray Mux when flow is set
	v, err := NewVless(option)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if v.xrayMux != nil {
		t.Fatal("Xray Mux must be disabled when flow is non-empty")
	}
}

func TestVlessXrayMuxNonRawTCPTransportPrecedenceIsSilent(t *testing.T) {
	for _, network := range []string{"xhttp", "grpc", "ws", "hysteria"} {
		t.Run(network, func(t *testing.T) {
			option := baseXrayMuxVlessOption()
			option.Network = network
			option.XrayMux.Concurrency = -1 // ignored together with Xray Mux
			v, err := NewVless(option)
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			if v.xrayMux != nil {
				t.Fatalf("Xray Mux must be disabled for %s transport", network)
			}
		})
	}
}

func TestVlessXrayMuxRejectsNegativeValues(t *testing.T) {
	option := baseXrayMuxVlessOption()
	option.XrayMux.MaxConnections = -1
	if _, err := NewVless(option); err == nil {
		t.Fatal("expected negative max-connections error")
	}
	option = baseXrayMuxVlessOption()
	option.XrayMux.MaxWorkerUses = -1
	if _, err := NewVless(option); err == nil {
		t.Fatal("expected negative max-worker-uses error")
	}
}

func TestVlessXrayMuxZeroConcurrencyUsesDefault(t *testing.T) {
	option := baseXrayMuxVlessOption()
	option.XrayMux.Concurrency = 0
	v, err := NewVless(option)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if v.xrayMux == nil {
		t.Fatal("Xray Mux pool was not created")
	}
}

func TestVlessXrayMuxXUDPDefaults(t *testing.T) {
	option := baseXrayMuxVlessOption()
	v, err := NewVless(option)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if v.option.XrayMux.XUDPConcurrency != defaultXrayMuxXUDPConcurrency {
		t.Fatalf("xudp-concurrency = %d, want %d", v.option.XrayMux.XUDPConcurrency, defaultXrayMuxXUDPConcurrency)
	}
	if v.option.XrayMux.XUDPProxyUDP443 != xrayMuxUDP443Reject {
		t.Fatalf("xudp-proxy-udp443 = %q, want reject", v.option.XrayMux.XUDPProxyUDP443)
	}
	if v.xrayMux == nil {
		t.Fatal("shared TCP/XUDP pool was not created")
	}
}

func TestVlessXrayMuxXUDPYAMLFields(t *testing.T) {
	decoder := structure.NewDecoder(structure.Option{
		TagName:          "proxy",
		WeaklyTypedInput: true,
		KeyReplacer:      structure.DefaultKeyReplacer,
	})
	var option XrayMuxOption
	if err := decoder.Decode(map[string]any{
		"enabled":           true,
		"xudp-concurrency":  24,
		"xudp-proxy-udp443": "allow",
	}, &option); err != nil {
		t.Fatal(err)
	}
	if option.XUDPConcurrency != 24 || option.XUDPProxyUDP443 != "allow" {
		t.Fatalf("decoded XUDP options = %+v", option)
	}
}

func TestVlessXrayMuxRejectsInvalidXUDPOptions(t *testing.T) {
	option := baseXrayMuxVlessOption()
	option.XrayMux.XUDPConcurrency = -1
	if _, err := NewVless(option); err == nil {
		t.Fatal("expected negative xudp-concurrency error")
	}
	option = baseXrayMuxVlessOption()
	option.XrayMux.XUDPProxyUDP443 = "drop"
	if _, err := NewVless(option); err == nil {
		t.Fatal("expected invalid xudp-proxy-udp443 error")
	}
}

func TestVlessXrayMuxRejectsUDP443BeforeDial(t *testing.T) {
	option := baseXrayMuxVlessOption()
	v, err := NewVless(option)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	packetConn, err := v.ListenPacketContext(context.Background(), &C.Metadata{
		NetWork: C.UDP,
		DstIP:   netip.MustParseAddr("1.1.1.1"),
		DstPort: 443,
	})
	if packetConn != nil || !errors.Is(err, ErrXrayMuxUDP443Rejected) {
		t.Fatalf("UDP/443 result = %v, %v", packetConn, err)
	}
}

func TestVlessXrayMuxUDP443Policies(t *testing.T) {
	v, err := NewVless(baseXrayMuxVlessOption())
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	tests := []struct {
		policy  string
		wantMux bool
		wantErr bool
	}{
		{policy: xrayMuxUDP443Reject, wantErr: true},
		{policy: xrayMuxUDP443Allow, wantMux: true},
		{policy: xrayMuxUDP443Skip},
	}
	for _, test := range tests {
		t.Run(test.policy, func(t *testing.T) {
			v.option.XrayMux.XUDPProxyUDP443 = test.policy
			useMux, err := v.useXrayMuxForUDP(443)
			if useMux != test.wantMux || (err != nil) != test.wantErr {
				t.Fatalf("policy %s = useMux %t, err %v", test.policy, useMux, err)
			}
		})
	}
	v.option.XrayMux.XUDPProxyUDP443 = xrayMuxUDP443Reject
	useMux, err := v.useXrayMuxForUDP(53)
	if err != nil || !useMux {
		t.Fatalf("non-443 UDP = useMux %t, err %v", useMux, err)
	}
}
