package outbound

import (
	"testing"
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
