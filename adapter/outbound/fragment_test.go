package outbound

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

type fragmentTestConn struct {
	net.Conn
	writes int
}

func (c *fragmentTestConn) Write(b []byte) (int, error) {
	c.writes++
	return len(b), nil
}

type fragmentTestPacketConn struct{ net.PacketConn }

type fragmentTestDialer struct {
	conn             *fragmentTestConn
	packet           net.PacketConn
	err              error
	network, address string
}

func (d *fragmentTestDialer) DialContext(_ context.Context, network, address string) (net.Conn, error) {
	d.network, d.address = network, address
	return d.conn, d.err
}

func (d *fragmentTestDialer) ListenPacket(_ context.Context, network, address string, _ netip.AddrPort) (net.PacketConn, error) {
	d.network, d.address = network, address
	return d.packet, d.err
}

func TestFragmentDialerPreservesTransport(t *testing.T) {
	ctx := context.Background()
	for _, network := range []string{"tcp", "tcp4", "tcp6", "udp"} {
		t.Run(network, func(t *testing.T) {
			raw := &fragmentTestDialer{conn: &fragmentTestConn{}}
			d, err := newFragmentDialer(raw, &FragmentOption{Packets: "1", Length: "2"}, "test")
			if err != nil {
				t.Fatal(err)
			}
			// The shared config must create independent state for each connection.
			for i := 0; i < 2; i++ {
				raw.conn = &fragmentTestConn{}
				conn, err := d.DialContext(ctx, network, "example.com:443")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := conn.Write([]byte("abcdef")); err != nil {
					t.Fatal(err)
				}
				want := 3
				if network == "udp" {
					want = 1
					if conn != raw.conn {
						t.Fatal("UDP connection was wrapped")
					}
				}
				if raw.conn.writes != want || raw.network != network || raw.address != "example.com:443" {
					t.Fatalf("writes=%d, dial=%s %s", raw.conn.writes, raw.network, raw.address)
				}
			}
		})
	}
	packet := &fragmentTestPacketConn{}
	raw := &fragmentTestDialer{packet: packet}
	d, err := newFragmentDialer(raw, &FragmentOption{Packets: "tlshello", Split: "sni"}, "test")
	if err != nil {
		t.Fatal(err)
	}
	pc, err := d.ListenPacket(ctx, "udp4", "127.0.0.1:0", netip.AddrPort{})
	if err != nil || pc != packet || raw.network != "udp4" || raw.address != "127.0.0.1:0" {
		t.Fatal("packet dialer was not preserved", err)
	}
	raw.err = errors.New("dial failed")
	if _, err := d.DialContext(ctx, "tcp", "example.com:443"); !errors.Is(err, raw.err) {
		t.Fatal("dial error was not preserved", err)
	}
	unchanged, err := newFragmentDialer(raw, nil, "test")
	if err != nil || unchanged != raw {
		t.Fatal("absent fragment changed the dialer", err)
	}
}

// Observe real ClientHello records below TLS/REALITY, including pooled
// transports. SNI www.example.com produces two cuts and three TLS records.
func TestInlineProxyFragmentClientHello(t *testing.T) {
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	reality := RealityOptions{PublicKey: base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes())}
	for _, mode := range []string{
		"vless", "vless-reality", "vless-smux", "vless-xraymux",
		"vless-ws", "vless-h2", "vless-grpc", "vless-xhttp", "trojan", "vmess",
	} {
		t.Run(mode, func(t *testing.T) {
			addr, records := recordServer(t)
			fragment := &FragmentOption{Packets: "tlshello", Split: "sni", Length: "100-200", Interval: "0"}
			var proxy ProxyAdapter
			var err error
			switch mode {
			case "trojan":
				proxy, err = NewTrojan(TrojanOption{
					Name: "test", Server: "127.0.0.1", Port: addr.Port, Password: "test",
					SNI: "www.example.com", ClientFingerprint: "firefox", Fragment: fragment,
				})
			case "vmess":
				proxy, err = NewVmess(VmessOption{
					Name: "test", Server: "127.0.0.1", Port: addr.Port,
					UUID: "00000000-0000-0000-0000-000000000001", Cipher: "auto",
					TLS: true, ServerName: "www.example.com", ClientFingerprint: "firefox", Fragment: fragment,
				})
			default:
				option := VlessOption{
					Name: "test", Server: "127.0.0.1", Port: addr.Port,
					UUID: "00000000-0000-0000-0000-000000000001",
					TLS:  true, ServerName: "www.example.com", ClientFingerprint: "firefox", Fragment: fragment,
				}
				switch mode {
				case "vless-reality", "vless-smux", "vless-xraymux":
					option.RealityOpts = reality
				}
				switch mode {
				case "vless-ws":
					option.Network = "ws"
				case "vless-h2":
					option.Network = "h2"
				case "vless-grpc":
					option.Network = "grpc"
				case "vless-xhttp":
					option.Network = "xhttp"
				case "vless-xraymux":
					option.XrayMux.Enabled = true
				}
				proxy, err = NewVless(option)
				if err == nil && mode == "vless-smux" {
					inner := proxy
					proxy, err = NewSingMux(SingMuxOption{Enabled: true, Protocol: "h2mux", Padding: true, MaxConnections: 3}, inner)
					if err != nil {
						_ = inner.Close()
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			defer proxy.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				conn, err := proxy.DialContext(ctx, &C.Metadata{NetWork: C.TCP, Host: "example.com", DstPort: 443})
				if conn != nil {
					_ = conn.Close()
				}
				done <- err
			}()
			select {
			case n := <-records:
				if n != 3 {
					t.Fatalf("ClientHello arrived in %d TLS records, want 3", n)
				}
			case <-ctx.Done():
				t.Fatal("timed out waiting for fragmented ClientHello")
			}
			cancel()
			_ = proxy.Close() // smux can retry when the capture server closes before TLS completes.
			select {
			case <-done: // The capture server closes after ClientHello, before completing TLS.
			case <-time.After(time.Second):
				t.Fatal("dial did not stop after capture")
			}
		})
	}
}
