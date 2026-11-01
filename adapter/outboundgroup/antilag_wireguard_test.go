package outboundgroup

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/proxydialer"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mipstack"
	wgconn "github.com/metacubex/wireguard-go/conn"
	"github.com/metacubex/wireguard-go/device"
	"github.com/metacubex/wireguard-go/tun"
	"github.com/stretchr/testify/require"
)

type labTun struct {
	*mipstack.Stack
	events chan tun.Event
}

func (d *labTun) File() *os.File           { return nil }
func (d *labTun) Events() <-chan tun.Event { return d.events }
func (d *labTun) Close() error             { close(d.events); return d.Stack.Close() }

// A real WireGuard peer terminates on an in-process IP stack. Its UDP relay
// allocates one external socket per inner source, like endpoint-independent NAT.
func TestAntiLagWireGuardRoaming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	g, _, _ := labGroup(t, netip.AddrPort{})
	serverKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	clientKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	stack, err := mipstack.New(mipstack.Config{LocalAddresses: []netip.Prefix{netip.MustParsePrefix("10.91.0.1/24")}, MTU: 1280})
	require.NoError(t, err)
	require.NoError(t, stack.Start())
	dev := device.NewDevice(&labTun{stack, make(chan tun.Event, 1)}, wgconn.NewStdNetBind(), device.NewLogger(device.LogLevelSilent, ""), 1)
	defer dev.Close()
	require.NoError(t, dev.IpcSet(fmt.Sprintf("private_key=%x\nlisten_port=0\npublic_key=%x\nallowed_ip=10.91.0.2/32\n", serverKey.Bytes(), clientKey.PublicKey().Bytes())))
	require.NoError(t, dev.Up())
	state, err := dev.IpcGet()
	require.NoError(t, err)
	var port int
	for _, line := range strings.Split(state, "\n") {
		if strings.HasPrefix(line, "listen_port=") {
			port, err = strconv.Atoi(strings.TrimPrefix(line, "listen_port="))
			require.NoError(t, err)
		}
	}
	require.NotZero(t, port)
	wg, err := outbound.NewWireGuard(outbound.WireGuardOption{Name: "stable-exit", BasicOption: outbound.BasicOption{DialerForAPI: proxydialer.New(g, false)}, WireGuardPeerOption: outbound.WireGuardPeerOption{Server: "127.0.0.1", Port: port, PublicKey: base64.StdEncoding.EncodeToString(serverKey.PublicKey().Bytes())}, PrivateKey: base64.StdEncoding.EncodeToString(clientKey.Bytes()), Ip: "10.91.0.2/32", Workers: 1, MTU: 1280, UDP: true})
	require.NoError(t, err)
	defer wg.Close()
	udpAddr := netip.MustParseAddrPort("10.91.0.1:9001")
	inside, err := stack.ListenUDP(ctx, "udp4", udpAddr)
	require.NoError(t, err)
	defer inside.Close()
	echo := labServer(t, false)
	go func() {
		sockets := map[string]*net.UDPConn{}
		defer func() {
			for _, c := range sockets {
				c.Close()
			}
		}()
		buf := make([]byte, 4096)
		for {
			n, from, e := inside.ReadFrom(buf)
			if e != nil {
				return
			}
			c := sockets[from.String()]
			if c == nil {
				c, e = net.DialUDP("udp4", nil, net.UDPAddrFromAddrPort(echo))
				if e != nil {
					return
				}
				sockets[from.String()] = c
			}
			c.SetDeadline(time.Now().Add(2 * time.Second))
			if _, e = c.Write(buf[:n]); e != nil {
				return
			}
			n, e = c.Read(buf)
			if e != nil {
				return
			}
			if _, e = inside.WriteTo(buf[:n], from); e != nil {
				return
			}
		}
	}()
	tcpAddr := netip.MustParseAddrPort("10.91.0.1:9002")
	listener, err := stack.ListenTCP(ctx, "tcp4", tcpAddr)
	require.NoError(t, err)
	defer listener.Close()
	go func() {
		c, e := listener.Accept()
		if e == nil {
			defer c.Close()
			io.Copy(c, c)
		}
	}() // accept exactly once
	udp, err := wg.ListenPacketContext(ctx, labMeta(udpAddr))
	require.NoError(t, err)
	defer udp.Close()
	defer func() {
		if t.Failed() {
			info, _ := dev.IpcGet()
			for _, line := range strings.Split(info, "\n") {
				if strings.HasPrefix(line, "endpoint=") || strings.HasPrefix(line, "rx_bytes=") || strings.HasPrefix(line, "tx_bytes=") || strings.HasPrefix(line, "last_handshake_time_sec=") {
					t.Log(line)
				}
			}
			t.Logf("carrier bindings: %+v", g.Bindings())
		}
	}()
	external := exchange(t, udp, udpAddr, "game")
	m := labMeta(tcpAddr)
	m.NetWork = C.TCP
	stream, err := wg.DialContext(ctx, m)
	require.NoError(t, err)
	defer stream.Close()
	stream.SetDeadline(time.Now().Add(15 * time.Second))
	for i := 0; i < 64; i++ {
		if i == 32 {
			bindings := g.Bindings()
			require.Len(t, bindings, 1)
			require.NoError(t, g.Failover(ctx, bindings[0].ID, "B"))
			require.Equal(t, external, exchange(t, udp, udpAddr, "game"))
		}
		payload := bytes.Repeat([]byte{byte(i)}, 16384)
		_, err = stream.Write(payload)
		require.NoError(t, err)
		got := make([]byte, len(payload))
		_, err = io.ReadFull(stream, got)
		require.NoError(t, err)
		require.Equal(t, payload, got)
	}
	require.Equal(t, "B", g.Bindings()[0].Active)
}
