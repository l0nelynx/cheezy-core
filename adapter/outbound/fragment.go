package outbound

import (
	"context"
	"fmt"
	"net"
	"strings"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/transport/fragment"
)

// FragmentOption configures the TCP connection below the proxy's TLS/REALITY
// handshake. It is also used by Freedom for direct connections.
type FragmentOption struct {
	Packets       string `proxy:"packets,omitempty"`
	Length        string `proxy:"length,omitempty"`
	Interval      string `proxy:"interval,omitempty"`
	MaxSplit      string `proxy:"max-split,omitempty"`
	Split         string `proxy:"split,omitempty"`
	Wait          string `proxy:"wait,omitempty"`
	FallbackDelay int    `proxy:"fallback-delay,omitempty"`
}

func (o *FragmentOption) parse(name string) (*fragment.Config, error) {
	if o == nil {
		return nil, nil
	}
	cfg, err := fragment.NewConfig(fragment.Option{
		Packets:       o.Packets,
		Length:        o.Length,
		Interval:      o.Interval,
		MaxSplit:      o.MaxSplit,
		Split:         o.Split,
		Wait:          o.Wait,
		FallbackDelay: o.FallbackDelay,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return cfg, nil
}

// Wrap the selected dialer rather than a logical proxy stream: pooled gRPC,
// XHTTP, sing-mux and Xray mux connections must fragment their own handshake
// exactly once, before encryption. UDP/QUIC sockets pass through unchanged.
func newFragmentDialer(d C.Dialer, option *FragmentOption, name string) (C.Dialer, error) {
	cfg, err := option.parse(name)
	if err != nil || cfg == nil {
		return d, err
	}
	return &fragmentDialer{Dialer: d, config: cfg}, nil
}

type fragmentDialer struct {
	C.Dialer
	config *fragment.Config
}

func (d *fragmentDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := d.Dialer.DialContext(ctx, network, address)
	if err != nil || !strings.HasPrefix(network, "tcp") {
		return conn, err
	}
	return fragment.NewConn(conn, d.config), nil
}
