package outbound

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/mihomo/component/connectionlimit"
	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/http"
	"github.com/metacubex/tls"
)

type Http struct {
	*Base
	user      string
	pass      string
	tlsConfig *tls.Config
	option    *HttpOption
	limiter   *connectionlimit.Limiter
	ports     map[uint16]struct{}
	rejected  atomic.Int64
}

type HttpOption struct {
	BasicOption
	Name           string            `proxy:"name"`
	Server         string            `proxy:"server"`
	Port           int               `proxy:"port"`
	UserName       string            `proxy:"username,omitempty"`
	Password       string            `proxy:"password,omitempty"`
	TLS            bool              `proxy:"tls,omitempty"`
	SNI            string            `proxy:"sni,omitempty"`
	SkipCertVerify bool              `proxy:"skip-cert-verify,omitempty"`
	NameCertVerify string            `proxy:"name-cert-verify,omitempty"`
	Fingerprint    string            `proxy:"fingerprint,omitempty"`
	Certificate    string            `proxy:"certificate,omitempty"`
	PrivateKey     string            `proxy:"private-key,omitempty"`
	Headers        map[string]string `proxy:"headers,omitempty"`
	MaxConnections int               `proxy:"max-connections,omitempty"`
	AllowedPorts   []int             `proxy:"allowed-connect-ports,omitempty"`
}

var httpConnectionLimiters = connectionlimit.NewRegistry()

type limitedConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// StreamConnContext implements C.ProxyAdapter
func (h *Http) StreamConnContext(ctx context.Context, c net.Conn, metadata *C.Metadata) (net.Conn, error) {
	if h.tlsConfig != nil {
		cc := tls.Client(c, h.tlsConfig)
		err := cc.HandshakeContext(ctx)
		c = cc
		if err != nil {
			return nil, fmt.Errorf("%s connect error: %w", h.addr, err)
		}
	}

	if err := h.shakeHandContext(ctx, c, metadata); err != nil {
		return nil, err
	}
	return c, nil
}

// DialContext implements C.ProxyAdapter
func (h *Http) DialContext(ctx context.Context, metadata *C.Metadata) (_ C.Conn, err error) {
	if len(h.ports) != 0 {
		if _, allowed := h.ports[metadata.DstPort]; !allowed {
			h.rejected.Add(1)
			return nil, fmt.Errorf("HTTP CONNECT destination port %d is not allowed", metadata.DstPort)
		}
	}

	release := func() {}
	if h.limiter != nil {
		release, err = h.limiter.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("waiting for HTTP CONNECT slot: %w", err)
		}
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			release()
		}
	}()

	c, err := h.dialer.DialContext(ctx, "tcp", h.addr)
	if err != nil {
		return nil, fmt.Errorf("%s connect error: %w", h.addr, err)
	}

	defer func(c net.Conn) {
		safeConnClose(c, err)
	}(c)

	c, err = h.StreamConnContext(ctx, c, metadata)
	if err != nil {
		return nil, err
	}

	releaseOnError = false
	return NewConn(&limitedConn{Conn: c, release: release}, h), nil
}

// ConnectionLimitStats returns aggregate, non-sensitive limiter diagnostics.
func (h *Http) ConnectionLimitStats() (active, waiting int64, limit int, rejectedPorts int64) {
	if h.limiter != nil {
		snapshot := h.limiter.Snapshot()
		active, waiting, limit = snapshot.Active, snapshot.Waiting, snapshot.Limit
	}
	return active, waiting, limit, h.rejected.Load()
}

// ProxyInfo implements C.ProxyAdapter
func (h *Http) ProxyInfo() C.ProxyInfo {
	info := h.Base.ProxyInfo()
	info.DialerProxy = h.option.DialerProxy
	return info
}

func (h *Http) shakeHandContext(ctx context.Context, c net.Conn, metadata *C.Metadata) (err error) {
	if ctx.Done() != nil {
		done := N.SetupContextForConn(ctx, c)
		defer done(&err)
	}

	addr := metadata.RemoteAddress()
	HeaderString := "CONNECT " + addr + " HTTP/1.1\r\n"
	tempHeaders := map[string]string{
		"Host":             addr,
		"User-Agent":       "Go-http-client/1.1",
		"Proxy-Connection": "Keep-Alive",
	}

	for key, value := range h.option.Headers {
		tempHeaders[key] = value
	}

	if h.user != "" && h.pass != "" {
		auth := h.user + ":" + h.pass
		tempHeaders["Proxy-Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
	}

	for key, value := range tempHeaders {
		HeaderString += key + ": " + value + "\r\n"
	}

	HeaderString += "\r\n"

	_, err = c.Write([]byte(HeaderString))

	if err != nil {
		return err
	}

	resp, err := http.ReadResponse(bufio.NewReader(c), nil)

	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	if resp.StatusCode == http.StatusProxyAuthRequired {
		return errors.New("HTTP need auth")
	}

	if resp.StatusCode == http.StatusMethodNotAllowed {
		return errors.New("CONNECT method not allowed by proxy")
	}

	if resp.StatusCode >= http.StatusInternalServerError {
		return errors.New(resp.Status)
	}

	return fmt.Errorf("can not connect remote err code: %d", resp.StatusCode)
}

func NewHttp(option HttpOption) (*Http, error) {
	var tlsConfig *tls.Config
	if option.TLS {
		sni := option.Server
		if option.SNI != "" {
			sni = option.SNI
		}
		var err error
		tlsConfig, err = ca.GetTLSConfig(ca.Option{
			TLSConfig: &tls.Config{
				InsecureSkipVerify: option.SkipCertVerify,
				ServerName:         sni,
			},
			Fingerprint:    option.Fingerprint,
			NameCertVerify: option.NameCertVerify,
			Certificate:    option.Certificate,
			PrivateKey:     option.PrivateKey,
		})
		if err != nil {
			return nil, err
		}
	}

	ports := make(map[uint16]struct{}, len(option.AllowedPorts))
	for _, port := range option.AllowedPorts {
		if port < 1 || port > 65535 {
			return nil, fmt.Errorf("invalid allowed CONNECT port: %d", port)
		}
		ports[uint16(port)] = struct{}{}
	}
	limiter, err := httpConnectionLimiters.Get(net.JoinHostPort(option.Server, strconv.Itoa(option.Port))+"\x00"+option.UserName, option.MaxConnections)
	if err != nil {
		return nil, err
	}

	outbound := &Http{
		Base: NewBase(BaseOption{
			Name:         option.Name,
			Addr:         net.JoinHostPort(option.Server, strconv.Itoa(option.Port)),
			Type:         C.Http,
			ProviderName: option.ProviderName,
			TFO:          option.TFO,
			MPTCP:        option.MPTCP,
			Interface:    option.Interface,
			RoutingMark:  option.RoutingMark,
			Prefer:       option.IPVersion,
		}),
		user:      option.UserName,
		pass:      option.Password,
		tlsConfig: tlsConfig,
		option:    &option,
		limiter:   limiter,
		ports:     ports,
	}
	outbound.dialer = option.NewDialer(outbound.DialOptions())
	return outbound, nil
}
