package outbound

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

func startConnectGateway(t *testing.T) (host string, port int, closeGateway func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					line, readErr := reader.ReadString('\n')
					if readErr != nil {
						return
					}
					if line == "\r\n" {
						break
					}
				}
				_, _ = conn.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))
				buffer := make([]byte, 1)
				_, _ = conn.Read(buffer)
			}()
		}
	}()
	host, rawPort, _ := net.SplitHostPort(listener.Addr().String())
	port, _ = strconv.Atoi(rawPort)
	return host, port, func() { _ = listener.Close() }
}

func TestHttpConnectionLimitAndAllowedPorts(t *testing.T) {
	host, port, closeGateway := startConnectGateway(t)
	defer closeGateway()
	httpProxy, err := NewHttp(HttpOption{
		Name:           "limited-wap",
		Server:         host,
		Port:           port,
		MaxConnections: 1,
		AllowedPorts:   []int{443},
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := &C.Metadata{Host: "example.com", DstPort: 443}
	first, err := httpProxy.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err = httpProxy.DialContext(ctx, metadata); err == nil || !strings.Contains(err.Error(), "waiting for HTTP CONNECT slot") {
		t.Fatalf("expected slot wait failure, got %v", err)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := httpProxy.DialContext(context.Background(), metadata)
	if err != nil {
		t.Fatalf("slot was not released: %v", err)
	}
	_ = second.Close()

	blocked := &C.Metadata{Host: "example.com", DstPort: 80}
	if _, err = httpProxy.DialContext(context.Background(), blocked); err == nil || !strings.Contains(err.Error(), "is not allowed") {
		t.Fatalf("expected blocked port, got %v", err)
	}
	active, waiting, limit, rejected := httpProxy.ConnectionLimitStats()
	if active != 0 || waiting != 0 || limit != 1 || rejected != 1 {
		t.Fatalf("unexpected limiter stats: active=%d waiting=%d limit=%d rejected=%d", active, waiting, limit, rejected)
	}
}
