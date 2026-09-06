package outbound

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// TestVlessXrayMuxInterop runs only when XRAY_BIN points to a pinned Xray-core
// binary. It validates the real VLESS RequestCommandMux and Mux.Cool framing.
func TestVlessXrayMuxInterop(t *testing.T) {
	xrayBin := os.Getenv("XRAY_BIN")
	if xrayBin == "" {
		t.Skip("XRAY_BIN is not set")
	}

	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoListener.Close()
	go func() {
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	xrayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	xrayPort := xrayListener.Addr().(*net.TCPAddr).Port
	xrayListener.Close()

	const uuid = "00000000-0000-0000-0000-000000000001"
	config := map[string]any{
		"log": map[string]any{"loglevel": "debug"},
		"inbounds": []any{map[string]any{
			"listen":   "127.0.0.1",
			"port":     xrayPort,
			"protocol": "vless",
			"settings": map[string]any{
				"clients":    []any{map[string]any{"id": uuid}},
				"decryption": "none",
			},
			"streamSettings": map[string]any{"network": "tcp"},
		}},
		"outbounds": []any{map[string]any{
			"protocol": "freedom",
			"settings": map[string]any{
				"finalRules": []any{map[string]any{"action": "allow", "network": "tcp,udp"}},
			},
		}},
	}
	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "xray.json")
	if err := os.WriteFile(configPath, configBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(xrayBin, "run", "-config", configPath)
	var xrayLogs bytes.Buffer
	cmd.Stdout = &xrayLogs
	cmd.Stderr = &xrayLogs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		probe, probeErr := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(xrayPort)), 100*time.Millisecond)
		if probeErr == nil {
			probe.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Xray-core did not start")
		}
		time.Sleep(25 * time.Millisecond)
	}

	v, err := NewVless(VlessOption{
		Name: "xray-interop", Server: "127.0.0.1", Port: xrayPort, UUID: uuid,
		XrayMux: XrayMuxOption{Enabled: true, Concurrency: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	echoPort := uint16(echoListener.Addr().(*net.TCPAddr).Port)
	conn, err := v.DialContext(context.Background(), &C.Metadata{
		NetWork: C.TCP,
		DstIP:   netip.MustParseAddr("127.0.0.1"),
		DstPort: echoPort,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("mux-cool")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("mux-cool"))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v\nXray logs:\n%s", err, xrayLogs.String())
	}
	if string(got) != "mux-cool" {
		t.Fatalf("echo = %q", got)
	}
}
