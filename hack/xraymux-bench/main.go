// Command xraymux-bench runs a local VLESS+Mux.Cool matrix against Xray-core.
//
// Usage:
//
//	XRAY_BIN=/tmp/xray-bin/xray go run ./hack/xraymux-bench \
//	  -out /opt/cursor/artifacts/xraymux-bench/results.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

type configCase struct {
	Name           string `json:"name"`
	Concurrency    int    `json:"concurrency"`
	MaxConnections int    `json:"max_connections"`
	MuxEnabled     bool   `json:"mux_enabled"`
	Streams        int    `json:"streams"`
}

type caseResult struct {
	Config        configCase `json:"config"`
	OK            bool       `json:"ok"`
	Error         string     `json:"error,omitempty"`
	Seconds       float64    `json:"seconds"`
	BytesTotal    int64      `json:"bytes_total"`
	Mbps          float64    `json:"mbps"`
	PhysicalDials int64      `json:"physical_dials"`
	PeakClientRSS uint64     `json:"peak_client_rss_bytes"`
	PeakXrayRSS   uint64     `json:"peak_xray_rss_bytes"`
	ClientCPUSec  float64    `json:"client_cpu_sec"`
	PerStreamMbps []float64  `json:"per_stream_mbps,omitempty"`
	RTTMs         int        `json:"rtt_ms"`
}

type summary struct {
	StartedAt  string       `json:"started_at"`
	XrayBin    string       `json:"xray_bin"`
	PayloadMB  int          `json:"payload_mb_per_stream"`
	RTTMs      int          `json:"rtt_ms"`
	GOMAXPROCS int          `json:"gomaxprocs"`
	Results    []caseResult `json:"results"`
}

func main() {
	xrayBin := flag.String("xray", envOr("XRAY_BIN", ""), "path to xray binary")
	outPath := flag.String("out", "/opt/cursor/artifacts/xraymux-bench/results.json", "JSON results path")
	payloadMB := flag.Int("payload-mb", 16, "payload size per stream in MiB")
	rttMs := flag.Int("rtt-ms", 80, "simulated RTT via delay proxy (0 disables)")
	flag.Parse()

	if *xrayBin == "" {
		fmt.Fprintln(os.Stderr, "XRAY_BIN / -xray required")
		os.Exit(2)
	}
	if _, err := os.Stat(*xrayBin); err != nil {
		fmt.Fprintf(os.Stderr, "xray binary: %v\n", err)
		os.Exit(2)
	}

	cases := []configCase{
		{Name: "mux_off_s4", MuxEnabled: false, Streams: 4},
		{Name: "c1_m0_s4", MuxEnabled: true, Concurrency: 1, MaxConnections: 0, Streams: 4},
		{Name: "c2_m0_s4", MuxEnabled: true, Concurrency: 2, MaxConnections: 0, Streams: 4},
		{Name: "c4_m0_s4", MuxEnabled: true, Concurrency: 4, MaxConnections: 0, Streams: 4},
		{Name: "c8_m0_s4", MuxEnabled: true, Concurrency: 8, MaxConnections: 0, Streams: 4},
		{Name: "c16_m0_s4", MuxEnabled: true, Concurrency: 16, MaxConnections: 0, Streams: 4},
		{Name: "c8_m1_s4", MuxEnabled: true, Concurrency: 8, MaxConnections: 1, Streams: 4},
		{Name: "c8_m2_s4", MuxEnabled: true, Concurrency: 8, MaxConnections: 2, Streams: 4},
		{Name: "c8_m3_s4", MuxEnabled: true, Concurrency: 8, MaxConnections: 3, Streams: 4},
		{Name: "c2_m0_s8", MuxEnabled: true, Concurrency: 2, MaxConnections: 0, Streams: 8},
		{Name: "c8_m0_s8", MuxEnabled: true, Concurrency: 8, MaxConnections: 0, Streams: 8},
		{Name: "c16_m1_s8", MuxEnabled: true, Concurrency: 16, MaxConnections: 1, Streams: 8},
	}

	sum := summary{
		StartedAt:  time.Now().UTC().Format(time.RFC3339),
		XrayBin:    *xrayBin,
		PayloadMB:  *payloadMB,
		RTTMs:      *rttMs,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
	}

	fmt.Printf("payload=%dMiB/stream rtt=%dms cases=%d\n", *payloadMB, *rttMs, len(cases))
	for _, c := range cases {
		res := runCase(*xrayBin, c, *payloadMB, *rttMs)
		sum.Results = append(sum.Results, res)
		status := "OK"
		if !res.OK {
			status = "FAIL:" + res.Error
		}
		fmt.Printf("%-14s streams=%d dials=%d  %6.1f Mbps  rss_cli=%s rss_xray=%s  %s\n",
			c.Name, c.Streams, res.PhysicalDials, res.Mbps,
			humanBytes(res.PeakClientRSS), humanBytes(res.PeakXrayRSS), status)
	}

	raw, _ := json.MarshalIndent(sum, "", "  ")
	_ = os.MkdirAll(filepath.Dir(*outPath), 0o755)
	if err := os.WriteFile(*outPath, raw, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write results: %v\n", err)
		os.Exit(1)
	}
	md := strings.TrimSuffix(*outPath, filepath.Ext(*outPath)) + ".md"
	_ = os.WriteFile(md, []byte(renderMarkdown(sum)), 0o644)
	fmt.Printf("\nwrote %s\nwrote %s\n", *outPath, md)
}

func runCase(xrayBin string, cfg configCase, payloadMB, rttMs int) (res caseResult) {
	res.Config = cfg
	res.RTTMs = rttMs
	runtime.GC()

	originLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		res.Error = err.Error()
		return res
	}
	defer originLn.Close()
	payloadBytes := int64(payloadMB) << 20
	originSrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.FormatInt(payloadBytes, 10))
		w.WriteHeader(http.StatusOK)
		var buf [32 * 1024]byte
		for i := range buf {
			buf[i] = byte(i)
		}
		left := payloadBytes
		for left > 0 {
			n := len(buf)
			if int64(n) > left {
				n = int(left)
			}
			if _, err := w.Write(buf[:n]); err != nil {
				return
			}
			left -= int64(n)
		}
	})}
	go func() { _ = originSrv.Serve(originLn) }()
	defer originSrv.Close()
	originPort := originLn.Addr().(*net.TCPAddr).Port

	xrayLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		res.Error = err.Error()
		return res
	}
	xrayPort := xrayLn.Addr().(*net.TCPAddr).Port
	_ = xrayLn.Close()

	const uuid = "00000000-0000-0000-0000-000000000001"
	cfgJSON, _ := json.Marshal(map[string]any{
		"log": map[string]any{"loglevel": "warning"},
		"inbounds": []any{map[string]any{
			"listen": "127.0.0.1", "port": xrayPort, "protocol": "vless",
			"settings": map[string]any{
				"clients": []any{map[string]any{"id": uuid}}, "decryption": "none",
			},
			"streamSettings": map[string]any{"network": "tcp"},
		}},
		"outbounds": []any{map[string]any{"protocol": "freedom"}},
	})
	dir, err := os.MkdirTemp("", "xraymux-bench-*")
	if err != nil {
		res.Error = err.Error()
		return res
	}
	defer os.RemoveAll(dir)
	cfgPath := filepath.Join(dir, "xray.json")
	if err := os.WriteFile(cfgPath, cfgJSON, 0o600); err != nil {
		res.Error = err.Error()
		return res
	}

	cmd := exec.Command(xrayBin, "run", "-c", cfgPath)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		res.Error = "start xray: " + err.Error()
		return res
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	if err := waitTCP(net.JoinHostPort("127.0.0.1", strconv.Itoa(xrayPort)), 5*time.Second); err != nil {
		res.Error = "xray not up: " + err.Error()
		return res
	}

	dialHost, dialPort := "127.0.0.1", xrayPort
	var dials atomic.Int64
	// Always front Xray with a counting proxy so physical dials are measured.
	// Optional one-way delay approximates RTT without netem (does not affect TCP cwnd).
	half := time.Duration(0)
	if rttMs > 0 {
		half = time.Duration(rttMs/2) * time.Millisecond
	}
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		res.Error = err.Error()
		return res
	}
	defer proxyLn.Close()
	go serveDelayProxy(proxyLn, net.JoinHostPort("127.0.0.1", strconv.Itoa(xrayPort)), half, &dials)
	dialPort = proxyLn.Addr().(*net.TCPAddr).Port

	opt := outbound.VlessOption{
		Name: "bench", Server: dialHost, Port: dialPort, UUID: uuid,
	}
	if cfg.MuxEnabled {
		opt.XrayMux = outbound.XrayMuxOption{
			Enabled: true, Concurrency: cfg.Concurrency, MaxConnections: cfg.MaxConnections,
		}
	}
	v, err := outbound.NewVless(opt)
	if err != nil {
		res.Error = "new vless: " + err.Error()
		return res
	}
	defer v.Close()

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
		total    atomic.Int64
		perMbps  = make([]float64, cfg.Streams)
		peakRSS  atomic.Uint64
		peakXray atomic.Uint64
		stopSamp = make(chan struct{})
	)
	go sampleRSS(os.Getpid(), &peakRSS, stopSamp)
	if cmd.Process != nil {
		go sampleRSS(cmd.Process.Pid, &peakXray, stopSamp)
	}

	cpuBefore := processCPUSeconds(os.Getpid())
	start := time.Now()
	for i := 0; i < cfg.Streams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st := time.Now()
			n, err := downloadVia(v, originPort, payloadBytes)
			total.Add(n)
			elapsed := time.Since(st).Seconds()
			if elapsed > 0 {
				perMbps[i] = float64(n) * 8 / elapsed / 1e6
			}
			if err != nil {
				errOnce.Do(func() { firstErr = err })
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(stopSamp)

	cpuAfter := processCPUSeconds(os.Getpid())
	res.Seconds = elapsed.Seconds()
	res.BytesTotal = total.Load()
	if res.Seconds > 0 {
		res.Mbps = float64(res.BytesTotal) * 8 / res.Seconds / 1e6
	}
	res.PerStreamMbps = perMbps
	res.PeakClientRSS = peakRSS.Load()
	res.PeakXrayRSS = peakXray.Load()
	res.ClientCPUSec = cpuAfter - cpuBefore
	res.PhysicalDials = dials.Load()

	want := payloadBytes * int64(cfg.Streams)
	if firstErr != nil {
		res.Error = firstErr.Error()
		return res
	}
	if res.BytesTotal != want {
		res.Error = fmt.Sprintf("bytes %d want %d", res.BytesTotal, want)
		return res
	}
	res.OK = true
	return res
}

func downloadVia(v *outbound.Vless, originPort int, want int64) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	conn, err := v.DialContext(ctx, meta(originPort))
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(120 * time.Second))
	req := "GET /bench.bin HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		return 0, err
	}
	buf := make([]byte, 64*1024)
	var (
		total   int64
		hdrDone bool
		pending []byte
	)
	for total < want {
		n, err := conn.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if !hdrDone {
				pending = append(pending, chunk...)
				if idx := indexHeaderEnd(pending); idx >= 0 {
					body := pending[idx:]
					total += int64(len(body))
					hdrDone = true
					pending = nil
				}
			} else {
				total += int64(n)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func indexHeaderEnd(b []byte) int {
	for i := 0; i+3 < len(b); i++ {
		if b[i] == '\r' && b[i+1] == '\n' && b[i+2] == '\r' && b[i+3] == '\n' {
			return i + 4
		}
	}
	return -1
}

func meta(port int) *C.Metadata {
	return &C.Metadata{
		NetWork: C.TCP,
		Host:    "127.0.0.1",
		DstIP:   netip.MustParseAddr("127.0.0.1"),
		DstPort: uint16(port),
	}
}

func serveDelayProxy(ln net.Listener, upstream string, oneWay time.Duration, dials *atomic.Int64) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		dials.Add(1)
		go func(c net.Conn) {
			defer c.Close()
			u, err := net.DialTimeout("tcp", upstream, 5*time.Second)
			if err != nil {
				return
			}
			defer u.Close()
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); delayCopy(u, c, oneWay) }()
			go func() { defer wg.Done(); delayCopy(c, u, oneWay) }()
			wg.Wait()
		}(c)
	}
}

func delayCopy(dst net.Conn, src net.Conn, d time.Duration) {
	type item struct {
		buf []byte
		at  time.Time
	}
	if d <= 0 {
		_, _ = io.Copy(dst, src)
		return
	}
	ch := make(chan item, 64)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for it := range ch {
			if wait := time.Until(it.at); wait > 0 {
				time.Sleep(wait)
			}
			if _, err := dst.Write(it.buf); err != nil {
				return
			}
		}
	}()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			cp := make([]byte, n)
			copy(cp, buf[:n])
			ch <- item{buf: cp, at: time.Now().Add(d)}
		}
		if err != nil {
			close(ch)
			wg.Wait()
			return
		}
	}
}

func waitTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}

func sampleRSS(pid int, peak *atomic.Uint64, stop <-chan struct{}) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if rss := readRSS(pid); rss > peak.Load() {
				peak.Store(rss)
			}
		}
	}
}

func readRSS(pid int) uint64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, _ := strconv.ParseUint(fields[1], 10, 64)
				return v * 1024
			}
		}
	}
	return 0
}

func processCPUSeconds(pid int) float64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(b))
	if len(fields) < 17 {
		return 0
	}
	utime, _ := strconv.ParseFloat(fields[13], 64)
	stime, _ := strconv.ParseFloat(fields[14], 64)
	return (utime + stime) / float64(clockTicks())
}

func clockTicks() int64 {
	return 100
}

func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := uint64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func renderMarkdown(sum summary) string {
	var b strings.Builder
	b.WriteString("# xray-mux local bench\n\n")
	fmt.Fprintf(&b, "- started: `%s`\n- payload/stream: **%d MiB**\n- simulated RTT: **%d ms**\n- GOMAXPROCS: %d\n\n",
		sum.StartedAt, sum.PayloadMB, sum.RTTMs, sum.GOMAXPROCS)
	b.WriteString("| case | streams | conc | max-conn | dials | Mbps | peak RSS client | peak RSS xray | CPU s | ok |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
	for _, r := range sum.Results {
		c := r.Config
		conc, maxc := "-", "-"
		if c.MuxEnabled {
			conc = strconv.Itoa(c.Concurrency)
			maxc = strconv.Itoa(c.MaxConnections)
		} else {
			conc = "off"
			maxc = "off"
		}
		ok := "yes"
		if !r.OK {
			ok = "NO: " + r.Error
		}
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %d | %.1f | %s | %s | %.2f | %s |\n",
			c.Name, c.Streams, conc, maxc, r.PhysicalDials, r.Mbps,
			humanBytes(r.PeakClientRSS), humanBytes(r.PeakXrayRSS), r.ClientCPUSec, ok)
	}
	b.WriteString("\nNotes:\n")
	b.WriteString("- `dials` counted at the delay-proxy accept (physical TCP to Xray path).\n")
	b.WriteString("- RTT is simulated with a userspace half-delay proxy (not REALITY).\n")
	b.WriteString("- This is localhost+VLESS/TCP mux; mobile loss/jitter is not modeled.\n")
	return b.String()
}
