package xraymux

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

func testTarget() *C.Metadata {
	return &C.Metadata{NetWork: C.TCP, Host: "example.com", DstPort: 443}
}

func echoDialer(counter *int, mu *sync.Mutex) physicalDialer {
	return func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		mu.Lock()
		*counter++
		mu.Unlock()
		go serveEchoMux(server)
		return client, nil
	}
}

func serveEchoMux(conn net.Conn) {
	defer conn.Close()
	for {
		id, status, option, err := readMetadata(conn)
		if err != nil {
			return
		}
		var payload []byte
		if option&optionData != 0 {
			var size [2]byte
			if _, err := io.ReadFull(conn, size[:]); err != nil {
				return
			}
			payload = make([]byte, binary.BigEndian.Uint16(size[:]))
			if _, err := io.ReadFull(conn, payload); err != nil {
				return
			}
		}
		if status == statusKeep && len(payload) > 0 {
			meta, _ := encodeMetadata(id, statusKeep, optionData, nil, nil)
			var size [2]byte
			binary.BigEndian.PutUint16(size[:], uint16(len(payload)))
			buffers := net.Buffers{meta, size[:], payload}
			if _, err := buffers.WriteTo(conn); err != nil {
				return
			}
		}
	}
}

func TestEncodeMetadataMatchesMuxCoolLayout(t *testing.T) {
	frame, err := encodeMetadata(7, statusNew, 0, testTarget(), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0, 20, // metadata length
		0, 7, statusNew, 0, targetTCP,
		1, 187, // port 443, then Xray address
		2, 11, 'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
	}
	if !bytes.Equal(frame, want) {
		t.Fatalf("frame = %v, want %v", frame, want)
	}
}

func TestEncodeMetadataUDPIncludesGlobalID(t *testing.T) {
	target := &C.Metadata{NetWork: C.UDP, Host: "dns.google", DstPort: 53}
	gid := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	frame, err := encodeMetadata(1, statusNew, 0, target, &gid)
	if err != nil {
		t.Fatal(err)
	}
	if frame[6] != targetUDP {
		t.Fatalf("network = %d, want UDP", frame[6])
	}
	if !bytes.Equal(frame[len(frame)-8:], gid[:]) {
		t.Fatalf("global id missing: %v", frame)
	}
}

func TestPoolFillsWorkerBeforeOpeningAnother(t *testing.T) {
	var mu sync.Mutex
	dials := 0
	pool := NewPool(Options{Concurrency: 2}, echoDialer(&dials, &mu), func(context.Context) string { return "test:443" })
	defer pool.Close()

	c1, err := pool.DialContext(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	c2, err := pool.DialContext(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if dials != 1 {
		t.Fatalf("two sessions opened %d physical connections", dials)
	}
	mu.Unlock()
	c3, err := pool.DialContext(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if dials != 2 {
		t.Fatalf("third session opened %d physical connections", dials)
	}
	mu.Unlock()

	if _, err := c1.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	_ = c1.SetReadDeadline(time.Now().Add(time.Second))
	got := make([]byte, 5)
	if _, err := io.ReadFull(c1, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("echo = %q", got)
	}
	_ = c1.Close()
	_ = c2.Close()
	_ = c3.Close()
}

func TestPoolPacksOverflowInsteadOfDropping(t *testing.T) {
	globalPermits = permitRegistry{counts: make(map[string]int)}
	var mu sync.Mutex
	dials := 0
	// concurrency=2 => hard=8; maxConnections=1 forces packing beyond soft limit.
	pool := NewPool(Options{Concurrency: 2, MaxConnections: 1}, echoDialer(&dials, &mu), func(context.Context) string {
		return "203.0.113.1:443"
	})
	defer pool.Close()

	var conns []net.Conn
	for i := 0; i < 5; i++ {
		c, err := pool.DialContext(context.Background(), testTarget())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, c)
	}
	mu.Lock()
	if dials != 1 {
		t.Fatalf("overflow packed onto %d workers, want 1", dials)
	}
	mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func TestPoolWaitsWhenHardConcurrencyFull(t *testing.T) {
	globalPermits = permitRegistry{counts: make(map[string]int)}
	var mu sync.Mutex
	dials := 0
	// concurrency=1 => hard=4; fill all 4, then wait must respect context.
	pool := NewPool(Options{Concurrency: 1, MaxConnections: 1}, echoDialer(&dials, &mu), func(context.Context) string {
		return "203.0.113.9:443"
	})
	defer pool.Close()

	var conns []net.Conn
	for i := 0; i < 4; i++ {
		c, err := pool.DialContext(context.Background(), testTarget())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, c)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := pool.DialContext(ctx, testTarget()); err == nil {
		t.Fatal("expected wait timeout when hard concurrency is full")
	}

	// Freeing a slot must unblock a waiter.
	_ = conns[0].Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	c, err := pool.DialContext(ctx2, testTarget())
	if err != nil {
		t.Fatalf("expected dial after free: %v", err)
	}
	_ = c.Close()
	for _, c := range conns[1:] {
		_ = c.Close()
	}
}

func TestPermitIsSharedAcrossPools(t *testing.T) {
	globalPermits = permitRegistry{counts: make(map[string]int)}
	var mu sync.Mutex
	dials := 0
	dial := echoDialer(&dials, &mu)
	key := func(context.Context) string { return netip.MustParseAddrPort("203.0.113.2:443").String() }
	// concurrency=1, hard=4 — fill hard on p1 so p2 cannot dial a new carrier and must wait/fail.
	p1 := NewPool(Options{Concurrency: 1, MaxConnections: 1}, dial, key)
	p2 := NewPool(Options{Concurrency: 1, MaxConnections: 1}, dial, key)
	defer p1.Close()
	defer p2.Close()
	var held []net.Conn
	for i := 0; i < 4; i++ {
		c, err := p1.DialContext(context.Background(), testTarget())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := p2.DialContext(ctx, testTarget()); err == nil {
		t.Fatal("second pool should not open beyond shared physical limit")
	}
	for _, c := range held {
		_ = c.Close()
	}
}

func TestWorkerRetiresAfterConfiguredUses(t *testing.T) {
	var mu sync.Mutex
	dials := 0
	const uses = 8
	pool := NewPool(Options{Concurrency: 1, MaxWorkerUses: uses}, echoDialer(&dials, &mu), func(context.Context) string { return "test:443" })
	defer pool.Close()
	for i := 0; i < uses; i++ {
		conn, err := pool.DialContext(context.Background(), testTarget())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		_ = conn.Close()
	}
	conn, err := pool.DialContext(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mu.Lock()
	defer mu.Unlock()
	if dials != 2 {
		t.Fatalf("%d sessions opened %d workers, want 2", uses+1, dials)
	}
}

func TestConcurrentDialDoesNotSerializeOnExistingWorker(t *testing.T) {
	var mu sync.Mutex
	dials := 0
	var dialGate sync.WaitGroup
	dialGate.Add(1)
	slowDial := func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		mu.Lock()
		dials++
		n := dials
		mu.Unlock()
		if n == 1 {
			// First carrier is immediate.
			go serveEchoMux(server)
			return client, nil
		}
		// Subsequent dials block until released — must not block soft-alloc on worker 1.
		dialGate.Wait()
		go serveEchoMux(server)
		return client, nil
	}
	pool := NewPool(Options{Concurrency: 8}, slowDial, func(context.Context) string { return "test:443" })
	defer pool.Close()

	c1, err := pool.DialContext(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	// Saturate soft concurrency so the next dial starts a new physical dial (blocked).
	var fillers []net.Conn
	for i := 0; i < 7; i++ {
		c, err := pool.DialContext(context.Background(), testTarget())
		if err != nil {
			t.Fatal(err)
		}
		fillers = append(fillers, c)
	}
	started := make(chan struct{})
	go func() {
		close(started)
		_, _ = pool.DialContext(context.Background(), testTarget())
	}()
	<-started
	time.Sleep(20 * time.Millisecond)

	// Free one soft slot; allocation onto the existing worker must succeed without
	// waiting for the in-flight physical dial.
	_ = fillers[0].Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c, err := pool.DialContext(ctx, testTarget())
	if err != nil {
		t.Fatalf("soft alloc blocked by in-flight dial: %v", err)
	}
	_ = c.Close()
	dialGate.Done()
	for _, c := range fillers[1:] {
		_ = c.Close()
	}
}

func TestPipeBackpressureDoesNotKillDownload(t *testing.T) {
	// Producer outruns consumer: the pipe must block (TCP backpressure), not
	// tear down the session. Killing on full was collapsing speedtest download.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	const total = 256 * 1024 // well above the 64KiB pipe
	payload := bytes.Repeat([]byte("D"), 4*1024)
	frames := total / len(payload)

	serverDone := make(chan error, 1)
	go func() {
		server, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer server.Close()
		if _, _, _, err := readMetadata(server); err != nil {
			serverDone <- err
			return
		}
		for i := 0; i < frames; i++ {
			meta, _ := encodeMetadata(1, statusKeep, optionData, nil, nil)
			var size [2]byte
			binary.BigEndian.PutUint16(size[:], uint16(len(payload)))
			buffers := net.Buffers{meta, size[:], payload}
			if _, err := buffers.WriteTo(server); err != nil {
				serverDone <- err
				return
			}
		}
		meta, _ := encodeMetadata(1, statusEnd, 0, nil, nil)
		if _, err := server.Write(meta); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	pool := NewPool(Options{Concurrency: 1}, func(context.Context) (net.Conn, error) {
		return net.Dial("tcp", ln.Addr().String())
	}, func(context.Context) string { return "test:443" })
	defer pool.Close()

	conn, err := pool.DialContext(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	got := 0
	buf := make([]byte, 8*1024)
	for got < total {
		n, err := conn.Read(buf)
		if n > 0 {
			got += n
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("download aborted after %d/%d bytes: %v", got, total, err)
		}
	}
	if got != total {
		t.Fatalf("got %d bytes, want %d", got, total)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestReadMetadataRejectsMalformedLength(t *testing.T) {
	if _, _, _, err := readMetadata(bytes.NewReader([]byte{0, 3, 0, 0, 0})); err == nil {
		t.Fatal("expected short metadata error")
	}
	if _, _, _, err := readMetadata(bytes.NewReader([]byte{2, 1})); err == nil {
		t.Fatal("expected oversized metadata error")
	}
}

func TestDialDoesNotHoldLockAcrossPhysicalDial(t *testing.T) {
	var dials atomic.Int32
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	dial := func(context.Context) (net.Conn, error) {
		dials.Add(1)
		started <- struct{}{}
		<-release
		client, server := net.Pipe()
		go serveEchoMux(server)
		return client, nil
	}
	pool := NewPool(Options{Concurrency: 1}, dial, func(context.Context) string { return "lock-test:443" })
	defer pool.Close()

	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := pool.DialContext(context.Background(), testTarget())
			errCh <- err
		}()
	}
	// While dial is blocked, Close must still return (pool lock not held across dial).
	<-started
	done := make(chan struct{})
	go func() {
		_ = pool.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		close(release)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Close blocked while dial in progress")
		}
		return
	}
	close(release)
}
