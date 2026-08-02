package xraymux

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"sync"
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
			meta, _ := encodeMetadata(id, statusKeep, optionData, nil)
			var size [2]byte
			binary.BigEndian.PutUint16(size[:], uint16(len(payload)))
			if writeAll(conn, append(append(meta, size[:]...), payload...)) != nil {
				return
			}
		}
	}
}

func TestEncodeMetadataMatchesMuxCoolLayout(t *testing.T) {
	frame, err := encodeMetadata(7, statusNew, 0, testTarget())
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

func TestPoolFillsWorkerBeforeOpeningAnother(t *testing.T) {
	var mu sync.Mutex
	dials := 0
	pool := NewPool(2, 0, echoDialer(&dials, &mu), func(context.Context) string { return "test:443" })
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

func TestPoolRejectsWhenPhysicalLimitIsFull(t *testing.T) {
	globalPermits = permitRegistry{counts: make(map[string]int)}
	var mu sync.Mutex
	dials := 0
	pool := NewPool(2, 1, echoDialer(&dials, &mu), func(context.Context) string { return "203.0.113.1:443" })
	defer pool.Close()
	c1, _ := pool.DialContext(context.Background(), testTarget())
	c2, _ := pool.DialContext(context.Background(), testTarget())
	if _, err := pool.DialContext(context.Background(), testTarget()); !errorsIs(err, errMaxConnections) {
		t.Fatalf("third dial error = %v", err)
	}
	_ = c1.Close()
	_ = c2.Close()
}

func TestPermitIsSharedAcrossPools(t *testing.T) {
	globalPermits = permitRegistry{counts: make(map[string]int)}
	var mu sync.Mutex
	dials := 0
	dial := echoDialer(&dials, &mu)
	key := func(context.Context) string { return netip.MustParseAddrPort("203.0.113.2:443").String() }
	p1 := NewPool(1, 1, dial, key)
	p2 := NewPool(1, 1, dial, key)
	defer p1.Close()
	defer p2.Close()
	c, err := p1.DialContext(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.DialContext(context.Background(), testTarget()); !errorsIs(err, errMaxConnections) {
		t.Fatalf("second pool error = %v", err)
	}
	_ = c.Close()
}

func TestWorkerRetiresAfter128Sessions(t *testing.T) {
	var mu sync.Mutex
	dials := 0
	pool := NewPool(1, 0, echoDialer(&dials, &mu), func(context.Context) string { return "test:443" })
	defer pool.Close()
	for i := 0; i < maxWorkerUses; i++ {
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
		t.Fatalf("129 sessions opened %d workers, want 2", dials)
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

func errorsIs(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
