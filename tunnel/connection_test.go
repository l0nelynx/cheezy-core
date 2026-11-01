package tunnel

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

type queueTestPacket struct {
	C.PacketAdapter
	id       byte
	drops    atomic.Int32
	metadata C.Metadata
}

func newQueueTestPacket(id byte) *queueTestPacket {
	return &queueTestPacket{id: id, metadata: C.Metadata{
		NetWork: C.UDP, DstIP: netip.MustParseAddr("192.0.2.1"), DstPort: 1234,
	}}
}

func (p *queueTestPacket) Data() []byte          { return []byte{p.id} }
func (p *queueTestPacket) Drop()                 { p.drops.Add(1) }
func (p *queueTestPacket) Metadata() *C.Metadata { return &p.metadata }

type queueTestConn struct {
	C.PacketConn
	delay   time.Duration
	write   func([]byte) error
	resolve func(*C.Metadata) error
}

func (c *queueTestConn) UDPMaxQueueDelay() time.Duration { return c.delay }
func (c *queueTestConn) WriteTo(payload []byte, _ net.Addr) (int, error) {
	if err := c.write(payload); err != nil {
		return 0, err
	}
	return len(payload), nil
}
func (c *queueTestConn) SetReadDeadline(time.Time) error { return nil }
func (c *queueTestConn) ResolveUDP(_ context.Context, metadata *C.Metadata) error {
	return c.resolve(metadata)
}

// Models the tracker / reference wrappers around the actual packet connection.
type queueTestWrapper struct{ C.PacketConn }

func (c queueTestWrapper) Upstream() any { return c.PacketConn }

func enableQueueDiagnostics(t *testing.T) *statistic.UDPDiagnostics {
	t.Helper()
	statistic.SetUDPDiagnosticsEnabled(false)
	statistic.SetUDPDiagnosticsEnabled(true)
	t.Cleanup(func() { statistic.SetUDPDiagnosticsEnabled(false) })
	return statistic.ActiveUDPDiagnostics()
}

func TestUDPSenderExpiryBehindBlockedWrite(t *testing.T) {
	diagnostics := enableQueueDiagnostics(t)
	sender := newPacketSender().(*packetSender)
	entered := make(chan struct{})
	release := make(chan struct{})
	writes := make(chan byte, 3)
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	conn := &queueTestConn{delay: time.Second, write: func(payload []byte) error {
		if payload[0] == 1 {
			close(entered)
			<-release
		}
		writes <- payload[0]
		return nil
	}}
	first, stale, fresh := newQueueTestPacket(1), newQueueTestPacket(2), newQueueTestPacket(3)
	sender.Send(first)
	done := make(chan struct{})
	go func() { sender.Process(queueTestWrapper{conn}, nil); close(done) }()
	defer func() { sender.Close(); unblock.Do(func() { close(release) }); <-done }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first write did not start")
	}
	sender.ch <- queuedUDPPacket{packet: stale, queuedAt: time.Now().Add(-time.Minute), diagnostics: diagnostics}
	sender.Send(fresh)
	unblock.Do(func() { close(release) })
	for _, want := range []byte{1, 3} {
		select {
		case got := <-writes:
			if got != want {
				t.Fatalf("sent packet %d, want %d", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("missing packet %d", want)
		}
	}
	sender.Close()
	<-done
	for _, packet := range []*queueTestPacket{first, stale, fresh} {
		if packet.drops.Load() != 1 {
			t.Fatalf("packet %d released %d times", packet.id, packet.drops.Load())
		}
	}
	snapshot := statistic.SnapshotUDPDiagnostics()
	if snapshot.Drops["queueExpired"] != 1 || snapshot.OutboundWrite.Count != 2 || snapshot.SenderQueue.Count != 3 {
		t.Fatalf("unexpected measurements: %+v", snapshot)
	}
}

func TestUDPSenderPolicyDisabledPreservesOldPackets(t *testing.T) {
	statistic.SetUDPDiagnosticsEnabled(false)
	sender := newPacketSender().(*packetSender)
	defer sender.Close()
	packet := newQueueTestPacket(1)
	writes := 0
	conn := &queueTestConn{write: func([]byte) error { writes++; return nil }}
	sender.processPacket(conn, nil, queuedUDPPacket{packet: packet, queuedAt: time.Now().Add(-time.Hour)}, 0)
	if writes != 1 || packet.drops.Load() != 1 {
		t.Fatal("disabled policy discarded an old packet")
	}
	sender.maxQueueDelay.Store(0)
	sender.Send(newQueueTestPacket(2))
	queued := <-sender.ch
	defer queued.packet.Drop()
	if !queued.queuedAt.IsZero() || queued.diagnostics != nil {
		t.Fatal("disabled established session captured timing")
	}
}

func TestUDPSenderColdQueueExpiry(t *testing.T) {
	diagnostics := enableQueueDiagnostics(t)
	sender := newPacketSender().(*packetSender)
	defer sender.Close()
	packet := newQueueTestPacket(1)
	sender.Send(packet)
	queued := <-sender.ch
	if queued.queuedAt.IsZero() {
		t.Fatal("connection setup must preserve packet age")
	}
	queued.queuedAt = queued.queuedAt.Add(-time.Minute)
	conn := &queueTestConn{write: func([]byte) error { t.Fatal("expired setup packet was written"); return nil }}
	sender.processPacket(conn, nil, queued, time.Second)
	if packet.drops.Load() != 1 || diagnostics == nil || statistic.SnapshotUDPDiagnostics().Drops["queueExpired"] != 1 {
		t.Fatal("setup expiry was not recorded / released")
	}
}

func TestUDPSenderExpiryAfterResolution(t *testing.T) {
	enableQueueDiagnostics(t)
	sender := newPacketSender().(*packetSender)
	defer sender.Close()
	packet := newQueueTestPacket(1)
	packet.metadata.DstIP = netip.Addr{}
	packet.metadata.Host = "queue-test.invalid"
	queued := queuedUDPPacket{packet: packet, queuedAt: time.Now(), diagnostics: statistic.ActiveUDPDiagnostics()}
	const limit = 100 * time.Millisecond
	resolved := false
	conn := &queueTestConn{
		resolve: func(metadata *C.Metadata) error {
			resolved = true
			metadata.DstIP = netip.MustParseAddr("192.0.2.1")
			time.Sleep(time.Until(queued.queuedAt.Add(limit + time.Millisecond)))
			return nil
		},
		write: func([]byte) error { t.Fatal("packet expired during DNS was written"); return nil },
	}
	sender.processPacket(conn, nil, queued, limit)
	if !resolved || packet.drops.Load() != 1 || statistic.SnapshotUDPDiagnostics().Drops["queueExpired"] != 1 {
		t.Fatal("expiry after resolution failed")
	}
}

func TestUDPSenderQueueOverflowAndClose(t *testing.T) {
	enableQueueDiagnostics(t)
	sender := newPacketSender().(*packetSender)
	packets := make([]*queueTestPacket, senderCapacity+2)
	for i := 0; i < senderCapacity+1; i++ {
		packets[i] = newQueueTestPacket(byte(i))
		sender.Send(packets[i])
	}
	sender.Close()
	packets[senderCapacity+1] = newQueueTestPacket(0)
	sender.Send(packets[senderCapacity+1])
	sender.Close()
	for i, packet := range packets {
		if packet.drops.Load() != 1 {
			t.Fatalf("packet %d released %d times", i, packet.drops.Load())
		}
	}
	snapshot := statistic.SnapshotUDPDiagnostics()
	if snapshot.Drops["senderQueueFull"] != 1 || snapshot.Drops["senderClosed"] != senderCapacity+1 || len(sender.ch) != 0 {
		t.Fatalf("unexpected close/overflow counts: %+v", snapshot)
	}
}

func TestUDPSenderConcurrentClose(t *testing.T) {
	statistic.SetUDPDiagnosticsEnabled(false)
	for round := 0; round < 25; round++ {
		sender := newPacketSender().(*packetSender)
		var workers sync.WaitGroup
		packets := make([]*queueTestPacket, 256)
		for i := range packets {
			packets[i] = newQueueTestPacket(byte(i))
			workers.Add(1)
			go func(packet *queueTestPacket) { defer workers.Done(); sender.Send(packet) }(packets[i])
		}
		sender.Close()
		workers.Wait()
		if len(sender.ch) != 0 {
			t.Fatal("packet admitted after close drained the queue")
		}
		for _, packet := range packets {
			if packet.drops.Load() != 1 {
				t.Fatalf("packet released %d times", packet.drops.Load())
			}
		}
	}
}

func TestUDPSenderWriteErrorDiagnostics(t *testing.T) {
	diagnostics := enableQueueDiagnostics(t)
	sender := newPacketSender().(*packetSender)
	defer sender.Close()
	packet := newQueueTestPacket(1)
	conn := &queueTestConn{write: func([]byte) error { return errors.New("test write error") }}
	sender.processPacket(conn, nil, queuedUDPPacket{packet: packet, queuedAt: time.Now(), diagnostics: diagnostics}, 0)
	snapshot := statistic.SnapshotUDPDiagnostics()
	if snapshot.Drops["writeError"] != 1 || snapshot.OutboundWrite.Count != 1 || packet.drops.Load() != 1 {
		t.Fatalf("write error not recorded: %+v", snapshot)
	}
}

func TestUDPIngressQueueOverflow(t *testing.T) {
	enableQueueDiagnostics(t)
	queue := make(chan queuedUDPPacket, 1)
	first, overflow := newQueueTestPacket(1), newQueueTestPacket(2)
	enqueueUDPPacket(queue, first)
	enqueueUDPPacket(queue, overflow)
	queued := <-queue
	queued.packet.Drop()
	if queued.queuedAt.IsZero() || queued.diagnostics == nil {
		t.Fatal("ingress packet is missing its diagnostic timestamp")
	}
	if first.drops.Load() != 1 || overflow.drops.Load() != 1 || statistic.SnapshotUDPDiagnostics().Drops["ingressQueueFull"] != 1 {
		t.Fatal("ingress overflow was not recorded / released")
	}
	statistic.SetUDPDiagnosticsEnabled(false)
	enqueueUDPPacket(queue, newQueueTestPacket(3))
	queued = <-queue
	queued.packet.Drop()
	if !queued.queuedAt.IsZero() || queued.diagnostics != nil {
		t.Fatal("disabled ingress diagnostics captured timing")
	}
}

func BenchmarkUDPSenderQueue(b *testing.B) {
	for _, test := range []struct {
		name        string
		diagnostics bool
		delay       time.Duration
	}{
		{name: "disabled"},
		{name: "expiry", delay: 50 * time.Millisecond},
		{name: "diagnostics", diagnostics: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			statistic.SetUDPDiagnosticsEnabled(false)
			statistic.SetUDPDiagnosticsEnabled(test.diagnostics)
			defer statistic.SetUDPDiagnosticsEnabled(false)
			sender := newPacketSender().(*packetSender)
			defer sender.Close()
			sender.maxQueueDelay.Store(int64(test.delay))
			packet := newQueueTestPacket(1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sender.Send(packet)
				queued := <-sender.ch
				if queued.diagnostics != nil {
					queued.diagnostics.ObserveSenderQueue(time.Since(queued.queuedAt))
				}
				queued.packet.Drop()
			}
		})
	}
}
