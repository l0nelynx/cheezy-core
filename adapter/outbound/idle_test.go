package outbound

import (
	"net"
	"testing"
	"time"
)

func TestIdleSessionKeepsStreamsAndPendingDials(t *testing.T) {
	var activity sessionActivity
	closed := 0
	closeSession := func() { closed++ }
	activity.acquire()
	activity.acquire()
	activity.release()
	activity.lastUse = time.Now().Add(-2 * time.Minute)
	activity.closeIdle(time.Minute, closeSession)
	if closed != 0 {
		t.Fatal("an open stream or pending dial must keep the session alive")
	}
	activity.release()
	activity.closeIdle(time.Minute, closeSession)
	if closed != 0 {
		t.Fatal("the idle timeout must start after the last stream closes")
	}
	activity.lastUse = time.Now().Add(-2 * time.Minute)
	activity.closeIdle(time.Minute, closeSession)
	activity.closeIdle(time.Minute, closeSession)
	if closed != 1 {
		t.Fatalf("closed %d times, want 1", closed)
	}
	activity.acquire()
	activity.release()
	activity.lastUse = time.Now().Add(-2 * time.Minute)
	activity.closeIdle(time.Minute, closeSession)
	if closed != 2 {
		t.Fatal("a session must remain reusable after an idle close")
	}
}

func TestCountedConnectionsReleaseOnceAfterClose(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	released := 0
	conn := &countedConn{Conn: left, release: func() {
		if _, err := left.Write([]byte{1}); err == nil {
			t.Fatal("the underlying connection must close before releasing its session")
		}
		released++
	}}
	_ = conn.Close()
	_ = conn.Close()
	if released != 1 {
		t.Fatalf("TCP released %d times", released)
	}
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pc := &countedPacketConn{PacketConn: packet, release: func() { released++ }}
	_ = pc.Close()
	_ = pc.Close()
	if released != 2 {
		t.Fatalf("TCP+UDP released %d times, want 2", released)
	}
}
