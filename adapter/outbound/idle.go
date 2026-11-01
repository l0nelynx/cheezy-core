package outbound

import (
	"errors"
	"net"
	"sync"
	"time"

	N "github.com/metacubex/mihomo/common/net"
)

// errIdleSession closes a session C.IdleCloser found unused.
var errIdleSession = errors.New("idle session closed to save power")

// sessionActivity counts both established streams and dials in progress. The
// same lock covers acquiring a stream and closing an idle session, so a new
// dial cannot race the close between its idle check and the actual shutdown.
type sessionActivity struct {
	mu         sync.Mutex
	open       int
	lastUse    time.Time
	idleClosed bool
}

func (s *sessionActivity) acquire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.open++
	s.lastUse = time.Now()
	s.idleClosed = false
}

func (s *sessionActivity) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.open--
	s.lastUse = time.Now()
}

func (s *sessionActivity) closeIdle(idle time.Duration, closeSession func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open != 0 || s.idleClosed || time.Since(s.lastUse) < idle {
		return
	}
	s.idleClosed = true
	closeSession()
}

// countedConn calls release once, when it is closed: how an adapter keeps the
// number of connections open through a shared session (see Hysteria2.CloseIdle).
type countedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *countedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

func (c *countedConn) Upstream() any {
	return c.Conn
}

func (c *countedConn) ReaderReplaceable() bool {
	return true
}

func (c *countedConn) WriterReplaceable() bool {
	return true
}

type countedPacketConn struct {
	net.PacketConn
	once    sync.Once
	release func()
}

func (c *countedPacketConn) Close() error {
	err := c.PacketConn.Close()
	c.once.Do(c.release)
	return err
}

func (c *countedPacketConn) Upstream() any {
	return c.PacketConn
}

var (
	_ N.ReaderWithUpstream = (*countedConn)(nil)
	_ N.WriterWithUpstream = (*countedConn)(nil)
)
