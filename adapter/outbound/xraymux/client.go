package xraymux

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

const (
	statusNew       byte = 0x01
	statusKeep      byte = 0x02
	statusEnd       byte = 0x03
	statusKeepAlive byte = 0x04
	optionData      byte = 0x01
	targetTCP       byte = 0x01
	maxFramePayload      = 8 * 1024
	maxMetadataSize      = 512
	maxWorkerUses        = 128
	workerIdleTime       = 16 * time.Second
)

var errMaxConnections = errors.New("xray mux: max connections reached")

type physicalDialer func(context.Context) (net.Conn, error)
type endpointKeyer func(context.Context) string

// Pool multiplexes logical TCP connections over Xray Mux.Cool workers.
type Pool struct {
	mu             sync.Mutex
	workers        []*worker
	concurrency    int
	maxConnections int
	dial           physicalDialer
	endpointKey    endpointKeyer
	closed         bool
}

func NewPool(concurrency, maxConnections int, dial physicalDialer, endpointKey endpointKeyer) *Pool {
	return &Pool{concurrency: concurrency, maxConnections: maxConnections, dial: dial, endpointKey: endpointKey}
}

func (p *Pool) DialContext(ctx context.Context, metadata *C.Metadata) (net.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, net.ErrClosed
	}
	active := p.workers[:0]
	for _, w := range p.workers {
		if !w.isClosed() {
			active = append(active, w)
		}
	}
	p.workers = active
	for _, w := range p.workers {
		if conn := w.allocate(metadata); conn != nil {
			return conn, nil
		}
	}

	key := ""
	if p.maxConnections > 0 {
		key = p.endpointKey(ctx)
		if !globalPermits.acquire(key, p.maxConnections) {
			return nil, errMaxConnections
		}
	}
	physical, err := p.dial(ctx)
	if err != nil {
		if key != "" {
			globalPermits.release(key)
		}
		return nil, err
	}
	w := newWorker(physical, p.concurrency, func() {
		if key != "" {
			globalPermits.release(key)
		}
	})
	p.workers = append(p.workers, w)
	conn := w.allocate(metadata)
	if conn == nil {
		w.close()
		return nil, errors.New("xray mux: failed to allocate session")
	}
	return conn, nil
}

func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	workers := append([]*worker(nil), p.workers...)
	p.workers = nil
	p.mu.Unlock()
	for _, w := range workers {
		w.close()
	}
	return nil
}

type permitRegistry struct {
	sync.Mutex
	counts map[string]int
}

var globalPermits = permitRegistry{counts: make(map[string]int)}

func (r *permitRegistry) acquire(key string, limit int) bool {
	r.Lock()
	defer r.Unlock()
	if r.counts[key] >= limit {
		return false
	}
	r.counts[key]++
	return true
}

func (r *permitRegistry) release(key string) {
	r.Lock()
	defer r.Unlock()
	if r.counts[key] <= 1 {
		delete(r.counts, key)
		return
	}
	r.counts[key]--
}

type worker struct {
	mu          sync.Mutex
	writeMu     sync.Mutex
	conn        net.Conn
	sessions    map[uint16]*session
	concurrency int
	total       uint16
	closed      bool
	idleTimer   *time.Timer
	release     func()
	closeOnce   sync.Once
}

func newWorker(conn net.Conn, concurrency int, release func()) *worker {
	w := &worker{conn: conn, sessions: make(map[uint16]*session), concurrency: concurrency, release: release}
	go w.readLoop()
	return w
}

func (w *worker) allocate(metadata *C.Metadata) net.Conn {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || len(w.sessions) >= w.concurrency || w.total >= maxWorkerUses {
		return nil
	}
	if w.idleTimer != nil {
		w.idleTimer.Stop()
		w.idleTimer = nil
	}
	w.total++
	s := &session{
		id:      w.total,
		worker:  w,
		inbound: make(chan []byte, 8), // Xray uses a 64 KiB per-worker pipe.
		done:    make(chan struct{}),
	}
	w.sessions[s.id] = s
	if err := w.writeFrame(s.id, statusNew, 0, metadata, nil); err != nil {
		delete(w.sessions, s.id)
		s.once.Do(func() { close(s.done) })
		go w.close()
		return nil
	}
	return s
}

func (w *worker) removeSession(id uint16) {
	w.mu.Lock()
	delete(w.sessions, id)
	shouldClose := w.total >= maxWorkerUses && len(w.sessions) == 0
	if !shouldClose && len(w.sessions) == 0 && !w.closed {
		w.idleTimer = time.AfterFunc(workerIdleTime, w.close)
	}
	w.mu.Unlock()
	if shouldClose {
		w.close()
	}
}

func (w *worker) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

func (w *worker) close() {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		if w.idleTimer != nil {
			w.idleTimer.Stop()
		}
		sessions := make([]*session, 0, len(w.sessions))
		for _, s := range w.sessions {
			sessions = append(sessions, s)
		}
		w.sessions = make(map[uint16]*session)
		w.mu.Unlock()
		_ = w.conn.Close()
		for _, s := range sessions {
			s.finish(false)
		}
		if w.release != nil {
			w.release()
		}
	})
}

func (w *worker) writeFrame(id uint16, status, option byte, target *C.Metadata, payload []byte) error {
	meta, err := encodeMetadata(id, status, option, target)
	if err != nil {
		return err
	}
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if err := writeAll(w.conn, meta); err != nil {
		return err
	}
	if option&optionData != 0 {
		var size [2]byte
		binary.BigEndian.PutUint16(size[:], uint16(len(payload)))
		if err := writeAll(w.conn, size[:]); err != nil {
			return err
		}
		return writeAll(w.conn, payload)
	}
	return nil
}

func (w *worker) readLoop() {
	defer w.close()
	for {
		id, status, option, err := readMetadata(w.conn)
		if err != nil {
			return
		}
		var payload []byte
		if option&optionData != 0 {
			var size [2]byte
			if _, err = io.ReadFull(w.conn, size[:]); err != nil {
				return
			}
			payload = make([]byte, int(binary.BigEndian.Uint16(size[:])))
			if _, err = io.ReadFull(w.conn, payload); err != nil {
				return
			}
		}
		w.mu.Lock()
		s := w.sessions[id]
		w.mu.Unlock()
		switch status {
		case statusKeep:
			if s == nil {
				_ = w.writeFrame(id, statusEnd, 0, nil, nil)
				continue
			}
			if len(payload) > 0 {
				if err := s.push(payload); err != nil {
					s.finish(true)
				}
			}
		case statusEnd:
			if s != nil {
				s.finish(false)
			}
		case statusKeepAlive, statusNew:
			// Xray clients discard unsolicited heartbeat/reverse data.
		default:
			return
		}
	}
}

type session struct {
	id            uint16
	worker        *worker
	inbound       chan []byte
	done          chan struct{}
	once          sync.Once
	readMu        sync.Mutex
	readBuf       []byte
	deadlineMu    sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

func (s *session) push(payload []byte) error {
	copyPayload := append([]byte(nil), payload...)
	select {
	case s.inbound <- copyPayload:
		return nil
	case <-s.done:
		return net.ErrClosed
	}
}

func (s *session) finish(sendEnd bool) {
	s.once.Do(func() {
		if sendEnd && !s.worker.isClosed() {
			_ = s.worker.writeFrame(s.id, statusEnd, 0, nil, nil)
		}
		close(s.done)
		s.worker.removeSession(s.id)
	})
}

func (s *session) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if len(s.readBuf) > 0 {
		n := copy(p, s.readBuf)
		s.readBuf = s.readBuf[n:]
		return n, nil
	}
	deadline := s.getReadDeadline()
	var timer <-chan time.Time
	if !deadline.IsZero() {
		if !time.Now().Before(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		t := time.NewTimer(time.Until(deadline))
		defer t.Stop()
		timer = t.C
	}
	select {
	case data := <-s.inbound:
		n := copy(p, data)
		s.readBuf = data[n:]
		return n, nil
	case <-s.done:
		return 0, io.EOF
	case <-timer:
		return 0, os.ErrDeadlineExceeded
	}
}

func (s *session) Write(p []byte) (int, error) {
	select {
	case <-s.done:
		return 0, net.ErrClosed
	default:
	}
	deadline := s.getWriteDeadline()
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return 0, os.ErrDeadlineExceeded
	}
	written := 0
	for len(p) > 0 {
		n := len(p)
		if n > maxFramePayload {
			n = maxFramePayload
		}
		if err := s.worker.writeFrame(s.id, statusKeep, optionData, nil, p[:n]); err != nil {
			s.worker.close()
			return written, err
		}
		written += n
		p = p[n:]
	}
	return written, nil
}

func (s *session) Close() error         { s.finish(true); return nil }
func (s *session) LocalAddr() net.Addr  { return muxAddr("xray-mux-local") }
func (s *session) RemoteAddr() net.Addr { return muxAddr("xray-mux-remote") }

func (s *session) SetDeadline(t time.Time) error {
	s.deadlineMu.Lock()
	s.readDeadline, s.writeDeadline = t, t
	s.deadlineMu.Unlock()
	return nil
}

func (s *session) SetReadDeadline(t time.Time) error {
	s.deadlineMu.Lock()
	s.readDeadline = t
	s.deadlineMu.Unlock()
	return nil
}

func (s *session) SetWriteDeadline(t time.Time) error {
	s.deadlineMu.Lock()
	s.writeDeadline = t
	s.deadlineMu.Unlock()
	return nil
}

func (s *session) getReadDeadline() time.Time {
	s.deadlineMu.Lock()
	defer s.deadlineMu.Unlock()
	return s.readDeadline
}

func (s *session) getWriteDeadline() time.Time {
	s.deadlineMu.Lock()
	defer s.deadlineMu.Unlock()
	return s.writeDeadline
}

type muxAddr string

func (a muxAddr) Network() string { return "xray-mux" }
func (a muxAddr) String() string  { return string(a) }

func encodeMetadata(id uint16, status, option byte, target *C.Metadata) ([]byte, error) {
	body := make([]byte, 4, 64)
	binary.BigEndian.PutUint16(body[:2], id)
	body[2] = status
	body[3] = option
	if status == statusNew {
		if target == nil || target.NetWork != C.TCP {
			return nil, errors.New("xray mux: only TCP targets are supported")
		}
		body = append(body, targetTCP)
		var port [2]byte
		binary.BigEndian.PutUint16(port[:], target.DstPort)
		body = append(body, port[:]...)
		var err error
		body, err = appendAddress(body, target)
		if err != nil {
			return nil, err
		}
	}
	if len(body) > maxMetadataSize {
		return nil, errors.New("xray mux: metadata too large")
	}
	frame := make([]byte, 2, len(body)+2)
	binary.BigEndian.PutUint16(frame, uint16(len(body)))
	return append(frame, body...), nil
}

func appendAddress(dst []byte, metadata *C.Metadata) ([]byte, error) {
	if metadata.Host != "" {
		if len(metadata.Host) > 255 {
			return nil, errors.New("xray mux: domain name too long")
		}
		dst = append(dst, 0x02, byte(len(metadata.Host)))
		return append(dst, metadata.Host...), nil
	}
	if metadata.DstIP.Is4() {
		dst = append(dst, 0x01)
		return append(dst, metadata.DstIP.AsSlice()...), nil
	}
	if metadata.DstIP.Is6() {
		dst = append(dst, 0x03)
		return append(dst, metadata.DstIP.AsSlice()...), nil
	}
	return nil, errors.New("xray mux: target address is empty")
}

func readMetadata(r io.Reader) (id uint16, status, option byte, err error) {
	var size [2]byte
	if _, err = io.ReadFull(r, size[:]); err != nil {
		return
	}
	n := int(binary.BigEndian.Uint16(size[:]))
	if n < 4 || n > maxMetadataSize {
		err = fmt.Errorf("xray mux: invalid metadata length %d", n)
		return
	}
	meta := make([]byte, n)
	if _, err = io.ReadFull(r, meta); err != nil {
		return
	}
	id = binary.BigEndian.Uint16(meta[:2])
	status = meta[2]
	option = meta[3]
	return
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}
