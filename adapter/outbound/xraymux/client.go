package xraymux

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/contextutils"
	"github.com/metacubex/mihomo/common/pool"
	C "github.com/metacubex/mihomo/constant"

	"golang.org/x/sync/singleflight"
)

const (
	statusNew          byte = 0x01
	statusKeep         byte = 0x02
	statusEnd          byte = 0x03
	statusKeepAlive    byte = 0x04
	optionData         byte = 0x01
	optionError        byte = 0x02
	targetTCP          byte = 0x01
	targetUDP          byte = 0x02
	maxFramePayload         = 8 * 1024
	maxMetadataSize         = 512
	defaultMaxUses          = 128
	defaultConcurrency      = 8
	// Per-session download window. xray-core's 64KiB pipe.WithSizeLimit is the
	// *worker carrier* cushion between TLS and demux — not the app buffer.
	// Session output there defaults to policy.Buffer.PerConnection (512KiB on
	// amd64). Matching the carrier limit here capped bulk download at ~window/RTT
	// (~60Mbps @ ~8ms) after the initial socket-buffer burst.
	sessionPipeLimit = 512 * 1024
	// Async carrier downlink cushion between TLS/VLESS and demux — same role as
	// xray-core pipe.WithSizeLimit(64KiB) on the DialingWorkerFactory link.
	// Sized above xray's 64KiB so brief demux stalls on LTE (~200ms RTT) do not
	// immediately collapse the TCP window to ~64KiB/RTT (~2.5Mbps).
	carrierDownlinkLimit = 256 * 1024
	workerReadBuffer     = 64 * 1024
	hardConcurrencyMult  = 4
	workerIdleTime       = 16 * time.Second
	// Wait briefly for the first application write so it can share the New
	// frame. This matches Xray's mux behavior while still allowing server-first
	// protocols to start after the timeout.
	defaultFirstPayloadTimeout = 100 * time.Millisecond
)

var (
	errMaxConnections = errors.New("xray mux: max connections reached")
	errDialRateLimit  = errors.New("xray mux: dial rate limit")
)

// ProtocolError identifies malformed mux.cool frames and remote session
// failures without hiding the underlying cause.
type ProtocolError struct {
	Op  string
	Err error
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("xray mux %s: %v", e.Op, e.Err)
}

func (e *ProtocolError) Unwrap() error { return e.Err }

func protocolError(op string, err error) error {
	return &ProtocolError{Op: op, Err: err}
}

type physicalDialer func(context.Context) (net.Conn, error)
type endpointKeyer func(context.Context) string

// Options configures a Mux.Cool client pool.
type Options struct {
	Concurrency    int // soft streams/worker; 0 -> 8
	MaxConnections int // physical carriers; 0 -> unlimited (pack-first)
	MaxWorkerUses  int // retire worker after N sessions; 0 -> 128
	// MaxDialsPerMinute caps new physical dials in a sliding 60s window
	// (handshake budget for censors). 0 -> unlimited.
	MaxDialsPerMinute int
	// FirstPayloadTimeout delays a TCP New frame so the first payload can be
	// coalesced into it. 0 uses 100ms; a negative value disables the delay.
	FirstPayloadTimeout time.Duration
}

// Pool multiplexes logical TCP connections over Xray Mux.Cool workers.
type Pool struct {
	mu                  sync.Mutex
	dialMu              sync.Mutex // serializes physical dials (handshake pacing)
	workers             []*worker
	concurrency         int
	hardConcurrency     int
	maxConnections      int
	maxWorkerUses       int
	maxDialsPerMinute   int
	firstPayloadTimeout time.Duration
	dial                physicalDialer
	endpointKey         endpointKeyer
	closed              bool
	dialGroup           singleflight.Group
	free                *sync.Cond
	dialTimes           []time.Time // sliding window for MaxDialsPerMinute
}

func NewPool(opts Options, dial physicalDialer, endpointKey endpointKeyer) *Pool {
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	maxUses := opts.MaxWorkerUses
	if maxUses <= 0 {
		maxUses = defaultMaxUses
	}
	hard := concurrency * hardConcurrencyMult
	if hard < concurrency {
		hard = concurrency
	}
	firstPayloadTimeout := opts.FirstPayloadTimeout
	if firstPayloadTimeout == 0 {
		firstPayloadTimeout = defaultFirstPayloadTimeout
	} else if firstPayloadTimeout < 0 {
		firstPayloadTimeout = 0
	}
	p := &Pool{
		concurrency:         concurrency,
		hardConcurrency:     hard,
		maxConnections:      opts.MaxConnections,
		maxWorkerUses:       maxUses,
		maxDialsPerMinute:   opts.MaxDialsPerMinute,
		firstPayloadTimeout: firstPayloadTimeout,
		dial:                dial,
		endpointKey:         endpointKey,
	}
	p.free = sync.NewCond(&p.mu)
	return p
}

func (p *Pool) DialContext(ctx context.Context, metadata *C.Metadata) (net.Conn, error) {
	if metadata == nil || metadata.NetWork != C.TCP {
		return nil, errors.New("xray mux: only TCP targets are supported")
	}
	return p.dialSession(ctx, metadata, nil)
}

// DialUDPContext opens a Mux.Cool UDP sub-connection (XUDP Global ID optional).
func (p *Pool) DialUDPContext(ctx context.Context, metadata *C.Metadata, globalID [8]byte) (net.Conn, error) {
	if metadata == nil || metadata.NetWork != C.UDP {
		return nil, errors.New("xray mux: DialUDPContext requires a UDP target")
	}
	return p.dialSession(ctx, metadata, &globalID)
}

func (p *Pool) dialSession(ctx context.Context, metadata *C.Metadata, globalID *[8]byte) (net.Conn, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// 1) Soft grow (xmux-inspired): while under max-connections, prefer a
		// new carrier before packing — but only if the dial-rate budget allows.
		// Physical dials are serialized so parallel opens become 1,2,3… carriers
		// instead of a handshake storm.
		if p.wantGrow() {
			w, err := p.dialWorkerNew(ctx)
			if err == nil {
				s := w.reserve(p.concurrency)
				if s != nil {
					if err := w.openSession(s, metadata, globalID, p.firstPayloadTimeout); err != nil {
						return nil, err
					}
					return s, nil
				}
			} else if !errors.Is(err, errMaxConnections) && !errors.Is(err, errDialRateLimit) {
				return nil, err
			}
			// Rate-limited or lost the last permit — pack instead.
		}

		// 2) Soft-allocate onto the least-loaded existing worker.
		if s := p.tryReserve(p.concurrency); s != nil {
			if err := s.worker.openSession(s, metadata, globalID, p.firstPayloadTimeout); err != nil {
				return nil, err
			}
			return s, nil
		}

		// 3) Soft full: dial a new carrier (pack-first path when max-connections=0).
		w, err := p.dialWorker(ctx)
		if err == nil {
			s := w.reserve(p.concurrency)
			if s == nil {
				continue
			}
			if err := w.openSession(s, metadata, globalID, p.firstPayloadTimeout); err != nil {
				return nil, err
			}
			return s, nil
		}
		if !errors.Is(err, errMaxConnections) && !errors.Is(err, errDialRateLimit) {
			return nil, err
		}

		// 4) At physical / rate limit: pack up to hard concurrency (least-loaded).
		if s := p.tryReserve(p.hardConcurrency); s != nil {
			if err := s.worker.openSession(s, metadata, globalID, p.firstPayloadTimeout); err != nil {
				return nil, err
			}
			return s, nil
		}

		// 5) Everything full: wait for a free stream slot (context-cancellable).
		if err := p.waitForSlot(ctx); err != nil {
			return nil, err
		}
	}
}

func (p *Pool) wantGrow() bool {
	if p.maxConnections <= 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.pruneLocked()
	return len(p.workers) < p.maxConnections
}

func (p *Pool) tryReserve(limit int) *session {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.pruneLocked()
	// Least-loaded among workers that still have soft/hard capacity.
	var best *worker
	bestN := int(^uint(0) >> 1)
	for _, w := range p.workers {
		w.mu.Lock()
		n := len(w.sessions)
		can := !w.closed && n < limit && int(w.total) < w.maxUses
		w.mu.Unlock()
		if can && n < bestN {
			best = w
			bestN = n
		}
	}
	if best == nil {
		return nil
	}
	return best.reserve(limit)
}

func (p *Pool) pruneLocked() {
	active := p.workers[:0]
	for _, w := range p.workers {
		if !w.isClosed() {
			active = append(active, w)
		}
	}
	p.workers = active
}

func (p *Pool) dialAllowedLocked(now time.Time) bool {
	if p.maxDialsPerMinute <= 0 {
		return true
	}
	cutoff := now.Add(-time.Minute)
	kept := p.dialTimes[:0]
	for _, t := range p.dialTimes {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	p.dialTimes = kept
	return len(p.dialTimes) < p.maxDialsPerMinute
}

func (p *Pool) recordDialLocked(now time.Time) {
	if p.maxDialsPerMinute <= 0 {
		return
	}
	p.dialTimes = append(p.dialTimes, now)
}

// dialWorkerNew always opens a new physical carrier (soft-grow path).
// Dials are serialized on dialMu; fails with errDialRateLimit / errMaxConnections.
func (p *Pool) dialWorkerNew(ctx context.Context) (*worker, error) {
	p.dialMu.Lock()
	defer p.dialMu.Unlock()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, net.ErrClosed
	}
	p.pruneLocked()
	if p.maxConnections > 0 && len(p.workers) >= p.maxConnections {
		p.mu.Unlock()
		return nil, errMaxConnections
	}
	now := time.Now()
	if !p.dialAllowedLocked(now) {
		p.mu.Unlock()
		return nil, errDialRateLimit
	}
	key := "_"
	if p.endpointKey != nil {
		if k := p.endpointKey(ctx); k != "" {
			key = k
		}
	}
	permitKey := ""
	if p.maxConnections > 0 {
		permitKey = key
		if !globalPermits.acquire(permitKey, p.maxConnections) {
			p.mu.Unlock()
			return nil, errMaxConnections
		}
	}
	p.recordDialLocked(now)
	p.mu.Unlock()

	physical, err := p.dial(ctx)
	if err != nil {
		if permitKey != "" {
			globalPermits.release(permitKey)
		}
		// Roll back rate-limit token on failed dial.
		p.mu.Lock()
		if n := len(p.dialTimes); n > 0 {
			p.dialTimes = p.dialTimes[:n-1]
		}
		p.mu.Unlock()
		return nil, err
	}

	release := func() {
		if permitKey != "" {
			globalPermits.release(permitKey)
		}
		p.signalFree()
	}
	w := newWorker(physical, p.maxWorkerUses, release)
	w.onFree = p.signalFree

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		w.close(net.ErrClosed)
		return nil, net.ErrClosed
	}
	p.workers = append(p.workers, w)
	p.mu.Unlock()
	return w, nil
}

func (p *Pool) dialWorker(ctx context.Context) (*worker, error) {
	key := "_"
	if p.endpointKey != nil {
		if k := p.endpointKey(ctx); k != "" {
			key = k
		}
	}

	v, err, _ := p.dialGroup.Do(key, func() (any, error) {
		// Serialize with grow-path dials so rate limit stays accurate.
		p.dialMu.Lock()
		defer p.dialMu.Unlock()

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, net.ErrClosed
		}
		p.pruneLocked()
		for _, w := range p.workers {
			if !w.isClosed() && w.hasCapacity(p.concurrency) {
				p.mu.Unlock()
				return w, nil
			}
		}
		now := time.Now()
		if !p.dialAllowedLocked(now) {
			p.mu.Unlock()
			return nil, errDialRateLimit
		}
		permitKey := ""
		if p.maxConnections > 0 {
			permitKey = key
			if !globalPermits.acquire(permitKey, p.maxConnections) {
				p.mu.Unlock()
				return nil, errMaxConnections
			}
		}
		p.recordDialLocked(now)
		p.mu.Unlock()

		physical, err := p.dial(ctx)
		if err != nil {
			if permitKey != "" {
				globalPermits.release(permitKey)
			}
			p.mu.Lock()
			if n := len(p.dialTimes); n > 0 {
				p.dialTimes = p.dialTimes[:n-1]
			}
			p.mu.Unlock()
			return nil, err
		}

		release := func() {
			if permitKey != "" {
				globalPermits.release(permitKey)
			}
			p.signalFree()
		}
		w := newWorker(physical, p.maxWorkerUses, release)
		w.onFree = p.signalFree

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			w.close(net.ErrClosed)
			return nil, net.ErrClosed
		}
		p.workers = append(p.workers, w)
		p.mu.Unlock()
		return w, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*worker), nil
}

func (p *Pool) waitForSlot(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	stop := contextutils.AfterFunc(ctx, func() {
		p.mu.Lock()
		p.free.Broadcast()
		p.mu.Unlock()
	})
	defer stop()

	for {
		if p.closed {
			return net.ErrClosed
		}
		p.pruneLocked()
		for _, w := range p.workers {
			if w.hasCapacity(p.hardConcurrency) {
				return nil
			}
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("waiting for xray mux slot: %w", err)
		}
		p.free.Wait()
	}
}

func (p *Pool) signalFree() {
	p.mu.Lock()
	p.free.Broadcast()
	p.mu.Unlock()
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
	p.free.Broadcast()
	p.mu.Unlock()
	for _, w := range workers {
		w.close(net.ErrClosed)
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
	mu        sync.Mutex
	writeMu   sync.Mutex
	conn      net.Conn
	downlink  *sessionPipe // async cushion: TLS reader → demux (xray carrier pipe)
	sessions  map[uint16]*session
	maxUses   int
	total     uint16
	closed    bool
	idleTimer *time.Timer
	release   func()
	closeOnce sync.Once
	onFree    func()
}

func newWorker(conn net.Conn, maxUses int, release func()) *worker {
	w := &worker{
		conn:     conn,
		downlink: newSessionPipe(carrierDownlinkLimit),
		sessions: make(map[uint16]*session),
		maxUses:  maxUses,
		release:  release,
	}
	go w.carrierFill()
	go w.readLoop()
	return w
}

func (w *worker) hasCapacity(limit int) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return !w.closed && len(w.sessions) < limit && int(w.total) < w.maxUses
}

// reserve creates a session slot under w.mu without doing any network I/O.
func (w *worker) reserve(limit int) *session {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || len(w.sessions) >= limit || int(w.total) >= w.maxUses {
		return nil
	}
	if w.idleTimer != nil {
		w.idleTimer.Stop()
		w.idleTimer = nil
	}
	w.total++
	s := &session{
		id:     w.total,
		worker: w,
		pipe:   newSessionPipe(sessionPipeLimit),
		done:   make(chan struct{}),
	}
	w.sessions[s.id] = s
	return s
}

// openSession arms the Mux.Cool New frame outside any worker/pool lock. TCP
// waits briefly for the first payload; UDP always starts with its first packet.
func (w *worker) openSession(s *session, metadata *C.Metadata, globalID *[8]byte, firstPayloadTimeout time.Duration) error {
	s.target = metadata
	if globalID != nil {
		s.globalID = *globalID
		s.hasGlobalID = true
	}
	if err := s.start(firstPayloadTimeout); err != nil {
		w.abandonSession(s, err)
		go w.close(err)
		return err
	}
	return nil
}

func (w *worker) abandonSession(s *session, cause error) {
	w.mu.Lock()
	delete(w.sessions, s.id)
	w.mu.Unlock()
	s.once.Do(func() {
		s.setTerminalCause(cause)
		s.stopFirstPayloadTimer()
		s.pipe.close(cause)
		close(s.done)
	})
	if w.onFree != nil {
		w.onFree()
	}
}

func (w *worker) removeSession(id uint16) {
	w.mu.Lock()
	delete(w.sessions, id)
	shouldClose := int(w.total) >= w.maxUses && len(w.sessions) == 0
	if !shouldClose && len(w.sessions) == 0 && !w.closed {
		w.idleTimer = time.AfterFunc(workerIdleTime, func() { w.close(net.ErrClosed) })
	}
	w.mu.Unlock()
	if w.onFree != nil {
		w.onFree()
	}
	if shouldClose {
		w.close(net.ErrClosed)
	}
}

func (w *worker) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

func (w *worker) close(cause error) {
	w.closeOnce.Do(func() {
		if cause == nil {
			cause = net.ErrClosed
		}
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
		if w.downlink != nil {
			w.downlink.drop(cause)
		}
		for _, s := range sessions {
			s.finishWithCause(false, false, cause)
		}
		if w.release != nil {
			w.release()
		}
	})
}

func (w *worker) writeFrame(id uint16, status, option byte, target *C.Metadata, globalID *[8]byte, payload []byte) error {
	if len(payload) > maxFramePayload {
		return protocolError("encode", fmt.Errorf("payload length %d exceeds %d", len(payload), maxFramePayload))
	}
	if len(payload) > 0 && option&optionData == 0 {
		return protocolError("encode", errors.New("payload provided without data option"))
	}
	meta, err := encodeMetadata(id, status, option, target, globalID)
	if err != nil {
		return err
	}
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if option&optionData != 0 {
		var size [2]byte
		binary.BigEndian.PutUint16(size[:], uint16(len(payload)))
		buffers := net.Buffers{meta, size[:], payload}
		_, err = buffers.WriteTo(w.conn)
		return err
	}
	_, err = w.conn.Write(meta)
	return err
}

// carrierFill continuously drains the physical TLS/VLESS conn into downlink so
// demux backpressure does not immediately stop socket Reads (xray DialingWorkerFactory).
func (w *worker) carrierFill() {
	buf := make([]byte, 32*1024)
	var cause error
	for {
		n, err := w.conn.Read(buf)
		if n > 0 {
			chunk := pool.Get(n)
			copy(chunk, buf[:n])
			if werr := w.downlink.write(chunk); werr != nil {
				_ = pool.Put(chunk)
				cause = werr
				break
			}
		}
		if err != nil {
			cause = err
			break
		}
	}
	w.downlink.close(cause)
}

func (w *worker) readLoop() {
	r := bufio.NewReaderSize(&pipeReader{p: w.downlink}, workerReadBuffer)
	for {
		id, status, option, err := readMetadata(r)
		if err != nil {
			w.close(err)
			return
		}
		var payload []byte
		if option&optionData != 0 {
			var size [2]byte
			if _, err = io.ReadFull(r, size[:]); err != nil {
				w.close(err)
				return
			}
			n := int(binary.BigEndian.Uint16(size[:]))
			payload = pool.Get(n)
			if _, err = io.ReadFull(r, payload); err != nil {
				_ = pool.Put(payload)
				w.close(err)
				return
			}
		}
		w.mu.Lock()
		s := w.sessions[id]
		w.mu.Unlock()
		switch status {
		case statusKeep, statusNew:
			if s == nil {
				if payload != nil {
					_ = pool.Put(payload)
				}
				_ = w.writeFrame(id, statusEnd, 0, nil, nil, nil)
				continue
			}
			if payload != nil {
				if len(payload) == 0 {
					_ = pool.Put(payload)
				} else {
					// Block when the per-session pipe is full (TCP backpressure).
					// CarrierFill keeps draining TLS into downlink meanwhile.
					if err := s.push(payload); err != nil {
						_ = pool.Put(payload)
					}
				}
			}
			if option&optionError != 0 {
				s.finishWithCause(false, false, protocolError("remote session", errors.New("remote reported an error")))
			}
		case statusEnd:
			if s == nil {
				if payload != nil {
					_ = pool.Put(payload)
				}
				continue
			}
			if payload != nil {
				if len(payload) == 0 {
					_ = pool.Put(payload)
				} else if err := s.push(payload); err != nil {
					_ = pool.Put(payload)
				}
			}
			var cause error
			if option&optionError != 0 {
				cause = protocolError("remote session", errors.New("remote reported an error"))
			}
			s.finishWithCause(false, false, cause)
		case statusKeepAlive:
			if payload != nil {
				_ = pool.Put(payload)
			}
		default:
			if payload != nil {
				_ = pool.Put(payload)
			}
			w.close(protocolError("decode metadata", fmt.Errorf("invalid status %d", status)))
			return
		}
	}
}

// pipeReader adapts sessionPipe to io.Reader for bufio demux.
type pipeReader struct {
	p *sessionPipe
}

func (r *pipeReader) Read(b []byte) (int, error) {
	return r.p.readDeadline(b, time.Time{})
}

type sessionPipe struct {
	mu     sync.Mutex
	cond   *sync.Cond
	chunks [][]byte
	nbytes int
	limit  int
	closed bool
	err    error
}

func newSessionPipe(limit int) *sessionPipe {
	p := &sessionPipe{limit: limit}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *sessionPipe) write(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.closed {
			return net.ErrClosed
		}
		if p.nbytes+len(data) <= p.limit {
			// data is already a pooled buffer owned by the pipe until read consumes it.
			p.chunks = append(p.chunks, data)
			p.nbytes += len(data)
			p.cond.Signal()
			return nil
		}
		// Apply backpressure: wait until the consumer drains enough space.
		p.cond.Wait()
	}
}

func (p *sessionPipe) readDeadline(dst []byte, deadline time.Time) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.chunks) == 0 && !p.closed {
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return 0, os.ErrDeadlineExceeded
			}
			timer := time.AfterFunc(remaining, func() {
				p.mu.Lock()
				p.cond.Broadcast()
				p.mu.Unlock()
			})
			p.cond.Wait()
			timer.Stop()
			if len(p.chunks) == 0 && !p.closed && !time.Now().Before(deadline) {
				return 0, os.ErrDeadlineExceeded
			}
			continue
		}
		p.cond.Wait()
	}
	if len(p.chunks) == 0 {
		if p.err != nil {
			return 0, p.err
		}
		return 0, io.EOF
	}
	return p.readLocked(dst), nil
}

// readAvailable copies any already-queued bytes without blocking.
func (p *sessionPipe) readAvailable(dst []byte) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.chunks) == 0 {
		return 0
	}
	return p.readLocked(dst)
}

func (p *sessionPipe) readLocked(dst []byte) int {
	chunk := p.chunks[0]
	n := copy(dst, chunk)
	if n == len(chunk) {
		p.chunks = p.chunks[1:]
		_ = pool.Put(chunk)
	} else {
		left := pool.Get(len(chunk) - n)
		copy(left, chunk[n:])
		_ = pool.Put(chunk)
		p.chunks[0] = left
	}
	p.nbytes -= n
	p.cond.Broadcast()
	return n
}

func (p *sessionPipe) close(cause error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.err = cause
	// Keep buffered chunks so Read can drain in-flight download data after End.
	// Discarding here truncated speedtest downloads when End arrived before the
	// consumer emptied the pipe.
	p.cond.Broadcast()
}

// drop releases any remaining pooled chunks. Only for hard worker teardown.
func (p *sessionPipe) drop(cause error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.err = cause
	for _, c := range p.chunks {
		_ = pool.Put(c)
	}
	p.chunks = nil
	p.nbytes = 0
	p.cond.Broadcast()
}

type session struct {
	id                uint16
	worker            *worker
	target            *C.Metadata
	globalID          [8]byte
	hasGlobalID       bool
	pipe              *sessionPipe
	done              chan struct{}
	once              sync.Once
	readMu            sync.Mutex
	newMu             sync.Mutex
	newSent           bool
	newClosed         bool
	firstPayloadTimer *time.Timer
	causeMu           sync.Mutex
	cause             error
	deadlineMu        sync.Mutex
	readDeadline      time.Time
	writeDeadline     time.Time
}

func (s *session) push(payload []byte) error {
	return s.pipe.write(payload)
}

func (s *session) start(firstPayloadTimeout time.Duration) error {
	if s.target != nil && s.target.NetWork == C.UDP {
		// An initial UDP frame must carry the first datagram and GlobalID.
		return nil
	}
	if firstPayloadTimeout <= 0 {
		_, err := s.writeNew(nil)
		return err
	}
	s.newMu.Lock()
	s.firstPayloadTimer = time.AfterFunc(firstPayloadTimeout, func() {
		if _, err := s.writeNew(nil); err != nil {
			s.worker.close(err)
		}
	})
	s.newMu.Unlock()
	return nil
}

// writeNew sends exactly one New frame. The returned bool reports whether the
// payload was consumed by that frame rather than needing a Keep frame.
func (s *session) writeNew(payload []byte) (bool, error) {
	s.newMu.Lock()
	defer s.newMu.Unlock()
	if s.newClosed {
		return false, s.closedWriteError()
	}
	if s.newSent {
		return false, nil
	}
	select {
	case <-s.done:
		return false, s.closedWriteError()
	default:
	}
	if s.firstPayloadTimer != nil {
		s.firstPayloadTimer.Stop()
		s.firstPayloadTimer = nil
	}
	s.newSent = true
	option := byte(0)
	if len(payload) > 0 {
		option = optionData
	}
	var globalID *[8]byte
	if s.hasGlobalID {
		globalID = &s.globalID
	}
	if err := s.worker.writeFrame(s.id, statusNew, option, s.target, globalID, payload); err != nil {
		return true, err
	}
	return true, nil
}

func (s *session) stopFirstPayloadTimer() {
	s.newMu.Lock()
	s.newClosed = true
	if s.firstPayloadTimer != nil {
		s.firstPayloadTimer.Stop()
		s.firstPayloadTimer = nil
	}
	s.newMu.Unlock()
}

func (s *session) finish(sendEnd, withError bool) {
	s.finishWithCause(sendEnd, withError, nil)
}

func (s *session) finishWithCause(sendEnd, withError bool, cause error) {
	s.once.Do(func() {
		s.stopFirstPayloadTimer()
		if sendEnd && !s.worker.isClosed() {
			opt := byte(0)
			if withError {
				opt = optionError
			}
			_ = s.worker.writeFrame(s.id, statusEnd, opt, nil, nil, nil)
		}
		s.setTerminalCause(cause)
		s.pipe.close(cause)
		close(s.done)
		s.worker.removeSession(s.id)
	})
}

func (s *session) setTerminalCause(cause error) {
	s.causeMu.Lock()
	s.cause = cause
	s.causeMu.Unlock()
}

func (s *session) terminalCause() error {
	s.causeMu.Lock()
	defer s.causeMu.Unlock()
	return s.cause
}

func (s *session) closedWriteError() error {
	if cause := s.terminalCause(); cause != nil {
		return cause
	}
	return net.ErrClosed
}

func (s *session) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s.readMu.Lock()
	defer s.readMu.Unlock()
	n, err := s.pipe.readDeadline(p, s.getReadDeadline())
	if n == 0 {
		return 0, err
	}
	// Drain further queued chunks into the caller's buffer without blocking
	// (closer to xray MultiBuffer session reads).
	for n < len(p) {
		m := s.pipe.readAvailable(p[n:])
		if m == 0 {
			break
		}
		n += m
	}
	return n, nil
}

func (s *session) Write(p []byte) (int, error) {
	select {
	case <-s.done:
		return 0, s.closedWriteError()
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
		consumed, err := s.writeNew(p[:n])
		if err != nil {
			s.worker.close(err)
			return written, err
		}
		if consumed {
			written += n
			p = p[n:]
			continue
		}
		// UDP Keep frames must repeat the destination address (Mux.Cool).
		var keepTarget *C.Metadata
		if s.target != nil && s.target.NetWork == C.UDP {
			keepTarget = s.target
		}
		if err := s.worker.writeFrame(s.id, statusKeep, optionData, keepTarget, nil, p[:n]); err != nil {
			s.worker.close(err)
			return written, err
		}
		written += n
		p = p[n:]
	}
	return written, nil
}

func (s *session) Close() error         { s.finish(true, false); return nil }
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

func encodeMetadata(id uint16, status, option byte, target *C.Metadata, globalID *[8]byte) ([]byte, error) {
	switch status {
	case statusNew, statusKeep, statusEnd, statusKeepAlive:
	default:
		return nil, protocolError("encode metadata", fmt.Errorf("invalid status %d", status))
	}
	if option & ^byte(optionData|optionError) != 0 {
		return nil, protocolError("encode metadata", fmt.Errorf("invalid option %d", option))
	}
	if globalID != nil && *globalID != [8]byte{} && (status != statusNew || target == nil || target.NetWork != C.UDP || option&optionData == 0) {
		return nil, protocolError("encode metadata", errors.New("GlobalID is only valid on an initial UDP data frame"))
	}
	body := make([]byte, 4, 64)
	binary.BigEndian.PutUint16(body[:2], id)
	body[2] = status
	body[3] = option
	needAddr := status == statusNew || (status == statusKeep && target != nil && target.NetWork == C.UDP)
	if needAddr {
		if target == nil {
			return nil, errors.New("xray mux: missing target for frame")
		}
		switch target.NetWork {
		case C.TCP:
			body = append(body, targetTCP)
		case C.UDP:
			body = append(body, targetUDP)
		default:
			return nil, errors.New("xray mux: unsupported network type")
		}
		var port [2]byte
		binary.BigEndian.PutUint16(port[:], target.DstPort)
		body = append(body, port[:]...)
		var err error
		body, err = appendAddress(body, target)
		if err != nil {
			return nil, err
		}
		if status == statusNew && target.NetWork == C.UDP {
			var gid [8]byte
			if globalID != nil {
				gid = *globalID
			}
			body = append(body, gid[:]...)
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
		err = protocolError("read metadata length", err)
		return
	}
	n := int(binary.BigEndian.Uint16(size[:]))
	if n < 4 || n > maxMetadataSize {
		err = protocolError("read metadata", fmt.Errorf("invalid metadata length %d", n))
		return
	}
	meta := make([]byte, n)
	if _, err = io.ReadFull(r, meta); err != nil {
		err = protocolError("read metadata", err)
		return
	}
	id = binary.BigEndian.Uint16(meta[:2])
	status = meta[2]
	option = meta[3]
	switch status {
	case statusNew:
		if err = validateTargetMetadata(meta[4:], false); err != nil {
			err = protocolError("decode target", err)
			return
		}
	case statusKeep:
		if len(meta) > 4 {
			if err = validateTargetMetadata(meta[4:], true); err != nil {
				err = protocolError("decode target", err)
				return
			}
		}
	case statusEnd, statusKeepAlive:
		if len(meta) != 4 {
			err = protocolError("decode metadata", fmt.Errorf("unexpected trailing metadata: %d bytes", len(meta)-4))
			return
		}
	default:
		err = protocolError("decode metadata", fmt.Errorf("invalid status %d", status))
		return
	}
	if option & ^byte(optionData|optionError) != 0 {
		err = protocolError("decode metadata", fmt.Errorf("invalid option %d", option))
	}
	return
}

func validateTargetMetadata(raw []byte, keep bool) error {
	if len(raw) < 4 {
		return io.ErrUnexpectedEOF
	}
	network := raw[0]
	if network != targetTCP && network != targetUDP {
		return fmt.Errorf("invalid network %d", network)
	}
	if keep && network != targetUDP {
		return errors.New("follow-up target is only valid for UDP")
	}
	address := raw[3:]
	consumed := 0
	switch address[0] {
	case 0x01:
		consumed = 1 + net.IPv4len
	case 0x02:
		if len(address) < 2 || address[1] == 0 {
			return io.ErrUnexpectedEOF
		}
		consumed = 2 + int(address[1])
	case 0x03:
		consumed = 1 + net.IPv6len
	default:
		return fmt.Errorf("invalid address type %d", address[0])
	}
	if len(address) < consumed {
		return io.ErrUnexpectedEOF
	}
	trailing := len(address) - consumed
	if !keep && network == targetUDP {
		if trailing != 8 {
			return fmt.Errorf("invalid GlobalID length %d", trailing)
		}
	} else if trailing != 0 {
		return fmt.Errorf("unexpected trailing metadata: %d bytes", trailing)
	}
	return nil
}
