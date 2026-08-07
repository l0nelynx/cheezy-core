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
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/pool"
	C "github.com/metacubex/mihomo/constant"

	"golang.org/x/sync/singleflight"
)

const (
	statusNew       byte = 0x01
	statusKeep      byte = 0x02
	statusEnd       byte = 0x03
	statusKeepAlive byte = 0x04
	optionData      byte = 0x01
	optionError     byte = 0x02
	targetTCP       byte = 0x01
	targetUDP       byte = 0x02
	maxFramePayload      = 8 * 1024
	maxMetadataSize      = 512
	defaultMaxUses       = 128
	defaultConcurrency   = 8
	// session soft pipe follows xray-core policy.Buffer.PerConnection (512KiB).
	// session-max defaults to the same size so overflow is opt-in via YAML —
	// large default overflow+carrier (2MiB/4MiB) caused LTE bufferbloat and
	// collapsed multi-stream downloads to ~1–2Mbps on a single carrier.
	defaultSessionBuffer    = 512 * 1024
	defaultSessionMaxBuffer = 512 * 1024
	defaultCarrierBuffer    = 1024 * 1024
	defaultWorkerReadBuffer = 64 * 1024
	hardConcurrencyMult     = 4
	workerIdleTime          = 16 * time.Second
)

var (
	errMaxConnections = errors.New("xray mux: max connections reached")
)

type physicalDialer func(context.Context) (net.Conn, error)
type endpointKeyer func(context.Context) string

// Options configures a Mux.Cool client pool.
type Options struct {
	Concurrency    int // soft streams/worker; 0 -> 8
	MaxConnections int // physical carriers; 0 -> unlimited
	MaxWorkerUses  int // retire worker after N sessions; 0 -> 128

	// Demux isolation buffers (bytes). 0 -> package defaults.
	SessionBuffer    int // soft per-session pipe
	SessionMaxBuffer int // hard per-session (pipe + overflow)
	CarrierBuffer    int // hard sum of buffered bytes on one worker
	WorkerReadBuffer int // bufio size on demux reader
}

// bufferConfig is the resolved downlink buffering policy for a pool/worker.
type bufferConfig struct {
	sessionBuffer    int
	sessionMaxBuffer int
	carrierBuffer    int
	workerReadBuffer int
}

// resolveBufferConfig applies defaults and clamps invariants:
// sessionMax >= sessionBuffer, carrier >= sessionMax.
func resolveBufferConfig(opts Options) bufferConfig {
	sessionBuf := opts.SessionBuffer
	if sessionBuf <= 0 {
		sessionBuf = defaultSessionBuffer
	}
	sessionMax := opts.SessionMaxBuffer
	if sessionMax <= 0 {
		sessionMax = defaultSessionMaxBuffer
	}
	if sessionMax < sessionBuf {
		sessionMax = sessionBuf
	}
	carrier := opts.CarrierBuffer
	if carrier <= 0 {
		carrier = defaultCarrierBuffer
	}
	if carrier < sessionMax {
		carrier = sessionMax
	}
	workerRead := opts.WorkerReadBuffer
	if workerRead <= 0 {
		workerRead = defaultWorkerReadBuffer
	}
	return bufferConfig{
		sessionBuffer:    sessionBuf,
		sessionMaxBuffer: sessionMax,
		carrierBuffer:    carrier,
		workerReadBuffer: workerRead,
	}
}

// Pool multiplexes logical TCP connections over Xray Mux.Cool workers.
type Pool struct {
	mu              sync.Mutex
	workers         []*worker
	concurrency     int
	hardConcurrency int
	maxConnections  int
	maxWorkerUses   int
	buf             bufferConfig
	dial            physicalDialer
	endpointKey     endpointKeyer
	closed          bool
	dialGroup       singleflight.Group
	free            *sync.Cond
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
	p := &Pool{
		concurrency:     concurrency,
		hardConcurrency: hard,
		maxConnections:  opts.MaxConnections,
		maxWorkerUses:   maxUses,
		buf:             resolveBufferConfig(opts),
		dial:            dial,
		endpointKey:     endpointKey,
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

		// 1) Spread-first when max-connections is capped: open a new REALITY
		// carrier before packing soft concurrency. One TCP shared by many bulk
		// downloads collapses on LTE (single cwnd + TCP HoL); a few carriers
		// keep handshake count low while restoring throughput.
		if p.underMaxConnections() {
			w, err := p.dialWorker(ctx, true)
			if err == nil {
				s := w.reserve(p.concurrency)
				if s == nil {
					continue
				}
				if err := w.openSession(s, metadata, globalID); err != nil {
					return nil, err
				}
				return s, nil
			}
			if !errors.Is(err, errMaxConnections) {
				return nil, err
			}
			// Lost the race to the last permit — fall through to packing.
		}

		// 2) Soft-allocate onto an existing worker (brief lock, no I/O).
		if s := p.tryReserve(p.concurrency); s != nil {
			if err := s.worker.openSession(s, metadata, globalID); err != nil {
				return nil, err
			}
			return s, nil
		}

		// 3) Dial a new physical carrier (unlimited max-connections, or soft full).
		w, err := p.dialWorker(ctx, false)
		if err == nil {
			s := w.reserve(p.concurrency)
			if s == nil {
				// Another waiter may have filled it; try again.
				continue
			}
			if err := w.openSession(s, metadata, globalID); err != nil {
				return nil, err
			}
			return s, nil
		}
		if !errors.Is(err, errMaxConnections) {
			return nil, err
		}

		// 4) At physical limit: pack onto existing carriers up to hard concurrency.
		if s := p.tryReserve(p.hardConcurrency); s != nil {
			if err := s.worker.openSession(s, metadata, globalID); err != nil {
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

// underMaxConnections reports whether another physical carrier may be dialed
// under a configured max-connections cap. Unlimited (0) uses pack-first instead.
func (p *Pool) underMaxConnections() bool {
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
	// Prefer the least-loaded worker so streams spread across existing carriers.
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

func (p *Pool) dialWorker(ctx context.Context, forceNew bool) (*worker, error) {
	key := "_"
	if p.endpointKey != nil {
		if k := p.endpointKey(ctx); k != "" {
			key = k
		}
	}

	if forceNew {
		// Spread-first must not collapse concurrent dials via singleflight —
		// otherwise speedtest's parallel opens all land on one carrier.
		return p.dialWorkerOnce(ctx, key, true)
	}

	v, err, _ := p.dialGroup.Do(key, func() (any, error) {
		return p.dialWorkerOnce(ctx, key, false)
	})
	if err != nil {
		return nil, err
	}
	return v.(*worker), nil
}

func (p *Pool) dialWorkerOnce(ctx context.Context, key string, forceNew bool) (*worker, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, net.ErrClosed
	}
	p.pruneLocked()
	if !forceNew {
		for _, w := range p.workers {
			if !w.isClosed() && w.hasCapacity(p.concurrency) {
				p.mu.Unlock()
				return w, nil
			}
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
	p.mu.Unlock()

	physical, err := p.dial(ctx)
	if err != nil {
		if permitKey != "" {
			globalPermits.release(permitKey)
		}
		return nil, err
	}

	release := func() {
		if permitKey != "" {
			globalPermits.release(permitKey)
		}
		p.signalFree()
	}
	w := newWorker(physical, p.maxWorkerUses, release, p.buf)
	w.onFree = p.signalFree

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		w.close()
		return nil, net.ErrClosed
	}
	p.workers = append(p.workers, w)
	p.mu.Unlock()
	return w, nil
}

func (p *Pool) waitForSlot(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	stop := context.AfterFunc(ctx, func() {
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
	mu           sync.Mutex
	writeMu      sync.Mutex
	bufMu        sync.Mutex
	bufCond      *sync.Cond
	conn         net.Conn
	sessions     map[uint16]*session
	maxUses      int
	total        uint16
	closed       bool
	shuttingDown atomic.Bool
	idleTimer    *time.Timer
	release      func()
	closeOnce    sync.Once
	onFree       func()
	buf          bufferConfig
	buffered     int // pipe + overflow bytes across all sessions
}

func newWorker(conn net.Conn, maxUses int, release func(), buf bufferConfig) *worker {
	w := &worker{
		conn:     conn,
		sessions: make(map[uint16]*session),
		maxUses:  maxUses,
		release:  release,
		buf:      buf,
	}
	w.bufCond = sync.NewCond(&w.bufMu)
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
		pipe:   newSessionPipe(w.buf.sessionBuffer),
		done:   make(chan struct{}),
	}
	w.sessions[s.id] = s
	return s
}

// openSession sends the Mux.Cool New frame outside any worker/pool lock.
func (w *worker) openSession(s *session, metadata *C.Metadata, globalID *[8]byte) error {
	s.target = metadata
	if err := w.writeFrame(s.id, statusNew, 0, metadata, globalID, nil); err != nil {
		w.abandonSession(s)
		go w.close()
		return err
	}
	return nil
}

func (w *worker) abandonSession(s *session) {
	w.mu.Lock()
	delete(w.sessions, s.id)
	w.mu.Unlock()
	s.once.Do(func() {
		s.dropBuffers()
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
		w.idleTimer = time.AfterFunc(workerIdleTime, w.close)
	}
	w.mu.Unlock()
	if w.onFree != nil {
		w.onFree()
	}
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
		w.shuttingDown.Store(true)
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

		w.bufMu.Lock()
		w.bufCond.Broadcast()
		w.bufMu.Unlock()

		_ = w.conn.Close()
		for _, s := range sessions {
			s.dropBuffers()
			s.finish(false, false)
		}
		if w.release != nil {
			w.release()
		}
	})
}

func (w *worker) writeFrame(id uint16, status, option byte, target *C.Metadata, globalID *[8]byte, payload []byte) error {
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

func (w *worker) readLoop() {
	defer w.close()
	r := bufio.NewReaderSize(w.conn, w.buf.workerReadBuffer)
	for {
		id, status, option, err := readMetadata(r)
		if err != nil {
			return
		}
		var payload []byte
		if option&optionData != 0 {
			var size [2]byte
			if _, err = io.ReadFull(r, size[:]); err != nil {
				return
			}
			n := int(binary.BigEndian.Uint16(size[:]))
			payload = pool.Get(n)
			if _, err = io.ReadFull(r, payload); err != nil {
				_ = pool.Put(payload)
				return
			}
		}
		w.mu.Lock()
		s := w.sessions[id]
		w.mu.Unlock()
		switch status {
		case statusKeep:
			if s == nil {
				if payload != nil {
					_ = pool.Put(payload)
				}
				_ = w.writeFrame(id, statusEnd, 0, nil, nil, nil)
				continue
			}
			if len(payload) > 0 {
				// Admit under session-max / carrier-buffer. Do not tear the
				// session down on full pipe — that collapsed bulk downloads.
				if err := s.admit(payload); err != nil {
					_ = pool.Put(payload)
				}
			}
		case statusEnd:
			if payload != nil {
				_ = pool.Put(payload)
			}
			if s != nil {
				s.finish(false, option&optionError != 0)
			}
		case statusKeepAlive, statusNew:
			if payload != nil {
				_ = pool.Put(payload)
			}
			// Xray clients discard unsolicited heartbeat/reverse data.
		default:
			if payload != nil {
				_ = pool.Put(payload)
			}
			return
		}
	}
}

func (w *worker) releaseBuffered(n int) {
	if n <= 0 {
		return
	}
	w.bufMu.Lock()
	w.buffered -= n
	if w.buffered < 0 {
		w.buffered = 0
	}
	w.bufCond.Broadcast()
	w.bufMu.Unlock()
}

type sessionPipe struct {
	mu     sync.Mutex
	cond   *sync.Cond
	chunks [][]byte
	nbytes int
	limit  int
	closed bool
}

func newSessionPipe(limit int) *sessionPipe {
	p := &sessionPipe{limit: limit}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *sessionPipe) tryWrite(data []byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Soft-close still accepts enqueue so overflow can drain in order after End.
	if p.nbytes+len(data) <= p.limit {
		p.chunks = append(p.chunks, data)
		p.nbytes += len(data)
		p.cond.Signal()
		return true
	}
	return false
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
		return 0, io.EOF
	}
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
	return n, nil
}

func (p *sessionPipe) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	// Keep buffered chunks so Read can drain in-flight download data after End.
	p.cond.Broadcast()
}

// drop releases remaining pooled chunks and returns how many bytes were held.
func (p *sessionPipe) drop() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	n := p.nbytes
	for _, c := range p.chunks {
		_ = pool.Put(c)
	}
	p.chunks = nil
	p.nbytes = 0
	p.cond.Broadcast()
	return n
}

type session struct {
	id            uint16
	worker        *worker
	target        *C.Metadata
	pipe          *sessionPipe
	overflow      [][]byte
	overflowBytes int
	done          chan struct{}
	once          sync.Once
	readMu        sync.Mutex
	deadlineMu    sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

// admit parks payload into the session pipe or overflow without blocking the
// demux on a single full soft pipe. Blocks only when session-max or
// carrier-buffer is exhausted (TCP backpressure).
func (s *session) admit(payload []byte) error {
	n := len(payload)
	if n == 0 {
		return nil
	}
	w := s.worker
	w.bufMu.Lock()
	defer w.bufMu.Unlock()
	for {
		select {
		case <-s.done:
			return net.ErrClosed
		default:
		}
		if w.shuttingDown.Load() {
			return net.ErrClosed
		}
		s.pipe.mu.Lock()
		sessionTotal := s.pipe.nbytes + s.overflowBytes
		s.pipe.mu.Unlock()
		if sessionTotal+n <= w.buf.sessionMaxBuffer && w.buffered+n <= w.buf.carrierBuffer {
			w.buffered += n
			if !s.pipe.tryWrite(payload) {
				s.overflow = append(s.overflow, payload)
				s.overflowBytes += n
			}
			return nil
		}
		w.bufCond.Wait()
	}
}

func (s *session) flushOverflowToPipe() {
	w := s.worker
	w.bufMu.Lock()
	defer w.bufMu.Unlock()
	for len(s.overflow) > 0 {
		chunk := s.overflow[0]
		if !s.pipe.tryWrite(chunk) {
			break
		}
		s.overflow = s.overflow[1:]
		s.overflowBytes -= len(chunk)
	}
	w.bufCond.Broadcast()
}

func (s *session) dropBuffers() {
	w := s.worker
	w.bufMu.Lock()
	released := s.pipe.drop()
	for _, c := range s.overflow {
		released += len(c)
		_ = pool.Put(c)
	}
	s.overflow = nil
	s.overflowBytes = 0
	w.buffered -= released
	if w.buffered < 0 {
		w.buffered = 0
	}
	w.bufCond.Broadcast()
	w.bufMu.Unlock()
}

func (s *session) finish(sendEnd, withError bool) {
	s.once.Do(func() {
		if sendEnd && !s.worker.isClosed() {
			opt := byte(0)
			if withError {
				opt = optionError
			}
			_ = s.worker.writeFrame(s.id, statusEnd, opt, nil, nil, nil)
		}
		s.pipe.close()
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
	deadline := s.getReadDeadline()
	for {
		s.flushOverflowToPipe()
		n, err := s.pipe.readDeadline(p, deadline)
		if n > 0 {
			s.worker.releaseBuffered(n)
			s.flushOverflowToPipe()
			return n, nil
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		s.worker.bufMu.Lock()
		hasOverflow := len(s.overflow) > 0
		s.worker.bufMu.Unlock()
		if hasOverflow {
			continue
		}
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		// Empty pipe, not closed, no overflow — readDeadline should have waited.
		return 0, err
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
		// UDP Keep frames must repeat the destination address (Mux.Cool).
		var keepTarget *C.Metadata
		if s.target != nil && s.target.NetWork == C.UDP {
			keepTarget = s.target
		}
		if err := s.worker.writeFrame(s.id, statusKeep, optionData, keepTarget, nil, p[:n]); err != nil {
			s.worker.close()
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
