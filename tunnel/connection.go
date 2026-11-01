package tunnel

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

type queuedUDPPacket struct {
	packet      C.PacketAdapter
	queuedAt    time.Time
	diagnostics *statistic.UDPDiagnostics
}

func (p queuedUDPPacket) drop(reason statistic.UDPDropReason) {
	p.diagnostics.Drop(reason)
	p.packet.Drop()
}

func (p queuedUDPPacket) expired(maxDelay time.Duration) bool {
	return maxDelay > 0 && !p.queuedAt.IsZero() && time.Since(p.queuedAt) >= maxDelay
}

type packetSender struct {
	ctx           context.Context
	cancel        context.CancelFunc
	ch            chan queuedUDPPacket
	queueMutex    sync.Mutex // serializes admission with Close, never held during I/O
	closed        bool
	maxQueueDelay atomic.Int64 // -1 until the outbound has been established

	// destination NAT mapping
	originToTarget map[string]netip.Addr
	targetToOrigin map[netip.Addr]netip.Addr
	mappingMutex   sync.RWMutex
}

// newPacketSender return a chan based C.PacketSender
// It ensures that packets can be sent sequentially and without blocking
func newPacketSender() C.PacketSender {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan queuedUDPPacket, senderCapacity)
	sender := &packetSender{
		ctx:    ctx,
		cancel: cancel,
		ch:     ch,

		originToTarget: make(map[string]netip.Addr),
		targetToOrigin: make(map[netip.Addr]netip.Addr),
	}
	sender.maxQueueDelay.Store(-1)
	return sender
}

func (s *packetSender) AddMapping(originMetadata *C.Metadata, metadata *C.Metadata) {
	s.mappingMutex.Lock()
	defer s.mappingMutex.Unlock()
	originKey := originMetadata.String()
	originAddr := originMetadata.DstIP
	targetAddr := metadata.DstIP
	if addr := s.originToTarget[originKey]; !addr.IsValid() { // overwrite only if the record is illegal
		s.originToTarget[originKey] = targetAddr
	}
	if addr := s.targetToOrigin[targetAddr]; !addr.IsValid() { // overwrite only if the record is illegal
		s.targetToOrigin[targetAddr] = originAddr
	}
}

func (s *packetSender) RestoreReadFrom(addr netip.Addr) netip.Addr {
	s.mappingMutex.RLock()
	defer s.mappingMutex.RUnlock()
	if originAddr := s.targetToOrigin[addr]; originAddr.IsValid() {
		return originAddr
	}
	return addr
}

func (s *packetSender) processPacket(pc C.PacketConn, proxy C.WriteBackProxy, queued queuedUDPPacket, maxDelay time.Duration) {
	packet := queued.packet
	defer packet.Drop()
	if queued.diagnostics != nil && !queued.queuedAt.IsZero() {
		queued.diagnostics.ObserveSenderQueue(time.Since(queued.queuedAt))
	}
	if s.ctx.Err() != nil {
		queued.diagnostics.Drop(statistic.UDPSenderClosed)
		return
	}
	if queued.expired(maxDelay) {
		queued.diagnostics.Drop(statistic.UDPQueueExpired)
		return
	}
	metadata := packet.Metadata()

	var addr *net.UDPAddr

	s.mappingMutex.RLock()
	targetAddr := s.originToTarget[metadata.String()]
	s.mappingMutex.RUnlock()

	if targetAddr.IsValid() {
		addr = net.UDPAddrFromAddrPort(netip.AddrPortFrom(targetAddr, metadata.DstPort))
	}

	if addr == nil {
		originMetadata := metadata  // save origin metadata
		metadata = metadata.Clone() // don't modify PacketAdapter's metadata

		_ = preHandleMetadata(metadata) // error was pre-checked
		metadata = metadata.Pure()
		if metadata.Host != "" {
			// TODO: ResolveUDP may take a long time to block the Process loop
			//       but we want keep sequence sending so can't open a new goroutine
			if err := pc.ResolveUDP(s.ctx, metadata); err != nil {
				queued.diagnostics.Drop(statistic.UDPResolveError)
				log.Warnln("[UDP] Resolve Ip error: %s", err)
				return
			}
		}

		if !metadata.DstIP.IsValid() {
			queued.diagnostics.Drop(statistic.UDPResolveError)
			log.Warnln("[UDP] Destination ip not valid: %#v", metadata)
			return
		}
		s.AddMapping(originMetadata, metadata)
		addr = metadata.UDPAddr()
	}
	// Resolution can block after dequeueing. Do not send a packet that has
	// expired in the meantime. The transport's WriteTo itself is not cancellable.
	if s.ctx.Err() != nil {
		queued.diagnostics.Drop(statistic.UDPSenderClosed)
		return
	}
	if queued.expired(maxDelay) {
		queued.diagnostics.Drop(statistic.UDPQueueExpired)
		return
	}
	if proxy != nil {
		proxy.UpdateWriteBack(packet)
	}
	var started time.Time
	if queued.diagnostics != nil {
		started = time.Now()
	}
	err := handleUDPToRemote(packet, pc, addr)
	if queued.diagnostics != nil {
		queued.diagnostics.ObserveOutboundWrite(time.Since(started))
		if err != nil {
			queued.diagnostics.Drop(statistic.UDPWriteError)
		}
	}
}

func (s *packetSender) Process(pc C.PacketConn, proxy C.WriteBackProxy) {
	var maxDelay time.Duration
	if policy, ok := N.FindUpstream[C.UDPQueuePolicy](pc, func(policy C.UDPQueuePolicy) bool {
		return policy.UDPMaxQueueDelay() > 0
	}); ok {
		maxDelay = policy.UDPMaxQueueDelay()
	}
	s.maxQueueDelay.Store(int64(maxDelay))
	for {
		select {
		case <-s.ctx.Done():
			return // sender closed
		case packet := <-s.ch:
			s.processPacket(pc, proxy, packet, maxDelay)
		}
	}
}

func (s *packetSender) dropAll() {
	for {
		select {
		case data := <-s.ch:
			data.drop(statistic.UDPSenderClosed)
		default:
			return // no data, exit goroutine
		}
	}
}

func (s *packetSender) Send(packet C.PacketAdapter) {
	queued := queuedUDPPacket{packet: packet, diagnostics: statistic.ActiveUDPDiagnostics()}
	s.queueMutex.Lock()
	if s.closed {
		s.queueMutex.Unlock()
		queued.drop(statistic.UDPSenderClosed)
		return
	}
	// Preserve timestamps during connection setup: the eventual outbound may
	// opt into expiry. Established sessions without either feature skip clocks.
	if s.maxQueueDelay.Load() != 0 || queued.diagnostics != nil {
		queued.queuedAt = time.Now()
	}
	select {
	case s.ch <- queued:
		s.queueMutex.Unlock()
	default:
		s.queueMutex.Unlock()
		queued.drop(statistic.UDPSenderQueueFull)
	}
}

func (s *packetSender) Close() {
	s.queueMutex.Lock()
	if s.closed {
		s.queueMutex.Unlock()
		return
	}
	s.closed = true
	s.cancel()
	s.queueMutex.Unlock()
	s.dropAll()
}

func (s *packetSender) DoSniff(metadata *C.Metadata) error { return nil }

func handleUDPToRemote(packet C.UDPPacket, pc C.PacketConn, addr *net.UDPAddr) error {
	if addr == nil {
		return errors.New("udp addr invalid")
	}

	if _, err := pc.WriteTo(packet.Data(), addr); err != nil {
		return err
	}
	// reset timeout
	_ = pc.SetReadDeadline(time.Now().Add(udpTimeout))

	return nil
}

func handleUDPToLocal(writeBack C.WriteBack, pc C.PacketConn, sender C.PacketSender, key string, oAddrPort netip.AddrPort) {
	defer func() {
		sender.Close()
		_ = pc.Close()
		closeAllLocalCoon(key)
		natTable.Delete(key)
	}()

	for {
		_ = pc.SetReadDeadline(time.Now().Add(udpTimeout))
		data, put, from, err := pc.WaitReadFrom()
		if err != nil {
			return
		}

		fromUDPAddr, isUDPAddr := from.(*net.UDPAddr)
		if !isUDPAddr {
			fromUDPAddr = net.UDPAddrFromAddrPort(oAddrPort) // oAddrPort was Unmapped
			log.Warnln("server return a [%T](%s) which isn't a *net.UDPAddr, force replace to (%s), this may be caused by a wrongly implemented server", from, from, oAddrPort)
		} else if fromUDPAddr == nil {
			fromUDPAddr = net.UDPAddrFromAddrPort(oAddrPort) // oAddrPort was Unmapped
			log.Warnln("server return a nil *net.UDPAddr, force replace to (%s), this may be caused by a wrongly implemented server", oAddrPort)
		}

		fromAddrPort := fromUDPAddr.AddrPort()
		fromAddr := fromAddrPort.Addr().Unmap()

		// restore DestinationNAT
		fromAddr = sender.RestoreReadFrom(fromAddr).Unmap()

		fromAddrPort = netip.AddrPortFrom(fromAddr, fromAddrPort.Port())

		_, err = writeBack.WriteBack(data, net.UDPAddrFromAddrPort(fromAddrPort))
		if put != nil {
			put()
		}
		if err != nil {
			statistic.ActiveUDPDiagnostics().Drop(statistic.UDPWriteBackError)
			return
		}
	}
}

func closeAllLocalCoon(lAddr string) {
	natTable.RangeForLocalConn(lAddr, func(key string, value *net.UDPConn) bool {
		conn := value

		conn.Close()
		log.Debugln("Closing TProxy local conn... lAddr=%s rAddr=%s", lAddr, key)
		return true
	})
}

func handleSocket(inbound, outbound net.Conn) {
	N.Relay(inbound, outbound)
}
