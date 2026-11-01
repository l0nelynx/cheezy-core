package dns

import (
	"github.com/metacubex/mihomo/component/powersave"

	"github.com/metacubex/quic-go"
)

// quietQUICConfig drops the keep-alive of a DNS-over-QUIC/HTTP3 connection
// made while the device is not in use (component/powersave): its ping every
// 20 seconds kept the connection, and the radio, up for as long as the screen
// stayed off, with nothing to resolve. Without it the connection closes after
// its idle timeout and the next query opens one again (0-RTT with the token
// store). Existing idle QUIC connections are closed by tunnel/powersave.go.
func quietQUICConfig(cfg *quic.Config) *quic.Config {
	if cfg == nil || cfg.KeepAlivePeriod == 0 || powersave.Current() == powersave.Active {
		return cfg
	}
	cfg = cfg.Clone()
	cfg.KeepAlivePeriod = 0
	return cfg
}

func (doh *dnsOverHTTPS) CloseIdleQUICConnections() {
	// Hold this through the whole exchange, including reading the body:
	// http3.Transport's own idle accounting ends after response headers.
	if !doh.activity.TryLock() {
		return
	}
	defer doh.activity.Unlock()
	doh.clientMu.Lock()
	defer doh.clientMu.Unlock()
	if doh.client != nil && isHTTP3(doh.client) {
		doh.client.CloseIdleConnections()
	}
}

func closeIdleQUICClients(clients []dnsClient) {
	for _, client := range clients {
		if closer, ok := client.(interface{ CloseIdleQUICConnections() }); ok {
			closer.CloseIdleQUICConnections()
		}
	}
}

func (r *Resolver) CloseIdleQUICConnections() {
	if r == nil {
		return
	}
	closeIdleQUICClients(r.main)
	closeIdleQUICClients(r.fallback)
	for _, policy := range r.policy {
		policy.closeIdleQUICConnections()
	}
	r.defaultResolver.CloseIdleQUICConnections()
}

func (rs Resolvers) CloseIdleQUICConnections() {
	rs.Resolver.CloseIdleQUICConnections()
	rs.ProxyResolver.CloseIdleQUICConnections()
	rs.DirectResolver.CloseIdleQUICConnections()
}
