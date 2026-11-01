package dns

import (
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/powersave"
	"github.com/metacubex/mihomo/component/trie"
	"github.com/metacubex/mihomo/constant/features"

	"github.com/metacubex/http"
	"github.com/metacubex/quic-go"
)

func TestQuietQUICConfig(t *testing.T) {
	defer powersave.Set(powersave.Active)
	cfg := &quic.Config{KeepAlivePeriod: 20 * time.Second, TokenStore: newQUICTokenStore()}
	for _, mode := range []powersave.Mode{powersave.Active, powersave.Background, powersave.Idle} {
		powersave.Set(mode)
		quiet := quietQUICConfig(cfg)
		if features.WithPowerSave && mode != powersave.Active {
			if quiet.KeepAlivePeriod != 0 || quiet == cfg {
				t.Fatalf("%s must disable keep-alive on a clone", mode)
			}
		} else if quiet != cfg {
			t.Fatalf("%s unexpectedly changed the config", mode)
		}
		if cfg.KeepAlivePeriod != 20*time.Second || quiet.TokenStore != cfg.TokenStore {
			t.Fatal("the original config and token store must be preserved")
		}
	}
}

type powerSaveTestDNSClient struct {
	dnsClient
	closed int
}

func (c *powerSaveTestDNSClient) CloseIdleQUICConnections() { c.closed++ }

func TestPowerSaveVisitsAllDNSResolvers(t *testing.T) {
	clients := make([]*powerSaveTestDNSClient, 6)
	for i := range clients {
		clients[i] = &powerSaveTestDNSClient{}
	}
	policyTrie := trie.New[[]dnsClient]()
	if err := policyTrie.Insert("example.com", []dnsClient{clients[3]}); err != nil {
		t.Fatal(err)
	}
	rs := Resolvers{
		Resolver: &Resolver{
			main: []dnsClient{clients[0]}, fallback: []dnsClient{clients[1]},
			defaultResolver: &Resolver{main: []dnsClient{clients[2]}},
			policy:          []dnsPolicy{domainTriePolicy{policyTrie}, domainMatcherPolicy{dnsClients: []dnsClient{clients[4]}}},
		},
		ProxyResolver: &Resolver{main: []dnsClient{clients[5]}},
	}
	rs.CloseIdleQUICConnections()
	for i, client := range clients {
		if client.closed != 1 {
			t.Fatalf("DNS client %d closed %d times, want 1", i, client.closed)
		}
	}
}

type powerSaveHTTPTransport struct {
	http.RoundTripper
	closed int
}

func (t *powerSaveHTTPTransport) CloseIdleConnections() { t.closed++ }

func TestPowerSaveLeavesHTTPConnectionsAlone(t *testing.T) {
	transport := &powerSaveHTTPTransport{}
	client := &http.Client{Transport: transport}
	doh := &dnsOverHTTPS{client: client}
	doh.CloseIdleQUICConnections()
	if transport.closed != 0 || doh.client != client {
		t.Fatal("power save must not reset HTTP/1 or HTTP/2 transports")
	}
}

func TestPowerSaveSkipsActiveDNSQueries(t *testing.T) {
	// An active exchange holds activity through reading the entire response.
	doq := &dnsOverQUIC{}
	doh := &dnsOverHTTPS{client: &http.Client{Transport: &http3Transport{}}}
	doq.activity.RLock()
	doh.activity.RLock()
	defer doq.activity.RUnlock()
	defer doh.activity.RUnlock()
	done := make(chan struct{})
	go func() {
		doq.CloseIdleQUICConnections()
		doh.CloseIdleQUICConnections()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("power save must skip busy resolvers without waiting for queries")
	}
}
