# WAP upstream support

Cheezy Core extends the standard Mihomo HTTP outbound with optional controls
for carrier WAP gateways that expose Internet access only through an HTTP
proxy. The feature is opt-in. Existing HTTP proxy configurations keep their
upstream behavior when the new fields are absent.

## Background

The target mobile network has no working direct Internet route. Its APN
provides an HTTP proxy at `192.168.192.192:9201`; Internet destinations are
reached with HTTP `CONNECT`. Field diagnostics confirmed:

- HTTPS subscription access and general HTTPS through `CONNECT`;
- TCP destinations on ports `443`, `8443`, and `13324`;
- at least 16 tunnels can be opened concurrently;
- idle tunnels may be closed by the gateway, so clients must tolerate closure
  and reconnect without falling back to a direct route;
- UDP transports such as WireGuard are not usable through this HTTP upstream.

CheezyClash currently budgets eight carrier-proxy connections: seven physical
tunnels for the core and one control-plane connection for the primary
subscription update.

## HTTP outbound options

Two fields were added to `adapter/outbound.HttpOption`:

```yaml
proxies:
  - name: __CHEEZY_WAP_UPSTREAM__
    type: http
    server: 192.168.192.192
    port: 9201
    max-connections: 7
    allowed-connect-ports:
      - 443
      - 8443
      - 13324
```

### `max-connections`

Limits the number of physical HTTP `CONNECT` tunnels to the same upstream.

- A value greater than zero enables the limiter.
- Omitted or zero leaves the HTTP outbound unlimited, matching upstream
  Mihomo behavior.
- When all slots are occupied, new dials wait instead of opening another
  socket.
- Waiting observes the dial context. Cancellation and deadlines stop the wait
  without consuming a slot.

Limiters are stored in a process-wide registry keyed by upstream
`server:port` and username. Recreated adapters therefore share the same slot
budget during a configuration reload. A reload that tries to assign a
different limit to the same key is rejected instead of temporarily creating a
second independent pool.

The password is deliberately excluded from the registry key. It is neither
logged nor exposed through limiter diagnostics.

### `allowed-connect-ports`

Restricts the destination ports accepted by this HTTP outbound.

- A non-empty list acts as an allowlist.
- Destinations outside the list are rejected before a limiter slot is acquired
  and before any socket is opened.
- Ports outside `1..65535` make the outbound configuration invalid.
- An omitted or empty list allows every destination port, matching upstream
  behavior.

This protects the measured carrier profile from accidental connections to
unsupported ports. It is not a general policy engine and does not replace
Mihomo rules.

## Limiter lifecycle

The implementation lives in `component/connectionlimit` and is integrated at
the beginning of `adapter/outbound.Http.DialContext`.

1. The destination port is checked against the allowlist.
2. The dial waits for a limiter token when limiting is enabled.
3. Mihomo opens the TCP connection to the HTTP upstream.
4. The HTTP `CONNECT` handshake is performed.
5. A successful connection is wrapped so that `Close` releases the token
   exactly once.

The token is also released on every TCP dial or `CONNECT` handshake error. The
release callback is idempotent, which prevents duplicate closes from corrupting
the active count.

The guarantee applies to physical tunnels created through `Http.DialContext`.
A caller that supplies an already-created connection directly to
`StreamConnContext` is outside the limiter lifecycle.

## Diagnostics

`Http.ConnectionLimitStats` provides non-sensitive counters:

- `active`: occupied slots;
- `waiting`: dials waiting for a slot;
- `limit`: configured physical-connection limit;
- `rejectedPorts`: destinations rejected by the allowlist for that adapter.

The limiter counters are atomic. `active`, `waiting`, and `limit` describe the
shared registry entry; `rejectedPorts` belongs to the concrete HTTP adapter.
No destination hostnames, credentials, or traffic contents are recorded.

## CheezyClash integration contract

Cheezy Core provides the bounded HTTP outbound primitive. The Android client
is responsible for constructing the complete fail-closed mode. When WAP mode
is enabled, CheezyClash:

- resolves the APN HTTP proxy, with a manual mode and the carrier fallback
  `192.168.192.192:9201`;
- creates the hidden `__CHEEZY_WAP_UPSTREAM__` outbound with a limit of seven
  and the measured port allowlist;
- assigns it with `dialer-proxy` to compatible TCP nodes and providers;
- removes UDP/QUIC-only nodes, WireGuard, unmeasured ports, the recursive WAP
  proxy node, and direct fallbacks;
- routes DNS over DoH/443 through the WAP outbound;
- preserves required same-origin subscription headers without forwarding HWID
  headers to unrelated domains;
- binds outbound sockets to the selected cellular Android `Network` before
  calling `VpnService.protect`, and rejects a socket if either operation fails;
- stops the VPN when the selected cellular network is lost.

These Android policies are intentionally not hard-coded into the generic
Mihomo rule engine. A non-Android consumer must construct an equivalent
fail-closed configuration if it wants the same guarantees.

## Effect on normal operation

The WAP patch does not globally cap Mihomo connections. A regular HTTP proxy
without `max-connections` receives no limiter, and an outbound without
`allowed-connect-ports` receives no port filter. Other outbound types are not
modified by this patch.

The additional package and conditional checks are present in every build, but
the queue, shared registry entry, and rejection behavior are activated only by
the new HTTP outbound fields.

## Tests

Run the focused tests with:

```sh
go test ./component/connectionlimit ./adapter/outbound
```

The tests cover:

- queueing at capacity and release after close;
- context cancellation while waiting;
- registry reuse and rejection of a changed limit;
- port rejection before dialing;
- release after a real local mock `CONNECT` exchange;
- limiter and rejected-port counters.

All Cheezy Core integration PRs must also pass `go test ./...` and the Android
core build in CheezyClash before a commit SHA is promoted.

## Maintenance boundaries

The patch is intentionally limited to:

- `component/connectionlimit/limiter.go`;
- `adapter/outbound/http.go`;
- focused tests beside those packages.

Keep WAP-specific routing and Android network ownership outside these files.
When synchronizing a new `Alpha` upstream, preserve the two YAML fields and the
`DialContext` acquire/release sequence, then run the focused tests before the
full CI suite.
