# AntiLag proxy group

An AntiLag group selects an exit for a new connection. For UDP it maintains a
logical packet socket with a binding per remote IP and port. Each binding has
one active path and attempts to open one standby. A failed standby preparation
is retried while the binding is active. Idle destination bindings expire after
two minutes. Each logical UDP socket supports up to 256 concurrent destinations.

## Independent exits

Add a group to the profile and place the game rule before broader matching rules.

```yaml
proxy-groups:
  - name: game-routes
    type: antilag
    proxies: [path-a, path-b]
    # Alternatively use existing proxy providers with "use".
    # Optional. Replace these documentation IPs with real UDP STUN servers.
    # probe-targets:
    #   - {cidr: 203.0.113.0/24, address: '192.0.2.10:3478'}
    #   - {address: '192.0.2.20:3478'}
rules:
  - PROCESS-NAME,Darktide.exe,game-routes
```

The most specific matching CIDR wins. One entry without a CIDR supplies the
default. Addresses are literal IPv4 or bracketed IPv6 addresses with ports.
Use reflectors representative of the destination regions. With no matching
reflector, manual switching and recovery from a terminal transport error work.
A successful probe measures a round trip through the exit to the reflector.

Independent exits generally change the public UDP address or port seen by the
game server. Whether the game accepts that change depends on its protocol.
New TCP connections use the selected route. Established TCP connections keep
their original exit.

## Stable exit

Configure an existing WireGuard proxy with `dialer-proxy: game-routes` and
point the game rule at that **WireGuard proxy**. Configure both carrier nodes
for UDP. Keep one persistent WireGuard peer and its NAT state at the exit.
Place a STUN reflector near the WireGuard peer for carrier comparison.

```yaml
# Inside the existing WireGuard outbound with its keys and addresses:
dialer-proxy: game-routes

# In rules:
# - PROCESS-NAME,Darktide.exe,stable-wireguard-exit
```

The binding then represents the WireGuard peer endpoint. Switching it moves
all enclosed flows on that peer together. The peer learns the new carrier
address through authenticated WireGuard traffic. The tunnel's inner addresses
and the exit NAT mapping can survive the switch. A failure of the common exit,
loss of its NAT state, or an outage beyond application timeouts can still end
a session. Probes run through separate packet sockets to the STUN reflector.

## Switching a running session

Use the existing controller authentication. Get the active bindings with
`GET /group/game-routes/bindings`. Choose the binding by process, source and
destination. A game can open more than one. POST one binding ID and an exact
group member name to `/group/game-routes/failover`.

```json
{"id":"binding-uuid","candidate":"path-b"}
```

HTTP 204 indicates completion. A second request for the active candidate
is idempotent. Preparation failure returns HTTP 409 and leaves the current
path active. Refresh the bindings after a reconnect or profile reload.

Binding details include the current active and ready reserve nodes, the reason
for selection and last change, and the control endpoint when configured.
`evidence` reports recent probe counts, replies, last probe time and p95 in
milliseconds once at least 20 replies exist. `detectMs` measures from the
first missed probe to the decision for silent failures. Hard errors are
observed directly and use zero. `prepareMs` includes the time originally
spent opening the chosen child; `switchMs` measures the transition including
any on-demand dial. `receiveGapMs` is populated when a reply is consumed on
the new path and a previous path had provided a reply.

Switching retires the old read and write generation. A datagram whose write
was interrupted may be lost. Its payload is never replayed on B. Subsequent
datagrams use B. The tunnel sender takes its UDP queue-age policy from the
initial path for the lifetime of the source session.

## Selection and probes

Each binding's `candidates` lists eligible exits in current preference order.
Recent repeated failures put a candidate behind healthy ones. Within each
class a lower `cost` is preferred. This is a heuristic cost in millisecond
units, not a measured game ping. `selection` records how the live path was
chosen; the ranking is recomputed when read.

The starting cost uses the group's health check (median known value when
missing, or 100 when all are unknown). For a qualified regional UDP
comparison it uses probe p95 plus 500 times the missed fraction, with the
confirmed regional winner as the cost floor until a challenger qualifies.
Real first-response times blend into that prior with weight n/(n+4).
Effective sample count decays with a 12-hour half-life and is capped at
16 for weighting. Older cache entries seed one observation. Exact endpoints
receive full weight, service/prefix evidence 45%, service/ASN evidence
20%. The highest-confidence source wins. Each recent consecutive failure
also adds a fading penalty (40 cost units per failure up to three, five-minute
half-life). History separates TCP and UDP, address family, group and
profile. Route history is capped at 4096 entries with seven-day expiry.
Opening a socket and receiving a response are distinct observations.
The existing bbolt cache stores route history and independent probe windows.
Measurements remain usable in memory when the cache is unavailable.

On active bidirectional UDP paths, ten-second windows with at least 32
receive intervals record the bucketed p95 minus median interval, capped at
100 ms. A gap over one second or lack of recent outbound activity discards
the window. This measures observed delivery cadence, including application
and local scheduling effects. A confidence-weighted difference from the
measured candidates' mean contributes only to that exact endpoint. The
observation is recorded once per window per path; individual datagrams are
not counted as independent first replies or evidence of network loss.

Approximately every eighth new connection to an endpoint can try a
less-used eligible candidate within 25 cost units or 25% of the leader,
whichever is larger. This permits learning without a reflector. A 30-tick
standby review can prepare a better reserve before retiring the old one;
the same 10-unit / 15% improvement margin limits reserve churn. Changes in
estimated quality affect new connections and reserve selection. Existing
healthy sessions stay pinned until manual switching or confirmed failure.

Active bindings share control probes for identical candidate/reflector
pairs. The monitor ticks once per second, probing active and ready standby
paths with a one-second timeout. Three successive paired observations with
A missing and B responding trigger failover. Both paths missing a response
clears that evidence. A healthy running flow stays pinned through changes
in rankings. Recovery of A makes it eligible for future sessions.

Every 30 monitor ticks an additional regional round compares the active
candidate to a rotating alternative with three transactions each. Probe
work is bounded; slow probes or many distinct reflectors can extend the
interval. Regional grades use up to 64 recent observations per member and
expire after five minutes. Two favorable comparisons with at least 20 replies
on each side are required. A challenger needs a loss advantage over five
percentage points, or a p95 improvement of at least the larger of 10 ms and
15 percent with a loss fraction within five percentage points of the leader.
Control loss and p95 describe the reflector path. Game-level latency and
actual match continuity must be measured in the game.

## Passive quality measurements

Both signals contribute to the cost for new connections and reserve selection.
They work without a configured reflector and do not trigger live failover.

* **UDP delivery stalls.** `GET /group/{name}/bindings` reports
  `candidates[].stalls` for each observed UDP destination IP, port and exit.
  After eight short reply intervals, the meter counts a gap exceeding the
  larger of 150 ms and four times the usual interval while successful writes
  continue. One uninterrupted gap counts once, even before replies resume.
  Long send pauses reset the observation. The API exposes `events`,
  `activeSeconds`, `perActiveMinute` and `excessSeconds` (time beyond the
  threshold). Concurrent bindings contribute separate active seconds. A cost
  applies after 20 active seconds and grows with frequency and duration.
  Six five-minute buckets are saved with the group's existing history.
  This is observable delivery cadence and includes effects from the game
  server and client scheduling.
* **Hysteria2 carrier.** `GET /proxies/{name}` and `candidates[].outerQuic`
  expose the latest five-second sample for an open Hysteria2 connection.
  The proxy endpoint returns it under `outerQuic`. All destinations using
  that node share the measurement. `inflationMs` is smoothed RTT above the
  active path minimum when `rttUsed` is true. `netLossFraction` is positive
  net growth of QUIC's sender-side loss counter divided by packets sent,
  capped at one. QUIC can revise earlier loss declarations. RTT contributes
  only when there is also incoming traffic. Windows with fewer than eight
  sent packets carry zero cost, and the weight reaches full strength at 32.
  Closing the connection removes the sample. Reconnecting starts a new
  generation. Sampling reads local counters without sending probes.

## Reproduce

```sh
go test ./adapter/outboundgroup -run TestAntiLag -count=1
python hack/antilag-smoke.py /path/to/built-core
```

The smoke test starts an isolated core and sends real datagrams through the
configured tunnel inbound. It checks controller authentication, changes the
exit and verifies the source port seen by an echo server. The Go tests cover
partial and complete probe blackouts, independent bindings, retirement of a
blocked write, persistence and a real WireGuard peer with an external UDP
echo service and a byte-checked TCP stream across a carrier switch.
