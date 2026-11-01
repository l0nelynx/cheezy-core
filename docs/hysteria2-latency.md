# Hysteria2 UDP latency experiments

This first stage adds local UDP queue diagnostics and an optional queue-age
limit for Hysteria2 outbounds. Both features are disabled by default. It does
not change the BBR profile, congestion window, pacing, receive windows, MTU,
TUN stack, QUIC dependencies, or the server protocol.

## Optional sender queue expiry

Add this to an existing Hysteria2 proxy, preserving its server, TLS and
authentication settings:

```yaml
bbr-profile: conservative
udp-max-queue-delay: 50ms
```

`50ms` is an experimental example, not a recommended value for every network.
First compare `standard` and `conservative` with expiry disabled. Keep `up`
and `down` absent or zero for BBR. The client's BBR profile affects its sending
direction; it does not configure the server's profile.

`udp-max-queue-delay` accepts a non-negative Go duration such as `50ms` or
`100ms`. Omitted, `0`, and `0ms` disable expiry. Values without a unit other
than zero, negative durations, and overflowing durations are rejected.

The limit applies to UDP packets from their admission to the tunnel's
per-source sender queue until the final age check before the outbound write.
It includes waiting for outbound connection setup and destination resolution.
Age is checked before resolution and again afterwards. Expired packets are
released without a write; remaining packets retain their order. There is no
retry, duplicate delivery, or additional sending goroutine.

Apply this only to an outbound selected for traffic that can tolerate stale
packet discards, using the existing process / destination routing rules. It
affects all UDP traffic routed through that outbound, not just games. HTTP/3
downloads are also UDP. Existing UDP sessions retain their selected outbound
and policy; use a new application session for a configuration comparison.

### Boundaries

- This is a pre-write age limit, **not** an end-to-end delivery deadline.
- It does not cancel an in-progress `WriteTo` / `SendDatagram`, remove data
  already in QUIC's queue, or change the receiver's queues.
- Time before sender admission (TUN, the ingress worker queue, or a sniffer
  holding packets) is not covered by the limit.
- The first packets of a cold connection can expire while DNS / QUIC setup
  completes. Too small a limit can impair application startup or increase
  loss, especially on high-RTT paths. Test cold and established connections.
- TCP streams and UDP sessions bypassing the tunnel sender (for example,
  an embedded caller writing directly to the outbound) do not use this limit.
- Reduced queue age may mean more intentional local drops. It cannot repair
  network loss or remove buffering in a router or at the server.

The queue policy is carried by the actual packet connection and found through
tracker / reference wrappers, so selecting a proxy group does not discard it.

## Diagnostics API

The external controller exposes the following endpoints under its existing
authentication. No debug build or additional build tag is required.

| Request | Behavior |
| --- | --- |
| `PATCH /udp/diagnostics` with `{"enabled":true}` | Start recording; HTTP 204 |
| `GET /udp/diagnostics` | Return the current / most recent session |
| `PATCH /udp/diagnostics` with `{"enabled":false}` | Stop recording new observations; HTTP 204 |

A false-to-true transition starts a fresh session. Repeated `true` requests
preserve the current one. Disabling retains its results; already admitted
packets can still complete their observations. Re-enabling uses a new
collector, so these completions cannot contaminate the new session.
State is process-local and not persisted in YAML. Recording is off on startup.
An embedded host can use `statistic.SetUDPDiagnosticsEnabled` and
`statistic.SnapshotUDPDiagnostics` directly.

The response contains `enabled`, `startedAt` (null before the first session),
`drops`, and three duration histograms:

| Histogram | Measurement |
| --- | --- |
| `ingressQueue` | Admission to the tunnel worker queue until dequeue |
| `senderQueue` | Admission to the per-source sender queue until dequeue, including connection setup while queued |
| `outboundWrite` | Duration of the outbound write helper, including resetting its read deadline |

`senderQueue` includes packets dequeued and then rejected as expired, but
not packets rejected on admission or drained on close. Destination resolution
after dequeue is outside this histogram. `outboundWrite` includes failed
write calls. Successful calls mean acceptance by the outbound, **not** wire
transmission, acknowledgement, or delivery to the remote application.

Each histogram has `count`, `totalUs`, `maxUs`, and cumulative `buckets`
containing `leUs` / `count`. Bounds are 100us, 500us, 1ms, 2ms, 5ms, 10ms,
20ms, 50ms, 100ms, 250ms, 1s, and a final unbounded bucket (`leUs: null`).
Durations in summary fields are truncated to whole microseconds. A percentile
can be bounded by the first bucket whose count reaches the desired fraction
of the total; the unbounded bucket cannot provide a finite upper bound.
Concurrent snapshots are approximate, not transactional.

`drops` contains these local reasons:

| Counter | Meaning |
| --- | --- |
| `ingressQueueFull` | The selected tunnel worker queue was full |
| `senderQueueFull` | The per-source sender queue was full |
| `queueExpired` | The packet exceeded the configured pre-write age limit |
| `senderClosed` | The sender was closed before processing / admission, or the packet was drained on close |
| `resolveError` | Destination resolution failed or produced no valid address |
| `writeError` | The outbound write returned an error |
| `writeBackError` | Writing a received datagram back to the local application failed |

These are process-wide measurements across tunnel UDP outbounds, with no
per-proxy attribution. They retain no payloads or addresses. They do not
measure QUIC-internal queue drops, Hysteria reassembly / fragmentation, TUN
driver drops, policy / metadata rejection, or loss in the network. They must
not be presented as total packet loss or divided by histogram counts to infer
a network loss rate.

Recording uses fixed-size atomic counters / histograms and creates no
per-packet metric allocations. With diagnostics and expiry disabled,
established sender sessions skip the added clock reads. During outbound
setup, packet timestamps are retained until the selected policy is known.
The timestamp fields increase queue slot size; queue capacities are unchanged.

## Validation and next steps

```sh
go test ./adapter ./adapter/outbound ./tunnel ./tunnel/statistic ./hub/route -run 'Test(ParseHysteria2|Hysteria2UDP|UDP)' -count=1
go test ./listener/inbound -run Hysteria2 -count=1
go test ./tunnel -run '^$' -bench BenchmarkUDPSenderQueue -benchmem
```

For network testing, compare expiry off / on using the same route and BBR
profile. Measure application UDP RTT percentiles, jitter, loss, and stalls
while independently saturating upload and download. Include cold startup,
slow DNS, a brief outage, and concurrent bulk UDP / TCP traffic. Record CPU
usage and throughput as well as latency. The local queue benchmark does not
establish an improvement in game or call latency.

Traffic-class QUIC connection separation, transport-level datagram expiry,
pacing changes, and adaptive datagram sizing remain separate follow-up work.
No network-performance improvement is asserted without those measurements.
