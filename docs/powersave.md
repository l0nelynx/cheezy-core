# Optional power saving

Build with `with_powersave` to let an embedded host or the external controller
reduce background work. The default mode is `active`, including after restart.
The tag alone does not detect screen state, Android doze, or desktop inactivity.

```sh
go build -tags with_gvisor,with_powersave
```

Keep the other tags required by your host. The tag applies when compiling the
Go binary or shared library, including its dependencies. `with_low_memory` is
independent and is not enabled by power saving. `features.Tags()` reports
`with_powersave` when compiled in.

This implementation is based on [Prizrak-Core commit 70d9da3](https://github.com/legiz-ru/Prizrak-Core/commit/70d9da336439784aa079ac81b894e3d274d64b15),
with traffic-driven statistics wake-up, selective idle DNS cleanup, and
synchronization between idle session cleanup and new dials.

## Host integration

An embedded Go host calls `component/powersave.Set`:

```go
powersave.Set(powersave.Background) // e.g. screen off
powersave.Set(powersave.Idle)       // e.g. Android doze
powersave.Set(powersave.Active)     // device back in use
```

The host must supply the initial device state and subsequent transitions.
Leaving the desktop window minimized does not imply that VPN users are idle.
Mode changes do not suspend the tunnel or reject new connections.

Without the tag, `Set` is a no-op and the core stays `active`.

## External controller

Both endpoints use the controller's existing authentication and are also
available in embedded mode:

- `GET /powersave` returns `{"enabled":true,"mode":"active"}`. Without the tag,
  it returns `{"enabled":false,"mode":"active"}`.
- `PATCH /powersave` accepts `{"mode":"active"}`, `{"mode":"background"}`,
  or `{"mode":"idle"}` and returns HTTP 204. Invalid or missing modes return
  HTTP 400. Builds without the tag return HTTP 501 for PATCH.

State is process-local and is not persisted in YAML. An external host should
reapply its device state after the core restarts.

## Behavior and trade-offs

| Work | Active | Background | Idle |
| --- | --- | --- | --- |
| Scheduled health checks | Configured interval | At least 30 minutes, or the longer configured interval | Paused, including the initial check |
| Manual / failed-dial health checks | Enabled | Enabled | Enabled |
| Unused Hysteria2 / TUIC sessions | Kept as before | Closed after at least one minute unused, swept once a minute | Same as background |
| DNS over QUIC / HTTP3 | Configured keep-alive | New connections have no keep-alive; idle connections are closed on sweeps | Same as background |
| Traffic speed sampler | Once a second | Sleeps after a zero sample; wakes on traffic or active mode | Same as background |

Health checks catch up once on returning active when overdue, honoring `lazy`.
The timer is
stopped while idle. Node health and latency measurements can be stale during
power saving; automatic recovery after failed dials keeps its existing thresholds.

Idle cleanup keeps open streams and dials in progress. The next connection
after cleanup may require a new handshake. DNS cleanup skips in-flight queries,
including response-body reads, and leaves DoT and HTTP/1/HTTP/2 connections alone.
DNS policies, fallback, bootstrap, proxy and direct resolvers are included.
Speed samples resume with new background traffic; total byte counters are never
discarded by sleeping or waking the sampler.

## Optional log gating

`log.SetGated(true)` separately enables skipping formatting and delivery of
events below the configured log level. It defaults to off and is a no-op
without the tag. It does not follow power-save mode automatically.

Custom subscribers requiring a lower level must hold a `log.Want(level)` lease
and call the returned release function when done. Controller `/logs` readers do
this automatically. This prevents log gating from losing events requested by
an active reader. Detailed events suppressed before a reader subscribes cannot
be recovered later.

## Validation

```sh
go test ./component/powersave ./adapter/provider ./adapter/outbound ./transport/tuic/... ./dns ./tunnel/... ./log ./hub/route
go test -tags with_powersave ./component/powersave ./adapter/provider ./adapter/outbound ./transport/tuic/... ./dns ./tunnel/... ./log ./hub/route
go test -race -tags with_powersave ./component/powersave ./adapter/provider ./adapter/outbound ./transport/tuic/... ./dns ./tunnel/statistic ./log ./hub/route
go build -tags with_gvisor,with_powersave ./...
```

Power saving is opt-in. Battery-life and throughput effects depend on the host,
network, proxy protocols, and background workload; no fixed savings are promised.
