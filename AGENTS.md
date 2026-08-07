# Cursor Cloud specific instructions

## Xray Mux

Client Mux.Cool for raw TCP VLESS+REALITY: `adapter/outbound/xraymux`. YAML, spread-first carriers, and LTE tradeoffs: [`docs/xray-mux.md`](docs/xray-mux.md).

On mobile prefer `max-connections: 2`–`3` (not `1`) with moderate `concurrency` — one carrier cannot fix TCP HoL for multi-stream speedtests.

```bash
go test ./adapter/outbound/xraymux/ -count=1
go test ./adapter/outbound/ -run XrayMux -count=1
```

Do not reintroduce: killing a session when its download pipe is full; a 64KiB per-session pipe; multi‑MiB default overflow that bufferbloats LTE.
