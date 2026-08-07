# Cursor Cloud specific instructions

## Xray Mux

Client Mux.Cool for raw TCP VLESS+REALITY lives in `adapter/outbound/xraymux`. YAML and demux-isolation design (divergence from xray-core runtime buffering only; wire-compatible) are documented in [`docs/xray-mux.md`](docs/xray-mux.md).

```bash
go test ./adapter/outbound/xraymux/ -count=1
go test ./adapter/outbound/ -run XrayMux -count=1
```

Do not reintroduce: (1) killing a session when its download pipe is full; (2) a 64KiB per-session pipe (that was xray's *worker* cushion, not session buffer).
