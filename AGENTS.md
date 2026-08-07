# Cursor Cloud specific instructions

## Xray Mux

Client Mux.Cool for raw TCP VLESS+REALITY: `adapter/outbound/xraymux`. See [`docs/xray-mux.md`](docs/xray-mux.md).

Stable line: pack-first pool, 512KiB session download pipe, blocking backpressure (no session kill on full). Demux-isolation / spread-first were reverted after field regression (1–2 Mbps / connection failures).

```bash
go test ./adapter/outbound/xraymux/ -count=1
go test ./adapter/outbound/ -run XrayMux -count=1
XRAY_BIN=/path/to/xray go run ./hack/xraymux-bench -payload-mb 12 -rtt-ms 80 -out /tmp/mux-bench.json
```
