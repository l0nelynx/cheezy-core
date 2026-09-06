# Cursor Cloud specific instructions

## Xray Mux

Client Mux.Cool for raw TCP VLESS+REALITY: `adapter/outbound/xraymux`. See [`docs/xray-mux.md`](docs/xray-mux.md).

Stable line: 512KiB session pipe + **async 256KiB carrier downlink** (xray DialingWorkerFactory pattern) + blocking session backpressure. Soft-grow when `max-connections > 0`, least-loaded packing, optional `max-dials-per-minute`. See [`docs/xray-mux.md`](docs/xray-mux.md).

```bash
go test ./adapter/outbound/xraymux/ -count=1
go test ./adapter/outbound/ -run XrayMux -count=1
XRAY_BIN=/path/to/xray go run ./hack/xraymux-bench -payload-mb 12 -rtt-ms 80 -out /tmp/mux-bench.json
```
