# Xray Mux.Cool client (Cheezy)

Клиентский Mux.Cool поверх raw TCP VLESS + REALITY. Код: [`adapter/outbound/xraymux`](../adapter/outbound/xraymux/), включается из [`adapter/outbound/vless.go`](../adapter/outbound/vless.go) опцией `xray-mux`.

Wire-протокол совместим с **xray-core**.

## YAML

```yaml
xray-mux:
  enabled: true
  concurrency: 2            # soft: сколько стримов на один carrier до открытия нового
  max-connections: 0        # физ. carrier’ов; 0 = без лимита
  max-worker-uses: 128      # 0 → 128
```

Только raw TCP VLESS; `flow` / ws / grpc / xhttp → mux тихо выключается.

## Поведение (актуальная стабильная линия)

После регрессии demux-isolation / spread-first клиент **откатан** к модели:

- **Pack-first**: сначала заполняется soft `concurrency` на существующем carrier, потом новый
- Per-session download pipe **512KiB** + `bufio` 64KiB на demux
- Backpressure при полном pipe (**блок demux**, сессию не рвём)

Это состояние после фиксов download≈0 (kill-on-full) и плато ~60Mbps из‑за окна 64KiB.

### Практические настройки на LTE

| Цель | Конфиг |
|---|---|
| Скорость multi-stream | низкий `concurrency` (1–2) → больше физических TCP |
| Меньше handshake | высокий `concurrency` / `max-connections: 1` → один TCP, скорость ниже |

Один TCP не масштабируется на несколько жадных download’ов (одно cwnd + TCP HoL).

### Локальный бенч (ядро + Xray)

```bash
XRAY_BIN=/path/to/xray go run ./hack/xraymux-bench \
  -payload-mb 12 -rtt-ms 80 \
  -out /tmp/xraymux-bench.json
```

Снимает Mbps, число physical dials, peak RSS клиента/Xray, CPU.

Пример (localhost, VLESS/TCP, userspace RTT≈80ms, **без** REALITY/потерь; netem в среде недоступен — cwnd не моделируется):

| case | streams | conc | dials | Mbps | RSS cli | RSS xray |
|---|---:|---:|---:|---:|---|---|
| mux off | 4 | — | 4 | ~1360 | ~42MiB | ~30MiB |
| c=1 | 4 | 1 | 4 | ~1360 | ~42MiB | ~36MiB |
| c=2 | 4 | 2 | 2 | ~755 | ~37MiB | ~35MiB |
| c≥4 | 4 | 4–16 | **1** | ~394 | ~31MiB | ~33MiB |
| c=2 | 8 | 2 | 4 | ~1490 | ~51MiB | ~41MiB |
| c=8 | 8 | 8 | **1** | ~411 | ~34MiB | ~39MiB |

Вывод: при pack-first скорость в этом стенде почти линейно следует числу dials. `max-connections: 2/3` при `concurrency: 8` и 4 стримах **не** открывает лишние carrier’ы (soft 8 вмещает всех на одном). Артефакты: `/opt/cursor/artifacts/xraymux-bench/`.

## Что откатили и почему

Эксперименты **не** оправдались в поле:

1. Demux isolation (overflow / carrier-buffer) — сложный admit-path; на практике скорость падала до 1–2 Mbps при любом concurrency
2. Spread-first при `max-connections > 0` — лишние параллельные REALITY-dial’ы / нестабильность («нет соединения»)

Не возвращать без новой модели и полевых замеров: kill session на полном pipe; per-session pipe 64KiB; сложный overflow-admit по умолчанию.

## Тесты

```bash
go test ./adapter/outbound/xraymux/ -count=1
go test ./adapter/outbound/ -run XrayMux -count=1
```
