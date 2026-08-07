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
| Скорость multi-stream | низкий `concurrency` (2–4) → чаще отдельные TCP/cwnd |
| Меньше handshake | высокий `concurrency`, `max-connections: 1` — на LTE multi-stream обычно сильно хуже |

Один TCP не масштабируется на несколько жадных download’ов на мобильной сети (одно cwnd + TCP HoL). Это ограничение стека REALITY+TCP, не «недокрученный concurrency».

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
