# Xray Mux.Cool client (Cheezy)

Клиентский Mux.Cool поверх raw TCP VLESS + REALITY. Код: [`adapter/outbound/xraymux`](../adapter/outbound/xraymux/), включается из [`adapter/outbound/vless.go`](../adapter/outbound/vless.go) опцией `xray-mux`.

Wire-протокол совместим с **xray-core**.

## YAML

```yaml
xray-mux:
  enabled: true
  concurrency: 4              # soft streams/carrier after carriers are grown
  max-connections: 3          # soft-grow target + hard cap (0 = pack-first, unlimited)
  max-dials-per-minute: 3     # handshake budget; 0 = unlimited
  max-worker-uses: 128        # 0 → 128
```

Только raw TCP VLESS; `flow` / ws / grpc / xhttp → mux тихо выключается.

## Политика пула (xmux-inspired)

| `max-connections` | Поведение |
|---|---|
| `0` | **Pack-first**: заполняем soft `concurrency`, потом новый dial |
| `> 0` | **Soft-grow**: пока carrier’ов < max — новый dial (сериализованно); затем **least-loaded** pack; hard = `concurrency×4`; потом wait |

`max-dials-per-minute`: sliding window на новые physical dials. При исчерпании бюджета — pack на существующие, без ожидания новой минуты (если есть soft/hard слот).

### Практические настройки под цензор ≤3 TLS/мин

```yaml
concurrency: 4
max-connections: 3
max-dials-per-minute: 3
```

Не ставь `max-connections: 1` «ради concurrency» — concurrency тогда почти не влияет на handshake.
### Локальный бенч (ядро + Xray)

```bash
XRAY_BIN=/path/to/xray go run ./hack/xraymux-bench \
  -payload-mb 12 -rtt-ms 80 \
  -out /tmp/xraymux-bench.json
```

Снимает Mbps, число physical dials, peak RSS клиента/Xray, CPU.

Пример после soft-grow + dial-rate (localhost, VLESS/TCP, userspace RTT≈80ms):

| case | conc | max-conn | dial/min | dials | Mbps |
|---|---:|---:|---:|---:|---:|
| mux off | — | — | — | 4 | ~1384 |
| c=8 m=0 | 8 | 0 | 0 | 1 | ~395 |
| c=8 m=1 | 8 | 1 | 0 | 1 | ~396 |
| c=8 m=2 | 8 | 2 | 0 | **2** | **~758** |
| c=8 m=3 | 8 | 3 | 0 | **3** | **~758** |
| c=8 m=3 r=3 s=8 | 8 | 3 | 3 | **3** | **~1036** |
| c=8 m=3 r=1 s=8 | 8 | 3 | 1 | **1** | ~411 |

Вывод: `max-connections: 2–3` включает soft-grow и поднимает dials/скорость; `max-dials-per-minute: 1` принудительно оставляет 1 carrier. Артефакты: `/opt/cursor/artifacts/xraymux-bench/`.

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
