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
