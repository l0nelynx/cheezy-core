# Xray Mux.Cool client (Cheezy)

Клиентский Mux.Cool поверх raw TCP VLESS + REALITY. Код: [`adapter/outbound/xraymux`](../adapter/outbound/xraymux/), включается из [`adapter/outbound/vless.go`](../adapter/outbound/vless.go) опцией `xray-mux`.

Wire-протокол совместим с **xray-core**. Расхождения только в **локальной** политике буферизации demux и выборе carrier’ов.

## Зачем

Цензор режет по числу TLS/REALITY handshake. Mux упаковывает много логических TCP в несколько физических carrier’ов → меньше ServerHello.

Hysteria/QUIC не дают REALITY, поэтому стек: **REALITY + TCP + Mux.Cool**.

## YAML

```yaml
proxies:
  - name: vless-reality-mux
    type: vless
    server: ...
    port: 443
    uuid: ...
    network: tcp          # только raw TCP; flow / ws / grpc / xhttp → mux тихо выключается
    tls: true
    reality-opts: { ... }
    xray-mux:
      enabled: true
      concurrency: 8            # soft streams/carrier после того, как набраны carrier’ы
      max-connections: 3        # физ. REALITY-коннектов; 0 = без лимита (pack-first)
      max-worker-uses: 128      # 0 → 128

      # Буферы demux (байты; 0 → дефолт)
      session-buffer: 0         # soft pipe; дефолт 524288 (512KiB)
      session-max-buffer: 0     # pipe+overflow hard; дефолт = session-buffer (overflow opt-in)
      carrier-buffer: 0         # сумма на worker; дефолт 1048576 (1MiB)
      worker-read-buffer: 0     # bufio demux; дефолт 65536
```

### Рекомендации

| Сеть / цель | Конфиг |
|---|---|
| LTE + мало handshake | `concurrency: 8`, **`max-connections: 2`–`3`** (spread-first) |
| Один carrier (макс. стелс) | `max-connections: 1` — на LTE multi-stream speedtest часто 1–2 Mbps (лимит TCP, не yaml) |
| Wi‑Fi, BDP | крупные `session-buffer` при необходимости; overflow: поднять `session-max-buffer` |
| Откат к xray demux HoL | `session-max-buffer` = `session-buffer` (и так по дефолту) |

### Почему concurrency≥4 на одном carrier даёт 1–2 Mbps на LTE

1. **Один TCP = одно cwnd**. Speedtest открывает несколько жадных download’ов → все делят одно окно; потеря сегмента стопорит весь carrier (TCP HoL). Demux isolation этого **не** лечит.
2. **Большой userspace buffer (2–4 MiB)** на мобилке даёт bufferbloat → ещё хуже. Поэтому дефолт overflow выключен, `carrier-buffer` = 1MiB.
3. **`concurrency: 2` казался «нормой»** не магией числа 2, а потому что при том же speedtest чаще открывался **второй** физический коннект (pack-first после soft fill).

Итог: против цензора нужен **не** один carrier на всё, а **мало** carrier’ов (2–3) со spread-first.

## Политика выбора carrier

| `max-connections` | Поведение |
|---|---|
| `0` (unlimited) | **Pack-first**: сначала soft `concurrency` на существующий, потом новый |
| `> 0` | **Spread-first**: пока carrier’ов < max — каждый новый стрим открывает новый REALITY-коннект; затем least-loaded pack до soft/hard |

`hard = concurrency × 4` — допаковка, когда max уже достигнут; потом wait (не drop).

## Demux isolation (opt-in)

Если `session-max-buffer` > `session-buffer`, кадры сверх soft pipe паркуются в overflow, demux продолжает соседей, пока не упрётесь в session-max / carrier-buffer.

| | xray-core | Cheezy |
|---|---|---|
| Wire | Mux.Cool | тот же |
| Полный session writer | блокирует весь demux | при overflow — парк и продолжение |
| TCP backpressure | сразу при полном pipe | на session-max / carrier-buffer |

Сервер xray разницы в протоколе не видит.

Не возвращать: (1) kill session при полном pipe; (2) per-session pipe 64KiB (это worker cushion xray, не session).

## Тесты

```bash
go test ./adapter/outbound/xraymux/ -count=1
go test ./adapter/outbound/ -run XrayMux -count=1
```
