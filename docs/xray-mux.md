# Xray Mux.Cool client (Cheezy)

Клиентский Mux.Cool поверх raw TCP VLESS + REALITY. Код: [`adapter/outbound/xraymux`](../adapter/outbound/xraymux/), включается из [`adapter/outbound/vless.go`](../adapter/outbound/vless.go) опцией `xray-mux`.

Wire-протокол совместим с **xray-core**. Расхождения только в **локальной** политике буферизации demux (см. ниже).

## Зачем

Цензор режет по числу TLS/REALITY handshake. Mux упаковывает много логических TCP в один физический carrier → меньше ServerHello.

Hysteria/QUIC не дают REALITY (валидный ServerHello с разрешённого домена), поэтому стек здесь: **REALITY + TCP + Mux.Cool**.

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
      concurrency: 16           # soft: сколько стримов на один carrier, прежде чем открыть новый
      max-connections: 1        # физических carrier’ов; 0 = без лимита
      max-worker-uses: 128      # после N сессий worker уходит в idle-close; 0 → 128

      # Demux isolation (байты; 0 / omit → дефолт)
      session-buffer: 0         # soft pipe на стрим; дефолт 524288 (512KiB)
      session-max-buffer: 0     # pipe + overflow hard на стрим; дефолт 2097152 (2MiB)
      carrier-buffer: 0         # сумма buffered на worker; дефолт 4194304 (4MiB)
      worker-read-buffer: 0     # bufio на demux; дефолт 65536 (64KiB)
```

### Смысл `concurrency`

Мягкий лимит **одновременно открытых** логических стримов на один физический коннект.

При `concurrency: 16` и `max-connections: 0`: стримы 1–16 → carrier #1; 17-й → новый carrier (если на первом уже 16 открытых). Закрыл слот — следующий снова садится на тот же carrier.

`hard concurrency = concurrency × 4`: если `max-connections` уже исчерпан, допаковываем до hard, потом ждём слот (не дропаем).

### Рекомендации под угрозу

| Цель | Конфиг |
|---|---|
| Мало handshake (цензор) | высокий `concurrency`, `max-connections: 1`–`2` |
| Откат к поведению «как xray demux HoL» | `session-max-buffer` = `session-buffer` (= `carrier-buffer`) |

## Архитектура

```text
DialContext (логический TCP)
  → Pool: soft reserve / dialWorker (singleflight) / hard pack / wait
  → worker.openSession: Mux New frame (вне pool/worker lock)
  → session: Read/Write ↔ Mux Keep frames

worker.readLoop (один на carrier)
  → bufio.Reader(worker-read-buffer)
  → admit(payload): tryWrite → session pipe; иначе overflow
  → TCP backpressure только на session-max / carrier-buffer
```

- Физический dial **не** держит `pool.mu`.
- Запись кадров сериализуется `writeMu` (`net.Buffers`).
- Upload идёт напрямую в `writeFrame` (без session pipe) → поэтому при старых багах download мог быть ≈0 при живом upload.

## Demux isolation (дизайн B)

**Проблема:** один медленный стрим заполняет soft pipe → раньше `readLoop` блокировался → не читал кадры соседей → TCP window падала у всего carrier (особенно LTE + высокий concurrency).

**Решение:**

1. Soft `session-buffer` — быстрый `tryWrite` в pipe.
2. Если pipe полон, но `(pipe+overflow+frame) ≤ session-max-buffer` и сумма по worker ≤ `carrier-buffer` — кадр в **overflow**, demux идёт дальше.
3. Иначе demux **ждёт** (TCP backpressure), сессию не рвём.
4. `Read` сначала дочитывает pipe (порядок), overflow сбрасывается в pipe по мере места.

### Расхождение с xray-core

| | xray-core | Cheezy (B) |
|---|---|---|
| Wire | Mux.Cool | тот же |
| Полный session writer | блокирует весь `fetchOutput` | парк в overflow, demux продолжает |
| TCP backpressure | сразу при полном session pipe | при `session-max` / `carrier-buffer` |
| Drop на full | нет | нет |
| Память downlink | ~N × 512KiB | до `carrier-buffer` на carrier (дефолт 4MiB) |

Сервер xray **не** видит разницы в протоколе. Меняется только то, как долго клиент продолжает `Read` из TLS при перегрузке одного стрима. На cap поведение снова как у xray (окно схлопывается). Рассинхрона байтового потока нет.

История багов, которые нельзя возвращать:

1. Kill session при полном pipe → download ≈ 0.
2. `sessionPipeLimit = 64KiB` (лимит **worker** pipe xray, не session) → плато ~window/RTT (~60 Mbit).

## Ограничения

- Только raw TCP VLESS; `flow` / не-TCP transport → mux disabled.
- TCP HoL при потерях на LTE **не** убирается (нужен QUIC); B снимает только **demux** HoL.
- Один жадный стрим может заполнить caps и вернуть demux-HoL — позже и с предсказуемым RAM.

## Тесты

```bash
go test ./adapter/outbound/xraymux/ -count=1
go test ./adapter/outbound/ -run XrayMux -count=1
# опционально interop:
XRAY_BIN=/path/to/xray go test ./adapter/outbound/ -run TestVlessXrayMuxInterop -count=1
```
