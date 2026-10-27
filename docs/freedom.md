# Freedom и фрагментация TLS

`type: freedom` — прямой outbound с необязательной TCP/TLS-фрагментацией и TCP socket options. Основа порта — Prizrak-Core [355d7467](https://github.com/legiz-ru/Prizrak-Core/commit/355d7467d8a93d526f1fb861f3608a1796ad9aa1) и [64161645](https://github.com/legiz-ru/Prizrak-Core/commit/64161645ba9857b814a8a4edfba3bf3cba45e031). Включается адресно через правила или как dialer другого outbound.

## Прямой HTTPS к конкретному хосту

Рабочий пример: [examples/freedom-host.yaml](examples/freedom-host.yaml).

```yaml
mode: rule

proxies:
  - name: FRAGMENT
    type: freedom
    fragment:
      packets: tlshello
      length: "100-200"
      interval: "0"

rules:
  - AND,((DOMAIN,example.com),(NETWORK,TCP),(DST-PORT,443)),FRAGMENT
  - MATCH,DIRECT
```

Замените `example.com` своим хостом. Это правило выбирает только TCP/443 к точному имени; `www.example.com` отдельно не совпадает. Для домена вместе со всеми поддоменами замените `DOMAIN` на `DOMAIN-SUFFIX`. Привязка хоста находится в `rules`, отдельного поля host внутри fragment нет.

В существующем конфиге добавьте outbound к своим `proxies`, а правило поставьте выше общих правил и `MATCH`. Последнюю строку примера не нужно подставлять вместо вашего существующего маршрута по умолчанию. Нужен режим `rule`; режим `global` эти правила не выбирает.

Правило `DOMAIN,example.com,FRAGMENT` тоже допустимо: оно направит весь трафик хоста в Freedom, но UDP всё равно уйдёт обычным прямым соединением. Первый пример явно ограничивает выбор TCP/443.

При работе через TUN домен должен быть известен ядру: например, из fake-ip DNS mapping или подходящего sniffer. Для подключения только по IP без восстановленного домена правило DOMAIN не совпадёт. В случае ECH `split: sni` видит только доступный внешний ClientHello/SNI.

## Fragment прямо в прокси: общий YAML подписки

VLESS, VMess и Trojan принимают `fragment` непосредственно в своём блоке. Это предпочтительный способ для подписок: отдельный outbound и ссылка `dialer-proxy` не нужны.

Добавьте к существующему узлу:

```yaml
fragment:
  packets: tlshello
  split: sni
  interval: "1-3"
```

Остальные поля узла, включая REALITY и smux, остаются прежними. Полный пример с sing-mux: [examples/vless-inline-fragment.yaml](examples/vless-inline-fragment.yaml). При `split: sni` параметр length не используется. Для дробления по длине задайте `split: length` и `length: "100-200"`.

Фрагментатор ставится под TLS/REALITY, на каждое новое TCP-соединение к серверу. Он действует и при smux/Xray mux, а также на TCP-соединения WS/H2/gRPC/XHTTP. Уже установленные соединения не меняются; для проверки перезапустите ядро или пересоздайте carriers. UDP/QUIC-сокеты не фрагментируются.

При наличии собственного `dialer-proxy` inline fragment оборачивает результат этого dialer; второй Freedom с тем же fragment не нужен, иначе получится повторная фрагментация. Для обычного узла достаточно одного inline-блока.

Ядра Mihomo с parser, игнорирующим неизвестные поля, могут принять тот же YAML и работать без фрагментации. Отдельные GUI/converter могут иметь собственную строгую проверку; совместимость каждого клиента определяется им. Обязательных ссылок на отсутствующий outbound этот способ не добавляет.

## Отдельный Freedom через dialer-proxy

Этот вариант остаётся доступен для локальных конфигураций. Создайте `FRAGMENT` и добавьте в существующий узел VLESS/Trojan или другой прокси с TCP/TLS:

```yaml
proxies:
  - name: FRAGMENT
    type: freedom
    fragment:
      packets: tlshello
      length: "100-200"
      interval: "0"

  - name: MY-REALITY
    type: vless
    server: proxy.example.net
    port: 443
    uuid: 00000000-0000-0000-0000-000000000001 # заменить своим
    tls: true
    client-fingerprint: chrome
    dialer-proxy: FRAGMENT
    # Оставьте здесь свои network, servername, reality-opts и остальные параметры.
```

Здесь Freedom фрагментирует внешний TLS/REALITY handshake к `proxy.example.net` для этого узла. Сайты продолжайте направлять в `MY-REALITY` или содержащую его группу. Это не фильтр сайтов внутри туннеля: handshake к серверу общий для обслуживаемого им трафика. Остальные узлы без `dialer-proxy: FRAGMENT` не используют этот outbound.

Сам Freedom всегда открывает прямое соединение; `dialer-proxy` задаётся у использующего его узла. Для QUIC/UDP TLS-фрагментация Freedom не применяется.

## Параметры fragment

| Поле | Значение |
| --- | --- |
| `packets: tlshello` | Первый TLS handshake record разбивается на несколько TLS records. Record, пришедший частями в нескольких Write, предварительно собирается |
| `packets: "1-3"` | Дробятся вызовы Write с первого по третий; это не номера IP-пакетов |
| `packets: ""` | Дробятся все вызовы Write |
| `split: length` | Режим по умолчанию, размеры задаёт `length` |
| `length: "100-200"` | Случайная длина от 100 до 199 байт; последний фрагмент может быть короче. Одиночное число — фиксированная длина |
| `max-split: "3"` | Не более трёх частей; последняя содержит весь остаток. По умолчанию 0 — без ограничения |
| `interval: "0"` | Пауза между частями в миллисекундах; допустим диапазон, например `"1-3"` |
| `split: sni` | Разрезы внутри меток имени SNI; length не требуется. Если подходящего SNI нет, запись проходит без таких разрезов |
| `wait: interval` | Режим по умолчанию: паузы по interval |
| `wait: ack` | Ожидание подтверждения между частями; при недоступной статистике применяется fallback-delay |
| `fallback-delay: 500` | Задержка в миллисекундах для ACK fallback; 500 — значение по умолчанию |

Например, вместо фиксирования длин можно включить SNI split:

```yaml
fragment:
  packets: tlshello
  split: sni
  interval: "1-3"
```

При `tlshello` + `interval: "0"` новые TLS records отправляются одним Write. Это дробление TLS records, а не гарантия отдельных TCP-сегментов. Ненулевой interval разделяет записи по времени, но итоговая пакетизация всё равно зависит от ОС и сети.

`wait: ack` использует статистику сокета на Linux/macOS; Windows в этом порте использует fallback-delay. При множестве фрагментов это может сильно увеличить handshake latency. После окончания выбранного диапазона записей wrapper разрешает relay перейти на исходное соединение.

Показанные значения — исходная точка для проверки на вашей сети, не универсальная настройка обхода DPI. Крайний вариант `length: "1-1"` создаёт TLS record на каждый байт payload и заметный overhead.

## HTTP/3 и UDP

Freedom не фрагментирует QUIC/HTTP/3. Если браузер использует UDP/443, TCP-правило его не затронет. При необходимости проверить HTTPS через TCP можно отключить QUIC в клиенте либо добавить адресное правило перед остальными:

```yaml
rules:
  - AND,((DOMAIN,example.com),(NETWORK,UDP),(DST-PORT,443)),REJECT
  - AND,((DOMAIN,example.com),(NETWORK,TCP),(DST-PORT,443)),FRAGMENT
  # Далее ваши существующие rules.
```

Это блокирует UDP/443 только для выбранного домена; fallback на TCP зависит от приложения.

## Необязательные socket options

```yaml
sockopt:
  tcp-congestion: bbr       # Linux/Android, если поддерживается ядром
  tcp-user-timeout: 10000   # миллисекунды, Linux/Android
  tcp-keep-alive-idle: 300   # секунды; отрицательное значение выключает keepalive
  tcp-keep-alive-interval: 60
  custom-sockopt:
    - system: linux
      network: tcp
      level: "6"
      opt: "1"             # TCP_NODELAY
      type: int
      value: "1"
```

Блок находится рядом с fragment внутри outbound Freedom и влияет только на его TCP-сокеты. Неподдержанная опция пропускается с предупреждением. Для custom поддерживаются int/str; Windows поддерживает int. На Go до 1.23 keepalive idle и interval нельзя независимо выставить через используемый API: применяется один period (idle, либо interval при idle=0).

Обычные `interface-name`, `routing-mark`, `ip-version`, `tfo`, `mptcp` работают как у DIRECT. Без fragment и sockopt Freedom ведёт себя как DIRECT. Отдельное правило подключения и шифрования на удалённом сервере для TLS-фрагментации обычно не требуется; совместимость и эффективность проверяются на выбранном сервере и сети.

## Проверки порта

Тесты покрывают сохранение TLS payload, длины/число records, SNI split, ClientHello в нескольких Write, применение через relay, полноценный HTTPS через proxy dialer, отмену паузы при Close и неизменный UDP. Пример с адресным правилом проверяется конфигурационным тестом: целевой TCP/443 выбирает Freedom, другой хост/порт, поддомен и QUIC — DIRECT.

Inline fragment дополнительно проверяется захватом ClientHello для VLESS/TLS, REALITY, smux, Xray mux, WS/H2/gRPC/XHTTP, VMess и Trojan. Тест подписки проверяет один узел с REALITY/smux/fragment без внешней зависимости dialer-proxy; ошибочные параметры fragment отклоняются при создании узла.

Для inline-порта тесты затронутых пакетов прошли на Go 1.26.3 и 1.20.14. Один и тот же YAML с REALITY, smux и fragment прошёл CLI-проверку `-t` как в Cheezy, так и в неизменённом vernesong/Mihomo Alpha `1f9b2c2b`: stock parser принимает неизвестное поле fragment и не применяет его.

Проверены Go 1.26.3 и Go 1.20.14 на Windows: fragment, adapter/parser, outbound, outboundgroup, config и существующий Xray mux. Полная сборка проходит; YAML-пример проходит CLI-проверку `-t`. Пакеты fragment/sockopt также кросс-компилируются для Linux и macOS; runtime-проверки socket options на этих ОС в этой работе не выполнялись.
