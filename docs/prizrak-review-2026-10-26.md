# Prizrak-Core: разбор изменений 7–26 октября 2026

Дополнение после разбора: Freedom/fragment и TCP socket options реализованы отдельным портом. Конфигурация и выбор конкретного хоста: [freedom.md](freedom.md). Рекомендации ниже отражают исходный анализ; H2 governor и finalmask остаются за его пределами реализации.

## Вывод для Cheezy Core

1. **uTLS обновлён** до `d5656b84bb5209311c4e3607834ccd481595cb52`: Chrome 155, обновления TLS и исправление UQUICConn. Это отдельное завершённое изменение.
2. **Freedom/fragment стоит рассмотреть первым**: небольшой по сравнению с finalmask блок, пригодный и для прямых HTTPS-соединений, и как `dialer-proxy` под REALITY/TLS. Включать адресно, после проверки на нужной сети.
3. **Наш Mux.Cool сохранять**. В Prizrak шире архитектура адаптера и есть собственный сервер, но нет наших soft-grow, least-loaded, бюджета новых соединений в минуту и асинхронного буфера carrier downlink.
4. **H2 flow governor испытывать отдельно, выключенным по умолчанию**. Это управление HTTP/2 backpressure в XHTTP, а не новый wire-протокол mux. Особенно интересно для большого числа медленных скачиваний.
5. **Finalmask переносить по потребности**, начиная с каркаса и TCP fragment. Полный коммит добавляет около 13,7 тыс. строк с тестами и несколько совершенно разных транспортных механизмов. Для header-custom/sudoku/xmc нужен совместимый сервер; DNS/ICMP-маски требуют отдельной инфраструктуры.

Пункты 2–5 — рекомендации по результатам чтения кода. В этом изменении они не реализованы.

## Зафиксированные версии и метод сравнения

Дата проверки: 26.10.2026. «Вплоть до 07.10» трактуется как просмотр от текущего HEAD назад до 7 октября включительно.

| Репозиторий | Ветка / состояние | SHA |
| --- | --- | --- |
| [Prizrak-Core](https://github.com/legiz-ru/Prizrak-Core/tree/634043dca9455281ae42756e2ccc3b3d29ae2c43) | `v-1-19-33-fm` | `634043dca9455281ae42756e2ccc3b3d29ae2c43` |
| [vernesong/mihomo](https://github.com/vernesong/mihomo/tree/1f9b2c2bfca4cf867967f8266cf609fe7c3d236d) | `Alpha` | `1f9b2c2bfca4cf867967f8266cf609fe7c3d236d` |
| [prizrak-utls](https://github.com/legiz-ru/prizrak-utls/tree/d5656b84bb5209311c4e3607834ccd481595cb52) | `v1.9.0-mod-meta-mldsa-chr155` | `d5656b84bb5209311c4e3607834ccd481595cb52` |
| Cheezy remote | `origin/cheezy-wap` | `20622405` |
| Cheezy local | remote объединён с исходной локальной историей | merge `a1325a7b` |

Использованы полные Git-истории и исходники. Сначала просмотрена first-parent история Prizrak с 07.10, затем проверены уникальные коммиты относительно Alpha и прямой diff двух HEAD. Общий предок Prizrak и Alpha — `462455136157a760937c666bba71afe33b1a809e`; Prizrak влил его 22 октября. Более позднее исправление Alpha от 25 октября по памяти geodata в Prizrak ещё отсутствует. Поэтому полный diff содержит и собственные добавления Prizrak, и отставание от Alpha — считать все его строки новыми функциями нельзя.

Полный diff HEAD к HEAD: 185 файлов, 34 758 добавленных и 323 удалённых строки, включая тесты и функции, появившиеся до 7 октября.

## Коммиты за рассматриваемый период

Даты ниже — даты автора в истории Git, без вывода об очередности по часам внутри одного дня.

| Дата | Коммит | Что изменилось |
| --- | --- | --- |
| 07.10 | [24c7c40d](https://github.com/legiz-ru/Prizrak-Core/commit/24c7c40dd219585e2abe8ce83d93ba025acb9deb) | uTLS на `36b6122`: Chrome 155, синхронизация TLS, исправления decompression, MLKEM1024 |
| 07.10 | [196b4455](https://github.com/legiz-ru/Prizrak-Core/commit/196b44553ac5b1c2dc6bb6d63c2d81127aa8eb47) | Нормализация GREASE при сравнении алгоритмов подписи в тесте REALITY |
| 10.10 | [63121115](https://github.com/legiz-ru/Prizrak-Core/commit/631211159a9485dbaabde5b05387ac4ee32aa865) | H2 governor уровня `mux-ka-6`: ускорение начального роста окна, модель пути по TCP min RTT/read rate, случайные PING |
| 10.10 | [995c1f38](https://github.com/legiz-ru/Prizrak-Core/commit/995c1f380b162521b182003d84ff1f2cdbdfa99e) | Настройки `xhttp-opts.h2-flow`, отдельные лимиты окна stream/connection, поддержка download-settings |
| 10.10 | [c6627771](https://github.com/legiz-ru/Prizrak-Core/commit/c6627771b0f0a14ce90f2a9e7136cb04cb32e297) | Перенос `h2Flow` из VLESS share links в YAML-настройки |
| 14.10 | [29c36c99](https://github.com/legiz-ru/Prizrak-Core/commit/29c36c99d9f1ab1e4d6911498967982d4275ede8) | `mux-ka-8`: согласование двух governors без взаимной остановки; учёт непрочитанного объёма всей connection |
| 14.10 | [d9e1c9ce](https://github.com/legiz-ru/Prizrak-Core/commit/d9e1c9ce1cebaeff8de95e68a6fa8e3369a7ff5d) | `mux-ka-17`: переход к модели пути после устойчивых RTT-раундов, чтобы не обрывать ramp-up слишком рано |
| 22.10 | [1b2e1552](https://github.com/legiz-ru/Prizrak-Core/commit/1b2e155236ee80c8a9566ba41c9bc24775874313) | Merge Alpha до `46245513`: это обновление upstream, не отдельная функция Prizrak |
| 22.10 | [b769dbf7](https://github.com/legiz-ru/Prizrak-Core/commit/b769dbf7f57e87b5ee3477a2e384c06da796a82f) | `mux-ka-20`: серверный учёт очереди, занятой другим трафиком; чтение unacked bytes из TCP_INFO |
| 22.10 | [488280c6](https://github.com/legiz-ru/Prizrak-Core/commit/488280c6401eeec243a166d0c452d51d1699b8ad) | `mux-ka-21`: серверная коррекция ограничения по измеряемому эффекту, новый `h2flow_effect.go` |
| 22.10 | [e85025d6](https://github.com/legiz-ru/Prizrak-Core/commit/e85025d63b19a3f40a8251d619cdcd63a116af0e) | `mux-ka-22`: серверный шаг регулятора ограничен RTT |
| 22.10 | [34f59fa0](https://github.com/legiz-ru/Prizrak-Core/commit/34f59fa0dd92ce25443a326d8731619165e0e8c6) | `mux-ka-24`: эффект считается только по собственному сокращению окна, реально дошедшему до cap |
| 22.10 | [a32f442d](https://github.com/legiz-ru/Prizrak-Core/commit/a32f442d0566b3708f523cf079b2e85232129286) | uTLS на `d5656b8`, включая синхронизацию UQUICConn с QUICConn |
| 24.10 | [64161645](https://github.com/legiz-ru/Prizrak-Core/commit/64161645ba9857b814a8a4edfba3bf3cba45e031) | Новый пакет TCP socket options |
| 24.10 | [355d7467](https://github.com/legiz-ru/Prizrak-Core/commit/355d7467d8a93d526f1fb861f3608a1796ad9aa1) | Outbound `freedom`, фрагментация TCP/TLS, SNI split и ACK wait |
| 26.10 | [634043dc](https://github.com/legiz-ru/Prizrak-Core/commit/634043dca9455281ae42756e2ccc3b3d29ae2c43) | Каркас finalmask, TCP/UDP-маски, VLESS/Trojan-интеграция, тесты совместимости с Xray |

За период — 15 обычных first-parent коммитов и один merge. Собственный Mux.Cool Prizrak появился раньше: основная серия в июле, исправления сборки/тестов в сентябре. Начальный H2 governor и HTTP fork тоже появились до 07.10, в конце сентября.

## Finalmask: устройство и назначение

Это перенесённый из Jolymmiles/Xray-core слой преобразования внешнего трафика. Он находится **ниже TLS/REALITY**, между транспортом и сокетом. Пример направления записи:

```text
VLESS/Trojan → TLS/REALITY → первая TCP-маска → последняя TCP-маска → socket
XHTTP/H3 → QUIC → первая UDP-маска → последняя UDP-маска → сеть
```

Последний элемент YAML-списка ближе всего к сети; внутренние массивы в коде разворачиваются для правильного порядка обёрток. TCP и UDP имеют разные интерфейсы. Некоторые UDP-маски сами открывают соединения (`DialHandler`), поэтому могут стоять только последними в списке. Есть механизм обхода завершивших работу TCP-обёрток для эффективного relay.

Источники: [каркас](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/transport/finalmask/finalmask.go), [реестр масок](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/transport/finalmask/option/option.go), [подключение к outbound](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/adapter/outbound/finalmask.go).

### Реально подключённые пути

- Outbound: только VLESS и Trojan. TCP-маски применяются к TCP-сокету под raw TCP, WS, gRPC, H2/HTTP и XHTTP, где соответствующий transport использует TCP.
- UDP: только VLESS + `network: xhttp` + строго `alpn: [h3]`. Это маска QUIC-сокета до прокси-сервера. Она не является общей маской произвольного UDP-трафика приложений, WireGuard, Hysteria или TUIC.
- Inbound: VLESS/Trojan, только TCP; маска ставится под TLS/REALITY и другие обёртки listener. Встроенного H3 inbound для UDP-масок нет; конфигурация с ними отклоняется.
- В пакете xdns есть серверная реализация для тестов, но подключённого xdns inbound нет. udphop/xicmp — клиентские маски.
- `mkcp-legacy`, `salamander` и `realm` явно не поддерживаются.

### Что делает каждая маска

| Маска | Путь | Что делает / зачем | Условия применения |
| --- | --- | --- | --- |
| `fragment` | TCP | Дробит начальные записи или первый ClientHello на TLS records; задаёт длины и задержки, включая списки по фрагментам | Для клиентского TLS обычно не требует специального сервера; проверить, принимает ли peer дробление |
| `header-custom` | TCP/UDP | Программируемый заголовок/обмен: bytes, random, capture/reuse, вычисления и метаданные | Нужна согласованная конфигурация сторон; внешний HTTP-подобный заголовок сам по себе не создаёт полноценный HTTP transport |
| `sudoku` | TCP/UDP | Перекодирует весь поток/датаграммы по таблицам; в TCP client upload classic, server download packed | Совместимый peer, пароль/таблицы, дополнительный трафик и CPU |
| `xmc` | TCP | Оборачивает поток в Minecraft login/game exchange, использует производный RSA-ключ и подписанные профили | Совпадающие password/profiles на двух сторонах; отдельные затраты handshake и поддержки формата |
| `noise` | UDP | Посылает настроенные шумовые датаграммы до полезного трафика и после reset interval | Получатель должен переносить посторонние датаграммы; это не шифрование и не гарантированный обход DPI |
| `udphop` | UDP | Периодически меняет local socket, remote IP/port или выбирает remote при создании | Несколько доступных адресов/портов должны вести к нужному серверу; проверить QUIC migration и NAT |
| `xdns` | UDP | Переносит датаграммы через DNS-запросы/ответы, с фрагментацией и polling | DNS-зона и совместимый сервер; дополнительные накладные расходы и зависимость от резолверов |
| `xicmp` | UDP | Датаграммы в ICMP echo request/reply | Совместимый сервер, доступность raw/dgram ICMP и нужные права ОС |

Finalmask sudoku и уже имеющийся у нас отдельный `type: sudoku` — разные уровни. Сходный codec не делает их взаимозаменяемыми transport/protocol. Переносить второй Sudoku outbound ради finalmask не требуется.

### Практическая ценность и ограничения

Для фрагментации TLS рациональнее сначала общий `transport/fragment`: его уже используют и freedom, и finalmask.fragment. Header-custom может быть полезен для конкретного сервера или внешнего префикса. Sudoku/xmc интересны при подтверждённой необходимости изменить весь внешний вид потока. Xdns/xicmp — отдельные способы доставки с более дорогой эксплуатацией, а не бесплатное усиление обычного REALITY.

Несколько масок не гарантируют лучший результат: растут overhead, задержки и число взаимодействующих состояний. Обёртки, меняющие весь поток, требуют соответствующей настройки на сервере; обычный Xray с одним REALITY inbound их сам не декодирует.

Перед переносом нужно отдельно проверить сохранение маршрутизации. В частности, **xicmp напрямую вызывает `icmp.ListenPacket` и игнорирует переданный dialer**, в отличие от большинства масок. Поэтому для него нельзя обещать работу `dialer-proxy`, interface binding и routing-mark по общему пути — потребуется отдельная реализация или явно документированное ограничение. Это видно в [xicmp.go](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/transport/finalmask/xicmp/xicmp.go).

Коммит содержит unit/round-trip и Xray interop tests: TCP-транспорты, UDP/H3, REALITY и несовпадение масок. Межпроцессные тесты требуют внешний Xray fork и автоматически пропускаются без `XRAY_BIN`/`XRAY_BINARY`; наличие этих файлов не доказывает проверку всех комбинаций в CI. В этой работе внешняя совместимость finalmask с Xray не запускалась.

## Freedom: что именно портировали

В этой ветке `freedom` — наследник нашего обычного DIRECT с двумя дополнительными TCP-возможностями: `fragment` и `sockopt`. Это не полный перенос всех функций Xray freedom. UDP идёт как у DIRECT, без фрагментации и без применения нового sockopt-блока.

Источники: [freedom.go](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/adapter/outbound/freedom.go), [fragment config](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/transport/fragment/config.go), [запись фрагментов](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/transport/fragment/conn.go).

### Фрагментация

- `packets: tlshello`: собирает первый TLS record, даже если крупный PQ ClientHello пришёл несколькими Write, и дробит его на несколько валидных TLS records.
- `packets: "1-3"`: дробит первые три вызова Write; это не буквальные IP-пакеты. Пустое значение — все записи.
- `split: length`: случайные длины из заданного диапазона; `max-split` ограничивает количество частей.
- `split: sni`: режет внутри меток доменного имени; если подходящего SNI нет, запись проходит без таких разрезов.
- `wait: interval`: паузы между частями. При tlshello и нулевых паузах все новые TLS records могут уйти **одним Write**, поэтому отдельные TCP-сегменты не гарантируются.
- `wait: ack`: ожидает подтверждение между частями. Linux/macOS используют статистику сокета; Windows в этом порте всегда использует fallback-delay (по умолчанию 500 мс). Такое значение на каждый фрагмент легко добавляет заметную задержку.
- После завершения начального преобразования wrapper разрешает relay перейти на исходное соединение.

Это потенциальный обход DPI, ищущего целый ClientHello/SNI в одной записи. Эффективность зависит от конкретного DPI; обход блокировки IP этим не обеспечивается. `length: "1-1"` из примера Prizrak не стоит автоматически брать как наш default: для каждого байта TLS payload создаётся новый record с собственным заголовком.

Freedom можно выбрать правилом для прямого HTTPS или использовать под прокси через `dialer-proxy`. В последнем случае он дробит внешнее TCP/TLS-соединение прокси. Для QUIC/UDP эффекта фрагментации нет.

### Socket options

`tcp-congestion`, `tcp-user-timeout`, TCP keepalive и `custom-sockopt` с числовыми level/opt и int/str. Настройки до connect идут через `ControlContext`; keepalive и custom повторяются после connect, чтобы их не перезаписала настройка Go. Неподдержанная опция не срывает соединение, а даёт предупреждение один раз и затем debug.

На Linux/Android доступны congestion и user-timeout. На Windows они пропускаются; custom string тоже не поддерживается. Keepalive имеет реализации для Go 1.20 и новых Go. Название коммита «per-outbound» шире фактического подключения: новый пакет `component/sockopt` используется здесь только Freedom; общего поля sockopt во всех outbound не добавили.

BBR на клиентском сокете не следует обещать как ускорение всех скачиваний: он меняет управление локальной TCP-отправкой, а не алгоритм отправки удалённого сервера. Включать конкретные опции нужно под измеряемую задачу.

При переносе важны и две небольшие правки групп: Freedom, как Direct, исключается из мест, где перебирают собственно прокси. Одних файлов freedom/fragment недостаточно.

## Mux: три разных механизма

### 1. Sing-mux (`smux`)

Остаётся upstream-механизмом через `github.com/metacubex/sing-mux`, с вариантами протокола и опциями padding/brutal. Он есть в Alpha, Prizrak и Cheezy. Это не wire-совместимая замена Xray Mux.Cool.

### 2. Xray Mux.Cool (`mux.cool` в Prizrak)

У Prizrak это собственный `transport/muxcool` с codec, клиентскими stream/packet session, pool и серверным runtime. Адаптер-обёртка находится на уровне общего parser; физическое соединение открывается через underlying proxy на специальное назначение `v1.mux.cool:9527`. VLESS распознаёт его как native mux command. Общий parser не делает любой прокси-сервер совместимым с Mux.Cool: принимающая сторона должна понимать такой режим.

Есть TCP и XUDP, GlobalID, политика UDP/443 reject/allow/skip, 100 мс ожидания первого payload для объединения с New, ротация после 128 lifetime sessions и закрытие idle carrier через 16 секунд. `smux` и `mux.cool` одновременно запрещены.

На inbound Prizrak заменил `sing-vmess.HandleMuxConnection` своим `ServerRuntime`. Он ведёт XUDP flows по principal/GlobalID, может переживать перенос attachment между carrier и имеет ограничения сессий/очередей. **В Cheezy при этом уже есть Mux.Cool inbound через sing-vmess** — это не отсутствующая у нас функция целиком.

Источники: [обёртка](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/adapter/outbound/muxcool.go), [pool](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/transport/muxcool/pool.go), [server](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/transport/muxcool/server.go).

| Свойство | Prizrak | Cheezy |
| --- | --- | --- |
| Конфигурация | `mux.cool` | `xray-mux` в VLESS |
| Конкурентные сессии | `max-concurrency`, жёсткий предел | `concurrency`, мягкий предел и резерв до ×4 |
| Lifetime sessions/carrier | **`max-connections`**, default 128 | `max-worker-uses`, default 128 |
| Физические carrier | **`max-carriers`**, 0 = unlimited | **`max-connections`**, 0 = pack-first/unlimited |
| Выбор carrier | Первый подходящий; заполняется до предела | Soft-grow при заданном max, затем least-loaded |
| Ограничение новых dials/мин | Нет | `max-dials-per-minute` |
| TCP/XUDP | Могут иметь отдельные pools, общий CarrierLimiter | Общие carrier, независимые soft concurrency и общий бюджет dials |
| Downlink клиента | Очередь 16 сообщений/session + goroutine + net.Pipe | 512 KiB pipe/session + async 256 KiB/carrier |
| Inbound | Собственный runtime | Реализация sing-vmess |
| Область клиента | Общая обёртка адаптера; требуется поддержка peer | Raw TCP VLESS без flow; другие режимы не включают xray-mux |

**Нельзя механически копировать YAML.** Наш `max-connections: 3` ограничивает физические соединения; у них это разрешит всего три lifetime session на carrier и вызовет частые пересоздания. Для похожего физического ограничения у них нужен `max-carriers: 3`, но даже тогда политика роста и использования пула отличается.

Клиентский demux Prizrak читает непосредственно carrier и блокируется на заполненной очереди session. Выделенного async carrier cushion, как у нас, нет. Размер очереди задан числом кадров, а не фиксированным байтовым лимитом. Это не основание объявлять их клиент быстрее или медленнее без сравнимого бенчмарка.

У серверного runtime другая политика: переполненная очередь возвращает `ErrSessionQueueFull` вместо ожидания в общем demux. Это компромисс между изоляцией медленной сессии и её обрывом; переносить такую политику в наш клиент без отдельного решения о backpressure нельзя.

Рекомендация: не заменять наш клиент этим портом. Если нужны дополнительные transports или собственный XUDP server runtime, перенести архитектурное расширение/сервер отдельно, сохранив наш пул, async downlink и ограничения handshakes.

### 3. XHTTP XMUX и H2 flow governor

XMUX/reuse-settings управляет повторным использованием HTTP-соединений и запросов XHTTP; базовый механизм уже есть в Alpha и у нас. Октябрьские `mux-ka-*` меняют **окна и backpressure HTTP/2**, а не Mux.Cool.

Prizrak перенёс governor из Medium1992/mihomo-proxy-ros и синхронизировал его с Medium1992/Xray-core-fork до `mux-ka-24`. В production-клиенте `newFlowClientConn` ставится **после TLS/REALITY, перед HTTP/2 stack**. Он анализирует кадры и регулирует SETTINGS/WINDOW_UPDATE: сколько данных server может отправить до новых credit. Цель — ограничить локально непрочитанные данные, уменьшить память и очереди у медленных readers.

Стартовая/адаптивная модель использует измеренный read rate и RTT; на Linux/Android может читать TCP_INFO сквозь wrappers, на остальных ОС использует PING. Считаются и отдельные streams, и общая connection; исправления `mux-ka-8/17` препятствуют остановке двух governors и слишком раннему завершению роста окна. В production здесь подключена клиентская роль; серверная логика присутствует в общем коде/тестах. Изменения 20/21/22 в самих коммитах прямо отмечены как не меняющие клиентскую роль. Поэтому нельзя приписывать каждый новый тег `mux-ka` ускорению клиента.

Источники: [h2flow.go](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/transport/xhttp/h2flow.go), [опции](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/transport/xhttp/h2flow_options.go), [production wiring](https://github.com/legiz-ru/Prizrak-Core/blob/634043dca9455281ae42756e2ccc3b3d29ae2c43/transport/xhttp/client.go).

```yaml
# Настройки Prizrak; в Cheezy пока не реализованы.
xhttp-opts:
  h2-flow:
    enabled: true
    max-stream-receive-window: 16777216
    max-connection-receive-window: 33554432
```

Governor выключен по умолчанию. Числа — потолки окна при включённом governor, фиксированные окна при выключенном. Допустимо 65535..1 GiB; 0 оставляет defaults HTTP stack. Есть shorthand `h2-flow-control`, а явное `h2-flow.enabled` имеет приоритет. H3 и HTTP/1.1 эту опцию не используют.

Кроме governor Prizrak заменяет `metacubex/http` на `prizrak-http`, ранее ограничив uplink scratch buffer до 32 KiB. Это отдельное изменение; при эксперименте надо разделить его эффект и эффект окон. В Cheezy используется исходный metacubex/http.

В описании коммита `29c36c99` приведён чужой замер: 30 slow readers, 100 Mbit/s, 50 мс, память 48–50 MiB вместо 166 MiB, параллельный запрос 0,15 с вместо 1,1 с при той же пропускной способности. Это заявленный результат автора исходного порта, **не наш замер** и не обещание такого выигрыша на нашей сети.

Рекомендация: отдельный эксперимент для XHTTP/H2; мерить RSS, TTFB параллельного короткого запроса, throughput, CPU и fairness при 1/30 slow readers, RTT 20/80/200 мс, с governor на клиенте/сервере/обоих. На Windows и Linux проверить отдельно, поскольку источник RTT различается. Такой governor не устраняет TCP head-of-line blocking и не меняет число handshakes raw VLESS Mux.Cool.

## Что есть сверх Alpha, но появилось раньше 7 октября

- Собственный Mux.Cool client/server и XUDP, рассмотренные выше.
- Начальный H2 governor и HTTP fork с меньшим scratch buffer.
- Более широкая REALITY/Xray-совместимость, ML-DSA/ML-KEM и вспомогательные настройки; часть уже реализована у нас в предыдущей работе — см. [REALITY notes](reality-mldsa.md).
- Импорт SingBox-конфигураций и расширения share-link converter.
- Пропуск некорректных узлов provider вместо ошибки всего provider.
- Изменения API/названия Prizrak-Box и собственные release/docker assets.

Это дополнительные различия HEAD к HEAD, но не октябрьские нововведения. Вливать всю ветку ради трёх функций нецелесообразно: вместе с ними попадут чужие API/CI, converter и изменения транспортов, а свежий Alpha всё равно нужно сохранять.

## Предлагаемый порядок внедрения

| Шаг | Объём | Условие готовности |
| --- | --- | --- |
| 1. uTLS | Выполнено: pin, sums, GREASE-aware тест, документация | Сборка и TLS/REALITY/mux проверки |
| 2. Freedom + fragment + sockopt | Отдельный небольшой PR; parser, adapter types, исключения групп | Реальный TLS с большим/split ClientHello, TUN, отмена/таймауты, Windows/Linux, проверка dialer-proxy и маршрута |
| 3. H2 governor | Отдельный opt-in PR/эксперимент; converter и download-settings вместе | Сравнимый benchmark и отсутствие deadlock/stall при обычном/modified server |
| 4. Finalmask TCP | Каркас + fragment; header-custom только при нужном сервере | Xray interop, mismatch, timeout/cancel, отсутствие обхода масок relay/splice |
| 5. Другие masks/server runtime | Только по отдельной потребности | Измеренный выигрыш, поддерживаемый сервер, ресурсные лимиты и понятная маршрутизация |

Для нашего сценария ограничения TLS handshakes первым остаётся существующий `xray-mux` с `max-connections` и `max-dials-per-minute`. Freedom и finalmask решают другие задачи. Их не стоит включать всем узлам одновременно или складывать две фрагментации без замера.

## Выполненные локальные изменения и проверки

Remote `origin/cheezy-wap` включён в локальную историю merge-коммитом `a1325a7b`; исходные локальные commits сохранены. Конфликты старого Alpha с новым разрешены в пользу актуального remote для Smart, зависимостей и upstream-lock. Незакоммиченные sync-файлы до начала работы совпадали с remote; их резервная копия сохранена в stash `Backup pre-remote-sync files already present in origin 2026-10-26`. Каталог `Remnawave/` не изменялся.

Обновлены `go.mod`, `go.sum`, `component/tls/reality_test.go`, `docs/reality-mldsa.md`. Нормализация GREASE сохраняет проверку реальных алгоритмов подписи и их порядка. `go mod edit` также убрал дублирующую indirect-строку leaves, уже присутствующую в direct require. Минимальная версия Go в go.mod не менялась.

Проверено на Windows/amd64:

- Go 1.26.3: `go build -buildvcs=false ./...` успешно. `-buildvcs=false` понадобился из-за Git ownership этой рабочей копии; обычные git-команды выполнялись с точечным `safe.directory`, глобальная настройка не менялась.
- Go 1.26.3: тесты `component/tls`, `transport/vmess`, `adapter/outbound`, `adapter/outbound/xraymux`, `listener/reality`, `listener/inbound`, `adapter/outboundgroup`; `component/smart` собирается, собственных тестов пакета нет. Первый запуск выявил GREASE в assertion; после исправления зависимые TLS/outbound/mux тесты повторены успешно.
- Go 1.20.14: `component/tls`, `transport/vmess`, `adapter/outbound/xraymux` проходят.
- На отдельной копии Prizrak: `transport/fragment`, все пакеты `transport/finalmask/...` и `transport/muxcool` проходят; `component/sockopt` на Windows собирается без собственных тестов. Linux-specific socket tests здесь не проверялись.
- Полная `transport/xhttp` suite Prizrak проходит на Go 1.26.3: 226 тестов/подтестов, 232,41 с. Запуск выполнен с общим timeout 300 с; первый запуск с лимитом 120 с не успел закончить suite. Fuzz-проверки здесь ограничены seed cases, отдельная fuzz-кампания не запускалась.
- `go mod verify`: все модули прошли проверку.
- Внешние Xray interop и сетевой benchmark не запускались; выводы о производительности — предложения для проверки, а не измерение этой работы.
