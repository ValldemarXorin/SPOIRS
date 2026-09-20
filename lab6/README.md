# Lab 6: P2P Chat (Broadcast + Multicast)

Одноранговый чат на UDP с поддержкой широковещательной и многоадресной передачи.

## Возможности
- **Broadcast режим** — обнаружение участников в локальном сегменте (L2)
- **Multicast режим** — эффективная доставка сообщений группе (группа 239.255.0.1, Admin Scope)
- **Автоопределение сети** — IP, маска, broadcast адрес интерфейса
- **Peer Discovery** — периодические HELLO сообщения, автоматическое добавление/удаление участников
- **Ignore список** — локальное игнорирование + принудительное игнорирование через IGNORE пакеты
- **Надёжная доставка (буфер + ретрансляция)** — ACK-подтверждения, адаптивный RTO (Jacobson/Karels), экспоненциальный backoff
- **Fallback для низкоскоростных сетей** — `/slow` или `--slow`: «терпеливый» RTO и больше попыток, чтобы не давить медленный канал
- **CLI интерфейс** — команды для управления

## Запуск

```bash
# Базовый запуск
python -m lab6

# С указанием имени
python -m lab6 -n "Alice"

# С указанием порта и multicast группы
python -m lab6 -p 50000 -g 239.255.0.1

# Принудительный выбор интерфейса
python -m lab6 -i "Ethernet0" --ip 192.168.1.5

# Надёжная доставка: свои параметры RTO
python -m lab6 -n "Alice" --rto 1.0 --max-retries 5 --backoff 2.0

# Fallback для низкоскоростной сети (RTO 5..120с, 12 попыток)
python -m lab6 -n "Alice" --slow

# Отключить надёжность (best-effort, как раньше)
python -m lab6 -n "Alice" --no-reliable

# Справка
python -m lab6 --help
```

## Команды чата

| Команда | Описание |
|---------|----------|
| `/help` | Показать справку |
| `/name <имя>` | Установить отображаемое имя |
| `/list` | Список участников (IP, имя, статус) |
| `/ignore <ip>` | Игнорировать участника (шлёт IGNORE всем) |
| `/unignore <ip>` | Перестать игнорировать |
| `/bcast` | Переключиться в broadcast режим отправки |
| `/mcast` | Переключиться в multicast режим отправки |
| `/mode` | Показать текущий режим |
| `/reliable` | Вкл/выкл ACK-подтверждения и буфер ретрансляции (`/reliable on\|off`) |
| `/slow` | Fallback для низкоскоростной сети (`/slow on\|off`) |
| `/buffer` | Показать сообщения, ожидающие ACK (буфер отправки) |
| `/rto` | Показать статистику RTO/SRTT |
| `/interfaces` | Показать доступные сетевые интерфейсы |
| `/quit` | Выйти из чата |

Обычный текст отправляется как сообщение в чат.

## Надёжная доставка (буфер + ретрансляция)

UDP не гарантирует доставку, поэтому каждое **чат-сообщение**:

1. **Кладётся в буфер** `_pending` вместе со списком участников (из discovery), которым оно адресовано.
2. **Отправляется** broadcast/multicast. Получатель подтверждает доставку **unicast-сообщением `ack`** на IP отправителя (порт тот же).
3. **Ретрансляция**: фоновый поток `_reliability_loop` каждые 200 мс проверяет буфер. Если за `RTO` не пришли ACK от всех участников — сообщение пересылается заново.
4. **Backoff**: после каждой попытки `RTO *= backoff` (по умолчанию ×2), но не больше `max_rto`.
5. **Отказ**: после `max_retries` попыток сообщение удаляется из буфера с пометкой `FAILED` (получатель офлайн).

**Адаптивный RTO** (Jacobson/Karels): при получении каждого ACK замеряется RTT и обновляется `RTO = SRTT + 4·RTTVAR` (границы `min_rto`..`max_rto`). На медленной сети RTT большой → RTO сам растёт.

**Fallback для низкоскоростной сети** — `/slow` или `--slow` переключает пресеты:

| Параметр | Нормальная сеть | Low-throughput |
|----------|-----------------|----------------|
| initial RTO | 1.0 с | 5.0 с |
| min RTO | 0.5 с | 2.0 с |
| max RTO | 30 с | 120 с |
| max retries | 5 | 12 |
| backoff | ×2 | ×2 |

Свои значения можно задать флагами `--rto --min-rto --max-rto --max-retries --backoff` (они сохраняются и при переключении `/slow`).

**Протокол ACK:**
```json
{"type":"ack","from_ip":"192.168.1.5","from_name":"Alice",
 "seq":7,"instance_id":"alice","ack_seq":3,"ack_instance_id":"bob"}
```
`ack_seq`/`ack_instance_id` ссылаются на подтверждаемое сообщение. ACK не дедуплицируется и обрабатывается до проверки ignore-листа, поэтому даже если первый ACK потерялся — повторная ретрансляция сообщения вызовет повторный ACK.

## Архитектура

```
┌─────────────────────────────────────────────────┐
│                   P2PChat                       │
├─────────────┬───────────────────┬───────────────┤
│  Network    │   Discovery       │    CLI        │
│  ────────   │   ───────────     │   ───         │
│  • Bcast    │   • HELLO/5s      │   • Input     │
│  • Mcast    │   • Peer registry │   • Output    │
│  • Auto IP  │   • Ignore list   │   • Commands  │
│  • Sockets  │   • TTL 30s       │   • Colors    │
└─────────────┴───────────────────┴───────────────┘
```

### Сетевой слой (`network.py`)
- `get_interfaces()` — автоопределение через `psutil` (fallback: socket)
- `create_broadcast_socket()` — UDP + `SO_BROADCAST=1`
- `create_multicast_socket()` — UDP + `IP_ADD_MEMBERSHIP` + `IP_MULTICAST_TTL=2`
- `NetworkManager` — высокоуровневая обёртка

### Обнаружение (`discovery.py`)
- `PeerDiscovery` — фоновые потоки: HELLO sender (5s) + cleanup (5s)
- Реестр пиров: `{ip: PeerInfo(name, last_seen, ignored, via_bcast, via_mcast)}`
- Обработка: HELLO, BYE, IGNORE, UNIGNORE
- TTL 30 секунд без HELLO → удаление

### Протокол (`protocol.py`)
```json
{
  "type": "msg|hello|bye|ignore|unignore|ack",
  "from_ip": "192.168.1.5",
  "from_name": "Alice",
  "text": "Hello!",
  "timestamp": 1699999999.123,
  "seq": 42,
  "target_ip": "192.168.1.10",      // для ignore/unignore
  "ack_seq": 42,                    // для ack: seq подтверждаемого сообщения
  "ack_instance_id": "abc"          // для ack: instance подтверждаемого отправителя
}
```

### Чат (`chat.py`)
- `P2PChat` — recv_loop (select на 2 сокетах) + send_loop
- Режимы: `SendMode.BROADCAST` / `SendMode.MULTICAST`
- Фильтр ignore-листа при получении
- **Надёжная доставка**: `_pending` буфер (`PendingMessage`), `_reliability_loop` (ретрансляция + backoff), `_update_rto` (Jacobson/Karels), `_handle_ack`

## Требования

- Python 3.8+
- `psutil` (опционально, для автоопределения интерфейсов) — `pip install psutil`
- `colorama` (опционально, для цветного вывода) — `pip install colorama`

На Windows/Linux работает без дополнительных зависимостей (использует socket fallback).

## Тестирование

Запустите два экземпляра в разных терминалах:

```bash
# Терминал 1
python -m lab6 -n "Alice"

# Терминал 2
python -m lab6 -n "Bob"
```

Оба увидят друг друга через HELLO. Попробуйте:
- Написать сообщения
- `/list` — список участников
- `/ignore <ip_bob>` — Алиса игнорирует Боба
- `/bcast` / `/mcast` — смена режима
- `/buffer` — посмотреть буфер сообщений, ожидающих ACK
- `/rto` — статистика RTO/SRTT
- `/slow` — включить fallback для медленной сети
- `/quit` — выход

## Ответы на вопросы защиты

См. файл `ANSWERS.md` — 5 вопросов про broadcast/multicast.