# Lab 6: P2P Chat (Broadcast + Multicast)

Одноранговый чат на UDP с поддержкой широковещательной и многоадресной передачи.

## Возможности
- **Broadcast режим** — обнаружение участников в локальном сегменте (L2)
- **Multicast режим** — эффективная доставка сообщений группе (группа 239.255.0.1, Admin Scope)
- **Автоопределение сети** — IP, маска, broadcast адрес интерфейса
- **Peer Discovery** — периодические HELLO сообщения, автоматическое добавление/удаление участников
- **Ignore список** — локальное игнорирование + принудительное игнорирование через IGNORE пакеты
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
| `/interfaces` | Показать доступные сетевые интерфейсы |
| `/quit` | Выйти из чата |

Обычный текст отправляется как сообщение в чат.

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
  "type": "msg|hello|bye|ignore|unignore",
  "from_ip": "192.168.1.5",
  "from_name": "Alice",
  "text": "Hello!",
  "timestamp": 1699999999.123,
  "seq": 42,
  "target_ip": "192.168.1.10"  // для ignore/unignore
}
```

### Чат (`chat.py`)
- `P2PChat` — recv_loop (select на 2 сокетах) + send_loop
- Режимы: `SendMode.BROADCAST` / `SendMode.MULTICAST`
- Фильтр ignore-листа при получении

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
- `/quit` — выход

## Ответы на вопросы защиты

См. файл `ANSWERS.md` — 5 вопросов про broadcast/multicast.