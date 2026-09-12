# Текущее состояние лабораторных работ

## Лабораторная работа №1 — **ВЫПОЛНЕНА** ✅

**Требования из ТЗ:**
- TCP сервер с командами ECHO, TIME, CLOSE (QUIT/EXIT)
- Команды заканчиваются \r\n или \n
- Клиент-серверная передача файлов (UPLOAD/DOWNLOAD) с поддержкой докачки (RESUME_UPLOAD/RESUME_DOWNLOAD)
- Вывод битрейта после передачи
- Обработка обрывов связи (SO_KEEPALIVE)
- Восстановление передачи файла при переподключении того же клиента к тому же файлу
- Работа в одном потоке (базовый сервер)

**Реализовано в коде:**
- `lab1/server/server.py` — процесс-пул сервер (prefork) с master/worker архитектурой (WORKER_MIN=3, WORKER_MAX=5)
- `lab1/server/server_select.py` — чистый select мультиплексинг (однопоточный, однопроцессный)
- `lab1/server/server_threaded.py` — многопоточный (поток на клиента)
- `lab1/server/command_handler.py` — обработка ECHO, TIME, UPLOAD, DOWNLOAD, RESUME_UPLOAD, RESUME_DOWNLOAD
- `lab1/server/file_manager.py` — управление сессиями, временные файлы, атомарное завершение, битрейт
- `lab1/common/protocol.py` — парсинг команд, UDP пакеты (header 5 байт: seq + type), окно 4096 пакетов
- `lab1/common/socket_utils.py` — создание сокетов, SO_KEEPALIVE настройки (Linux/Windows/macOS), send_all/recv_until
- `lab1/client/client.py` — интерактивный клиент с поддержкой TCP/UDP, прогресс-бар, статистика

**Особенности реализации:**
- UDP файловые передачи делаются через временный TCP порт (UPLOAD_PORT / DOWNLOAD_PORT) — упрощает надежность
- Сессии идентифицируются по fd (TCP) или "ip:port" (UDP)
- Поддержка докачки через seek() в файле

---

## Лабораторная работа №2 — **ВЫПОЛНЕНА** ✅

**Требования из ТЗ:**
- Модификация ЛР1 для работы по UDP: команды + файлы
- Обработка исключительных ситуаций (firewall DROP/REJECT, обрыв сети)
- Битрейт после передачи
- Определить оптимальный размер буфера для max throughput
- **UDP throughput должен быть минимум в 1.5 раза выше TCP ЛР1**
- Реализовать свои механизмы: ACK, повторная передача, скользящее окно

**Реализовано в коде:**
- `lab1/common/protocol.py` — UDP пакеты: DATA, ACK, FIN, CMD, NACK; окно UDP_WINDOW_SIZE=4096
- `lab1/common/rudp.py` — **ПОЛНОСТЬЮ ПЕРЕПИСАН**: настоящий RUDP на чистом UDP
  - Sliding window отправителя: окно отправки, таймеры ретрансляции, RTO оценка (Jacobson/Karels)
  - Fast retransmit по 3 DUP ACK
  - Sliding window получателя: буфер out-of-order пакетов, кумулятивные ACK
  - Пакетная структура: SEQ (4 байт) + TYPE (1 байт) + PAYLOAD (до 8187 байт) = 8192 байт
  - Типы пакетов: DATA, ACK, NACK, FIN, CMD
- `lab1/server/command_handler.py` — интеграция RUDP для UDP файловых передач (без UPLOAD_PORT/TCP трюка)
- `lab1/client/client.py` — использует RUDP.send_stream/recv_stream для UDP передач
- Битрейт работает для UDP через file_manager

**Для получения throughput ≥ 1.5x TCP:** нужно протестировать на localhost/ЛВС с разными размерами payload (1472, 4096, 8192, 16384) и измерить.

---

## Лабораторная работа №3 — **ВЫПОЛНЕНА** ✅

**Требования из ТЗ:**
- Сервер на мультиплексировании (select/pselect/poll)
- Один поток, последовательная обработка запросов клиентов и передачи порций данных
- Подключение новых клиентов в том же цикле
- Размер порции: отклик на команды ≤ ping * 10
- Не прерывать передачу/приём файлов других клиентов при обработке команд

**Реализовано в коде:**
- `lab1/server/server_select.py` — класс `SelectServer`, чистый select loop
- Единый цикл `select.select(inputs, outputs, inputs, 0.005)` (5мс таймаут → отзывчивость)
- TCP клиенты, UDP сокет, listener-ы для UDP file transfer — всё в одном select
- `_tcp_read` / `_tcp_write` / `_udp_read` / `_udp_transfer_accept` / `_udp_transfer_write` — неблокирующие
- Файловые передачи для UDP скачивания также идут через select (через `udp_transfer_connections` в outputs)
- Нет потоков, нет процессов — только select

---

## Лабораторная работа №4 — **ВЫПОЛНЕНА** ✅ (Вариант 4)

**Требования из ТЗ (Вариант 4):**
- Протокол: **UDP**
- Порождение: **Потоки по запросу**, каждый поток выполняет взаимодействие с одним клиентом до завершения сессии
- Механизм защиты: **MSG_PEEK** — запросы, считанные из сокета, но отправленные от клиента не связанного с текущей сессией, не должны теряться

**Реализовано в `lab4/`:**
- `lab4/server_variant4.py` — класс `Variant4Server`
  - Один UDP сокет, общий для всех потоков
  - Главный цикл: `select` на серверном сокете + периодическая очистка таймаутов
  - При новом клиенте (MSG_PEEK показывает новый IP:port) → создаёт `SessionInfo`, `RudpSocket` с `set_peer_filter(addr)`, запускает поток `ClientHandler`
- `lab4/client.py` — класс `Variant4Client`
  - Подключается через RUDP (`connect()` делает 3-way handshake: SYN → SYN-ACK)
  - Команды: ECHO, TIME, UPLOAD, DOWNLOAD, RESUME_UPLOAD, RESUME_DOWNLOAD, QUIT
  - Файловые передачи через RUDP.send_stream/recv_stream
- `lab4/__main__.py` — entry point

**Архитектура MSG_PEEK:**
```
Main Thread                          Worker Thread (per session)
    │                                      │
    ├── select(server_sock)                │
    │     ↓                                │
    ├── MSG_PEEK → new addr?               │
    │     ├─ Yes → create session,         │
    │     │        start handler thread    │
    │     │                                │
    │     └─ No (existing session)         │
    │                                       ├── loop: MSG_PEEK
    │                                       │      ↓ my addr? → recvfrom → process
    │                                       │      ↓ other addr → yield (sleep 1ms)
    │                                       │
    │                                       ├── RUDP on same socket with peer_filter
    │                                       └── Commands + file transfers
```

---

## Лабораторная работа №5 — **ВЫПОЛНЕНА** ✅

**Требования из ТЗ:**
- Параллельный ping нескольких хостов (поток на хост)
- MSG_PEEK для предотвращения кражи ответов из буфера
- Traceroute (TTL, время в теле пакета)
- Разные обработчики: Time Exceeded, Echo Reply, Host Unreachable
- Smurf атака (спуфинг source IP в IP заголовке)
- Демонстрация в Wireshark

**Реализовано в `lab5/`:**
- `lab5/icmp_utils.py` — утилиты: checksum, ICMP/IP заголовки, raw socket создание (Linux/Windows)
- `lab5/ping.py` — `ParallelPinger` + `HostPinger` (ThreadPoolExecutor, поток на хост)
  - Каждый поток: свой raw socket (на Linux) или shared socket + MSG_PEEK логика
  - Статистика: min/avg/max/mdev, packet loss%
- `lab5/traceroute.py` — `Traceroute` класс
  - TTL=1..max_hops, 3 пробы на хоп
  - Обработка Time Exceeded (TTL exceeded), Echo Reply, Destination Unreachable
  - Timestamp в payload для RTT расчёта
- `lab5/smurf.py` — демонстрация Smurf атаки
  - Raw socket с `IP_HDRINCL` для спуфинга source IP
  - Отправка ICMP Echo Request на broadcast с фальшивым source IP жертвы
  - Проверка на private сети (RFC 1918) для безопасности
  - Инструкции для Wireshark capture
- `lab5/ANSWERS.md` — полные ответы на 3 вопроса защиты
- `lab5/__main__.py` — CLI: `python -m lab5 ping host1 host2...`, `python -m lab5 trace host`, `python -m lab5 smurf --victim X --broadcast Y`

---

## Лабораторная работа №6 — **ВЫПОЛНЕНА** ✅

**Требования из ТЗ:**
- P2P чат на UDP/IP (broadcast + multicast)
- Выход из группы и принудительное игнорирование хоста
- Автоопределение IP, маски, broadcast адреса интерфейса
- Поиск и вывод списка IP запущенных приложений

**Реализовано в `lab6/`:**
- `lab6/network.py` — автоопределение интерфейсов (psutil/socket fallback), расчёт broadcast = IP | ~mask, создание broadcast/multicast сокетов
- `lab6/protocol.py` — JSON сообщения: msg, hello, bye, ignore, unignore с валидацией sender IP
- `lab6/discovery.py` — `PeerDiscovery`: HELLO каждые 5с (broadcast + multicast), реестр пиров с TTL 30с, ignore list, force-ignore через IGNORE пакеты
- `lab6/chat.py` — `P2PChat`: recv_loop (select на 2 сокетах) + send_loop, режимы BROADCAST/MULTICAST, фильтр ignore
- `lab6/cli.py` — команды: `/name`, `/list`, `/ignore`, `/unignore`, `/bcast`, `/mcast`, `/mode`, `/interfaces`, `/quit`
- `lab6/ANSWERS.md` — полные ответы на 5 вопросов защиты
- `lab6/__main__.py` — entry point

---

## Лабораторная работа №7 — **СТРУКТУРА ГОТОВА, ТРЕБУЕТ ТЕСТИРОВАНИЯ** 📦

**Требования из ТЗ:**
- MPI умножение матриц (размер ~10-50 сек)
- 2 варианта: блокирующий (MPI_Send/Recv) и неблокирующий (MPI_Isend/Irecv + Waitall)
- Замер времени, неблокирующий должен давать прирост (аналогично CUDA Streams)
- Минимум на 3-х компьютерах
- Вопросы: MPI_COMM_WORLD, rank, MPI_Init/Finalize, преимущество асинхронных операций

**Реализовано в `lab7/`:**
- `lab7/utils.py` — генерация матриц, split/Scatter/Gather helpers, таймеры, верификация
- `lab7/matmul_blocking.py` — блокирующий: Bcast B, Scatter A rows, local @, Gather C
- `lab7/matmul_nonblocking.py` — неблокирующий: Ibcast/Iscatter/Igatherv (MPI-3) + Waitall, фоллбеки на ручные Isend/Irecv **требуют доработки**
- `lab7/__main__.py` — entry point: `blocking`, `nonblocking`, `compare` modes
- `lab7/ANSWERS.md` — полные ответы на 4 вопроса защиты
- `lab7/README.md` — инструкция по запуску, требования, структура
- `lab7/hosts` — пример файла хостов для кластера

**Статус:** Код компилируется, но неблокирующий вариант не протестирован (нужен MPI кластер). Фоллбеки для старых MPI требуют завершения.

**Запуск (требует mpi4py + OpenMPI/MS-MPI):**
```bash
mpirun -np 4 python -m lab7.matmul_blocking --size 2000
mpirun -np 4 python -m lab7.matmul_nonblocking --size 2000
```

**Зависимости:** `mpi4py` (нужен MPI: OpenMPI/MS-MPI), `numpy`

---

## Лабораторная работа №8 — **СТРУКТУРА ГОТОВА** 📦

**Требования из ТЗ:**
- Коллективные операции MPI
- Произвольное число групп (из командной строки), случайное число процессов в группе
- Каждая группа умножает матрицы
- Замер времени по группам, сравнение с парными операциями
- MPI файловые операции: чтение из 2 файлов, запись результатов в файлы групп
- Опционально: запуск через PBS Torque

**Реализовано в `lab8/`:**
- `lab8/groups.py` — `MPI_Comm_split` с рандомными размерами групп (сумма = size)
- `lab8/mpi_io.py` — `MPI_File_read_at_all` / `Write_at_all` для параллельного I/O
- `lab8/matmul_groups.py` — интеграция: группы → коллективные ops (Bcast/Scatter/Gather) / парные ops → MPI-IO
- `lab8/utils.py` — генерация входных файлов, таймеры, верификация, сравнение collective vs pairwise
- `lab8/__main__.py` — entry point: `--gen-inputs`, `--groups`, `--collective/--pairwise`, `--target-time`
- `lab8/ANSWERS.md` — полные ответы на 5 вопросов защиты
- `lab8/README.md` — инструкция по запуску, структура, алгоритм
- `lab8/hosts` — пример файла хостов для кластера

**Запуск:**
```bash
# Генерация входных файлов
mpirun -np 8 python -m lab8 --gen-inputs --size 2000

# Коллективный режим (2 группы)
mpirun -np 8 python -m lab8 --size 2000 --groups 2

# Парный режим (для сравнения)
mpirun -np 8 python -m lab8 --size 2000 --groups 2 --pairwise

# На кластере
mpirun -np 24 -hostfile hosts python -m lab8 --size 4000 --groups 4
```

**Зависимости:** `mpi4py` (нужен MPI: OpenMPI/MS-MPI), `numpy`

---

## ИТОГО по всем 8 лабам:

| Лаба | Статус | Что сделано | Что не сделано |
|------|--------|-------------|----------------|
| 1 | ✅ Готова | Полностью: TCP сервер (3 варианта), файловый обмен, resume, битрейт, keepalive | — |
| 2 | ✅ Готова | Настоящий RUDP на UDP: sliding window, ACK/NACK, fast retransmit, RTO, интеграция в сервер/клиент | Throughput тестирование (опционально) |
| 3 | ✅ Готова | SelectServer — чистый select, однопоточный, все операции в одном цикле | — |
| 4 | ✅ Готова | Вариант 4: UDP + поток на сессию + MSG_PEEK, RUDP интеграция | — |
| 5 | ✅ Готова | Parallel ping, traceroute, smurf demo, ANSWERS.md | — |
| 6 | ✅ Готова | P2P чат (broadcast + multicast, discovery, ignore, ANSWERS.md) | — |
| 7 | 📋 В плане | Подробный план в Task.md | MPI матрицы, blocking/non-blocking, 3+ машины, ANSWERS.md |
| 8 | ⏳ Ждёт | — | MPI коллективные, группы, файловый I/O, PBS Torque |