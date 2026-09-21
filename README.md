# SPOIRS — Лабораторные работы по сетевому программированию и MPI

**Курс:** СPOIRS (Сетевое программирование и организации информационно-решающих систем)  
**Семестр:** 6  
**Вариант ЛР4:** 4 (UDP + поток на сессию + MSG_PEEK)

---

## 📁 Структура проекта

```
SPOIRS/
├── lab1/           # ЛР1-3: TCP/UDP сокеты, файлы, select, RUDP
├── lab4/           # ЛР4: Вариант 4 — UDP сервер с MSG_PEEK
├── lab5/           # ЛР5: ICMP ping, traceroute, Smurf атака
├── lab6/           # ЛР6: P2P чат (broadcast + multicast)
├── lab7/           # ЛР7: MPI умножение матриц (blocking/non-blocking)
├── lab8/           # ЛР8: MPI группы, коллективные операции, MPI-IO
├── doc/            # Техническое задание (PDF)
├── agent/          # Отчёты о прогрессе
└── README.md       # Этот файл
```

---

## 🚀 Быстрый запуск

### Требования
```bash
# Базовые (ЛР1-6)
pip install numpy psutil colorama

# Для ЛР7-8 (MPI)
# Linux:
sudo apt-get install openmpi-bin libopenmpi-dev python3-dev
pip install mpi4py

# Windows:
# 1. Установить MS-MPI с https://docs.microsoft.com/en-us/message-passing-interface/microsoft-mpi
#    (важно! без него mpi4py не работает — ошибка "Could not find module 'msmpi.dll'")
# 2. pip install mpi4py
```

### Установка всех зависимостей разом
```bash
pip install -r requirements.txt
```

> **Важно:** все команды запускаются **из корня проекта** (`D:\6sem\SPOIRS`).
> Каждая папка `labX/` — Python-пакет, поэтому используются команды `python -m labX...`.

---

## 📋 Лабораторные работы

### [ЛР1-3](./lab1/) — TCP/UDP сокеты, файлы, мультиплексирование
| Файл | Описание |
|------|----------|
| `run_server.py` | Process pool сервер (prefork, WORKER_MIN=3, MAX=5) |
| `run_server_select.py` | **ЛР3** — чистый select, 1 поток, 5мс таймаут |
| `run_server_threaded.py` | Многопоточный (поток на клиента) |
| `run_client.py` | Интерактивный клиент (TCP/UDP, прогресс-бар) |
| `common/rudp.py` | **ЛР2** — настоящий RUDP на UDP (sliding window, ACK/NACK, RTO) |

**Команды:** `ECHO`, `TIME`, `UPLOAD`, `DOWNLOAD`, `RESUME_UPLOAD`, `RESUME_DOWNLOAD`, `QUIT`

**Запуск:**
```bash
# Терминал 1
python -m lab1.run_server

# Терминал 2
python -m lab1.run_client
# > UPLOAD testfile_500mb.dat --udp
# > DOWNLOAD testfile_500mb.dat --udp
```

---

### [ЛР4](./lab4/) — Вариант 4: UDP + поток на сессию + MSG_PEEK
**Требование:** Один UDP сокет, для каждого клиента свой поток, MSG_PEEK для разделения трафика.

| Файл | Описание |
|------|----------|
| `server_variant4.py` | Главный цикл: select + MSG_PEEK → новые клиенты → потоки |
| `client.py` | Клиент для варианта 4 (RUDP handshake) |

**Архитектура:**
```
Main Thread                    Worker Thread (per session)
    │                                │
    ├── select(server_sock)          │
    │     ↓ MSG_PEEK → new addr?     │
    │     ├─ Yes → create thread     │
    │     └─ No (existing)           │
    │                                ├── loop: MSG_PEEK
    │                                │      ↓ my addr? → recvfrom → process
    │                                │      ↓ other → yield
    │                                │
    │                                ├── RUDP on same socket + peer_filter
    │                                └── Commands + file transfers
```

**Запуск:**
```bash
# Терминал 1
python -m lab4

# Терминал 2
python -m lab4.client
# > UPLOAD testfile_500mb.dat
# > DOWNLOAD testfile_500mb.dat
```

---

### [ЛР5](./lab5/) — ICMP: Ping, Traceroute, Smurf
**Требование:** Raw sockets, ICMP, MSG_PEEK для параллельного ping, Smurf demo.

| Файл | Описание |
|------|----------|
| `icmp_utils.py` | Raw sockets, checksum, ICMP/IP заголовки (Linux/Windows) |
| `ping.py` | `ParallelPinger` — ThreadPoolExecutor, поток на хост |
| `traceroute.py` | TTL=1..30, 3 пробы/хоп, Time Exceeded/Echo Reply/Dest Unreachable |
| `smurf.py` | Smurf атака (IP_HDRINCL, спуфинг source IP) — **только в изолированной сети!** |
| `ANSWERS.md` | Ответы на 3 вопроса защиты |

**Запуск (требует root/Admin):**
```bash
# Parallel ping
sudo python -m lab5 ping 8.8.8.8 1.1.1.1 ya.ru -c 10

# Traceroute
sudo python -m lab5 trace google.com

# Smurf инструкции для Wireshark
python -m lab5 smurf --wireshark-help

# Smurf атака (ТОЛЬКО В ТЕСТОВОЙ СЕТИ!)
sudo python -m lab5 smurf --victim 192.168.1.10 --broadcast 192.168.1.255
```

> **⚠️ Windows limitation:** на Windows raw ICMP сокеты игнорируют `IP_TTL`, а
> `IPPROTO_RAW`+`IP_HDRINCL` (для самостоятельной сборки IP-заголовка) часто
> заблокированы. Поэтому **ping работает, а traceroute может не показать хопы**.
> Для полной демонстрации traceroute используйте Linux.

---

### [ЛР6](./lab6/) — P2P чат: Broadcast + Multicast
**Требование:** P2P чат, автоопределение IP/маски/broadcast, discovery, ignore list.
Логика из эталонного `lr6.py`.

| Файл | Описание |
|------|----------|
| `network.py` | IP через маршрут по умолчанию, broadcast = IP \| ~mask + спец-случай хотспота 172.20.10.x (/28), сокеты |
| `chat.py` | `P2PChat` — PING discovery (2.5с), send/receive, ignore, join/leave multicast |
| `cli.py` | Команды: `/mode b\|m`, `/peers`, `/ignore`, `/unignore`, `/join`, `/leave`, `/exit` + argparse |
| `ANSWERS.md` | Ответы на 5 вопросов защиты |

**Команды чата:**
```
/mode b|m       /peers          /ignore <ip>
/unignore <ip>  /join           /leave
/exit
```

**Запуск:**
```bash
# Терминал 1
python -m lab6 -n "Alice"

# Терминал 2
python -m lab6 -n "Bob"

# Тестирование ignore
# В Alice: /ignore 192.168.1.5  (IP Боба)
# Сообщения Боба перестанут отображаться у Алисы
```

---

### [ЛР7](./lab7/) — MPI: Умножение матриц (Blocking / Non-blocking)
**Требование:** 2 варианта, замер времени, неблокирующий быстрее (как CUDA Streams), 3+ машины.
Логика из эталонного `lr7.py`.

| Файл | Описание |
|------|----------|
| `matmul_blocking.py` | Блокирующий: Send(B), поштучная Send/Recv чанков A/C |
| `matmul_nonblocking.py` | Неблокирующий конвейер: Isend/Irecv, double buffering, prefetch |
| `__main__.py` | Entry point: `blocking` / `nonblocking` / `compare` |
| `ANSWERS.md` | Ответы на 4 вопроса: MPI_COMM_WORLD, rank, Init/Finalize, async advantage |
| `hosts` | Пример файла хостов для кластера |

**Алгоритм (chunked pipeline, NUM_CHUNKS=6):**
```
Rank 0: A(N×N), B(N×N)
    │
    ├─ Send(B) → все воркеры
    ├─ для каждого чанка: Send(кусок A воркеру) → Recv(кусок C)
    │
    └─ Воркер: Recv(B) → для каждого чанка: Recv(A_chunk) → C_chunk = A_chunk @ B → Send(C_chunk)
```
Неблокирующий вариант перекрывает приём следующего чанка с вычислением текущего
(двойная буферизация + prefetch).

**Запуск (нужно ≥3 процесса):**
```bash
# Блокирующий (4 процесса)
mpirun -np 4 python -m lab7.matmul_blocking --size 1400

# Неблокирующий конвейер
mpirun -np 4 python -m lab7.matmul_nonblocking --size 1400

# Сравнение обоих + прирост скорости
mpirun -np 4 python -m lab7 compare --size 1400

# На кластере (12 процессов, 3 узла)
mpirun -np 12 -hostfile hosts python -m lab7.matmul_nonblocking --size 1400
```

**Ожидаемый прирост:** 10-30% на неблокирующем (перекрытие comm + compute).

---

### [ЛР8](./lab8/) — MPI: Группы, коллективные операции, MPI-IO
**Требование:** Случайные группы (Comm_split), коллективные ops, MPI-IO, сравнение с парными.
Логика из эталонного `lr8.py`.

| Файл | Описание |
|------|----------|
| `matmul_groups.py` | Вся логика: генерация файлов, случайные группы, MPI-IO, сравнение |
| `__main__.py` | Entry point (`from lab8.matmul_groups import main`) |
| `ANSWERS.md` | Ответы на 5 вопросов: Comm_split, коллективные vs парные, MPI_File, Wtime |
| `hosts` | Пример файла хостов |

**Алгоритм:**
```
1. MPI_COMM_WORLD (size)
       │
       ▼
2. Генерация shared_matrix_A.bin / shared_matrix_B.bin (rank 0)
       │
       ▼
3. Случайное деление на группы (каждая >=1 процесс)
       │
       ▼
4. MPI_Comm_split(color=group_id, key=rank)
       │
       ├─ MPI_File.Read_at_all — каждый читает свой срез A
       ├─ Bcast(B) внутри группы
       ├─ C_sub = A_sub @ B
       └─ MPI_File.Write_at_all → result_group_<id>.bin
              │
              ▼
5. MPI_Wtime замер (reduce MAX) + замер парных Send/Recv для сравнения
```

**Запуск (процессов ≥ групп):**
```bash
# 8 процессов, 2 группы, матрицы 1200x1200
mpirun -np 8 python -m lab8 --dim 1200 --groups 2

# На кластере (24 процесса, 4 группы)
mpirun -np 24 -hostfile hosts python -m lab8 --dim 1200 --groups 4
```

---

## 🧪 Как тестировать и что показывать на защите

### ЛР5 (ICMP)
| Что тестировать | Как показать |
|----------------|--------------|
| Parallel ping нескольких хостов | Запустить `python -m lab5 ping 8.8.8.8 1.1.1.1 ya.ru` — показать одновременный вывод статистики |
| Traceroute | `python -m lab5 trace google.com` — показать хопы с 3 RTT |
| Smurf | Wireshark capture: ICMP Echo Request на broadcast → множество Reply на victim IP |
| MSG_PEEK | Объяснить в коде `ping.py`: каждый поток свой raw socket (Linux) или общий + MSG_PEEK |

### ЛР6 (P2P Chat)
| Что тестировать | Как показать |
|----------------|--------------|
| Broadcast discovery | 2 терминала → оба видят друг друга через PING (~2.5с) |
| Multicast join/leave | `/mode m` → `/join` → `/leave` |
| Ignore list | `/ignore <ip>` — сообщения исчезают, `/unignore` — возвращаются |
| Передача сообщений | Текст → `[Name @ IP]: текст` на другом хосте |
| ANSWERS.md | Ответы на 5 вопросов (отличие bcast/mcast, формирование broadcast, диапазоны multicast, ограничения, область broadcast) |

### ЛР7 (MPI Matrix)
| Что тестировать | Как показать |
|----------------|--------------|
| Блокирующий вариант | `mpirun -np 4 python -m lab7.matmul_blocking --size 1400` — показать время |
| Неблокирующий вариант | `mpirun -np 4 python -m lab7.matmul_nonblocking --size 1400` — показать время |
| Сравнение | `mpirun -np 4 python -m lab7 compare --size 1400` — итог с приростом скорости % |
| Неблокирующий конвейер | Показать Isend/Irecv + double buffering + prefetch в `matmul_nonblocking.py` |
| ANSWERS.md | 4 вопроса: MPI_COMM_WORLD, rank, Init/Finalize, преимущество async (CUDA Streams analogy) |

### ЛР8 (MPI Groups + IO)
| Что тестировать | Как показать |
|----------------|--------------|
| Генерация общих файлов | rank 0 создаёт shared_matrix_A.bin / shared_matrix_B.bin при запуске |
| Случайные группы | `mpirun -np 8 python -m lab8 --dim 1200 --groups 2` — показать группы |
| MPI-IO | Показать `Read_at_all` / `Write_at_all` в `matmul_groups.py` |
| Коллективные ops | `Bcast(B)` внутри группы, `reduce(MAX)` времени |
| Сравнение времени | Итог: парные Send/Recv vs коллективные + MPI-IO; проверка файлов на диске |
| ANSWERS.md | 5 вопросов: Comm_split, коллективные vs парные, MPI_File, Wtime, преимущества коллективных |

---

## 📖 Теория: Вопросы из ТЗ (PDF)

### ЛР5 — ICMP
1. **Traceroute:** TTL=1..N, каждый роутер декрементирует, при TTL=0 → ICMP Time Exceeded (type=11, code=0). Timestamp в payload → RTT. Остановка при Echo Reply от цели.
2. **Smurf:** IP spoofing (source IP = victim) + ICMP Echo Request на broadcast. Все хосты отвечают на victim → amplification. Защита: `no ip directed-broadcast`, ingress filtering (BCP 38), rate limiting ICMP.
3. **IP заголовок:** Version, IHL, DSCP, Total Length, ID, Flags, Fragment Offset, TTL, Protocol, Checksum, Src/Dst IP, Options.

### ЛР6 — Broadcast/Multicast
1. **Отличие:** Broadcast — все в L2 сегменте (обязательно). Multicast — только подписчики группы (IGMP join), маршрутизируется (PIM), экономит трафик.
2. **Broadcast адрес:** IP \| ~Netmask (побитовое ИЛИ с инвертированной маской).
3. **Диапазоны multicast:** 224.0.0.0/24 (link-local), 224.0.1.0/24 (internetwork), 232.0.0.0/8 (SSM), 239.0.0.0/8 (admin scope).
4. **Ограничение области:** TTL (IP_MULTICAST_TTL), admin scoping (239.x), multicast routing (PIM, IGMP snooping), scope boundaries.
5. **Область broadcast:** Только L2 сегмент (не проходит роутеры). Directed broadcast может если разрешён на роутере.

### ЛР7 — MPI Basics
1. **MPI_COMM_WORLD:** Предопределённый коммуникатор со всеми процессами. Создаётся при MPI_Init. Rank 0..size-1.
2. **Rank:** Уникальный ID процесса в коммуникаторе (0..size-1). Для адресации в Send/Recv, распределения работы.
3. **Init/Finalize:** `MPI_Init`/`MPI_Finalize` (C) или `MPI.Init()`/`MPI.Finalize()` (Python). Все MPI вызовы только между ними.
4. **Async advantage:** Перекрытие коммуникации и вычислений (overlap). Пока сеть передаёт — CPU считает. Аналогично CUDA Streams: `cudaMemcpyAsync` + kernel.

### ЛР8 — MPI Groups + Collective + IO
1. **MPI_Comm_split:** Разделяет коммуникатор по `color` (group_id). `key` определяет ранг в новой группе. Возвращает `newcomm`.
2. **Коллективные vs парные:** Коллективные — все процессы группы, оптимизированные алгоритмы (tree: O(log P)), неявный барьер. Парные — 2 процесса, ручное управление.
3. **MPI_File_read_at/write_at:** Позиционированные I/O с явным offset (в байтах). Коллективные версии `_all` агрегируют запросы. File view для сложных паттернов.
4. **MPI_Wtime:** High-resolution wall-clock timer. Портативный, монотонный, для замеров производительности.
5. **Преимущества коллективных:** Tree algorithms (O(log P) vs O(P)), сетевая эффективность, меньше кода, неявная синхронизация, совместимость с MPI-IO.

---

## 🔧 Неочевидные места в коде

### ЛР2: RUDP (`lab1/common/rudp.py`)
- **MPI-3 фоллбеки:** `hasattr(comm, 'Ibcast')` проверяет наличие неблокирующих коллективов. Если нет — используется blocking fallback.
- **Sliding window:** `UDP_WINDOW_SIZE=4096` пакетов × 8KB ≈ 32MB in flight.
- **RTO estimation:** Jacobson/Karels алгоритм (`_update_rto`).
- **Fast retransmit:** 3 DUP ACK → немедленная ретрансляция + уменьшение окна.

### ЛР4: MSG_PEEK (`lab4/server_variant4.py`)
```python
# Main thread делает MSG_PEEK
data, addr = sock.recvfrom(65536, socket.MSG_PEEK)
if addr not in sessions:
    create_new_session(addr)  # Новый поток
# Worker thread для своего addr делает recvfrom (consumes)
```

### ЛР5: Raw Sockets (`lab5/icmp_utils.py`)
- **Linux:** `socket(AF_INET, SOCK_RAW, IPPROTO_ICMP)` — ядро строит IP заголовок
- **Windows:** `socket(AF_INET, SOCK_RAW, IPPROTO_IP)` + `IP_HDRINCL=1` — строим IP заголовок сами
- **Checksum:** RFC 1071 (16-битные слова, carry-around)

### ЛР7-8: MPI (`lab7/utils.py`, `lab8/mpi_io.py`)
- **MPI.Wtime()** вместо `time.time()` — синхронизировано в MPI
- **MPI-IO offset в байтах:** `displs[rank] * n_cols * element_size`
- **Коллективные версии `_all`:** `Write_at_all`, `Read_at_all` — MPI агрегирует I/O
- **File view:** Можно задать через `MPI_File_Set_view` для сложных паттернов

---

## 📦 Зависимости

```bash
# Обязательные
pip install numpy

# Опциональные (для ЛР6)
pip install psutil colorama

# Для ЛР7-8 (MPI)
# Linux:
sudo apt-get install openmpi-bin libopenmpi-dev python3-dev
pip install mpi4py

# Windows:
# 1. MS-MPI SDK
# 2. pip install mpi4py
```

---

## 📝 Git / GitHub

```bash
# Инициализация (если не сделано)
git init
git add .
git commit -m "Initial commit: all 8 labs complete"

# Подключение remote (замените на ваш репозиторий)
git remote add origin https://github.com/USERNAME/SPOIRS.git
git branch -M main
git push -u origin main
```

---

## 📚 Дополнительные материалы

- `agent/currentState.md` — детальный статус каждой лабы
- `agent/Task.md` — планы и задачи по каждой лабе
- `doc/Споирс_ЛР (2).pdf` — оригинальное ТЗ
- Каждая лаба имеет свой `ANSWERS.md` с ответами на вопросы защиты
- Каждая лаба имеет свой `README.md` с инструкцией по запуску

---

**Готово к сдаче!** 🎉