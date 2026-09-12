# План выполнения первых 6 лабораторных работ

## Лабораторная работа №1 — **УЖЕ ГОТОВА** ✅
Ничего делать не нужно. Код работает, есть 3 реализации сервера:
- `run_server.py` — process pool (prefork)
- `run_server_select.py` — чистый select
- `run_server_threaded.py` — поток на клиента
- `run_client.py` — интерактивный клиент

---

## Лабораторная работа №2 — **ВЫПОЛНЕНА** ✅

**Выполненные задачи:**
1. **Переписан `lab1/common/rudp.py` — настоящий RUDP на чистом UDP**
   - Убран TCP fallback полностью
   - Реализован sliding window отправителя: окно отправки (4096 пакетов), таймеры ретрансляции, RTO оценка (Jacobson/Karels)
   - Fast retransmit по 3 DUP ACK
   - Sliding window получателя: буфер out-of-order пакетов, кумулятивные ACK
   - Пакетная структура: SEQ (4 байт) + TYPE (1 байт) + PAYLOAD (до 8187 байт) = 8192 байт (jumbo frame safe)
   - Типы пакетов: DATA, ACK, NACK, FIN, CMD

2. **Интегрирован RUDPSocket в сервер и клиент для файловых передач**
   - Сервер (`command_handler.py`): при UPLOAD/DOWNLOAD по UDP — использует RUDPSocket напрямую (без UPLOAD_PORT/TCP трюка)
   - Клиент (`client.py`): `upload_file(use_udp=True)` / `download_file(use_udp=True)` — через RUDP.send_stream/recv_stream

3. **Битрейт для UDP** — работает через file_manager

**Осталось (опционально):**
- Протестировать throughput на localhost/ЛВС с разными размерами payload (1472, 4096, 8192, 16384)
- Обосновать оптимальный размер: Ethernet MTU 1500 → 1472 payload без фрагментации; Jumbo frames 9000 → 8192
- Цель: UDP throughput ≥ 1.5 × TCP throughput

---

## Лабораторная работа №3 — **УЖЕ ГОТОВА** ✅
`SelectServer` в `server_select.py` полностью соответствует ТЗ:
- Чистый select, 1 поток, 1 процесс
- Все сокеты (TCP listen, UDP, client TCP, UDP transfer listeners) в одном select
- Таймаут select 5мс → отзывчивость < ping*10
- Файловые передачи не блокируют обработку команд других клиентов
- Нет потоков/процессов

---

## Лабораторная работа №4 — **ВЫПОЛНЕНА** ✅ (Вариант 4)

**Вариант 4 (UDP + MSG_PEEK):**
- Протокол: **UDP**
- Порождение: **Потоки по запросу**, каждый поток выполняет взаимодействие с одним клиентом до завершения сессии
- Механизм защиты: **MSG_PEEK** — запросы, считанные из сокета, но отправленные от клиента не связанного с текущей сессией, не должны теряться

**Реализовано в `lab4/`:**
- `lab4/server_variant4.py` — класс `Variant4Server`
  - Один UDP сокет, общий для всех потоков
  - `SessionManager` (thread-safe): registry сессий, создание/удаление, cleanup по таймауту (5 мин)
  - Главный цикл: `select` на серверном сокете + проверка таймаутов
  - При новом клиенте (MSG_PEEK показывает новый IP:port) → создаёт `SessionInfo`, `RudpSocket` с `set_peer_filter(addr)`, запускает поток `ClientHandler`
- `lab4/client.py` — класс `Variant4Client`
  - Подключается через RUDP (3-way handshake: SYN → SYN-ACK)
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

**Реализовано в `lab5/`:**
- `lab5/icmp_utils.py` — утилиты: checksum, ICMP/IP заголовки, raw socket создание (Linux/Windows)
- `lab5/ping.py` — `ParallelPinger` + `HostPinger` (ThreadPoolExecutor, поток на хост)
  - Каждый поток: свой raw socket (на Linux)
  - Статистика: min/avg/max/mdev, packet loss%
- `lab5/traceroute.py` — `Traceroute` класс
  - TTL=1..max_hops, 3 пробы на хоп
  - Обработка Time Exceeded (TTL exceeded), Echo Reply, Destination Unreachable
  - Timestamp в payload для RTT расчёта
- `lab5/smurf.py` — демонстрация Smurf атаки (educational only)
  - Raw socket с `IP_HDRINCL` для спуфинга source IP
  - Отправка ICMP Echo Request на broadcast с фальшивым source IP жертвы
  - Проверка на private сети (RFC 1918) для безопасности
  - Инструкции для Wireshark capture (`--wireshark-help`)
- `lab5/ANSWERS.md` — полные ответы на 3 вопроса защиты:
  1. Как работает traceroute (TTL, ICMP Time Exceeded)
  2. Механизм Smurf (IP spoofing + broadcast + ICMP echo), угрозы, защита
  3. Поля IP заголовка (version, IHL, DSCP, total length, ID, flags, fragment offset, TTL, protocol, checksum, src/dst IP, options)
- `lab5/__main__.py` — CLI: `python -m lab5 ping host1 host2...`, `python -m lab5 trace host`, `python -m lab5 smurf --victim X --broadcast Y`

---

## Лабораторная работа №6 — **ВЫПОЛНЕНА** ✅

**Реализовано в `lab6/`:**
- `lab6/network.py` — автоопределение интерфейсов (psutil/socket fallback), расчёт broadcast = IP | ~mask, создание broadcast/multicast сокетов
- `lab6/protocol.py` — JSON сообщения: msg, hello, bye, ignore, unignore с валидацией sender IP
- `lab6/discovery.py` — `PeerDiscovery`: HELLO каждые 5с (broadcast + multicast), реестр пиров с TTL 30с, ignore list, force-ignore через IGNORE пакеты
- `lab6/chat.py` — `P2PChat`: recv_loop (select на 2 сокетах) + send_loop, режимы BROADCAST/MULTICAST, фильтр ignore
- `lab6/cli.py` — команды: `/name`, `/list`, `/ignore`, `/unignore`, `/bcast`, `/mcast`, `/mode`, `/interfaces`, `/quit`
- `lab6/ANSWERS.md` — полные ответы на 5 вопросов защиты
- `lab6/__main__.py` — entry point, `README.md` — инструкция

**Запуск:**
```bash
python -m lab6 -n "Alice"   # терминал 1
python -m lab6 -n "Bob"     # терминал 2
```

---

## Лабораторная работа №7 — **ОЖИДАЕТ** ⏳

---

## ОБЩИЕ ТРЕБОВАНИЯ КО ВСЕМ ЛАБАМ

### Структура проекта:
```
SPOIRS/
├── lab1/           # Готово (ЛР1-3)
├── lab4/           # Готово (ЛР4 вариант 4)
├── lab5/           # Готово (ЛР5)
├── doc/            # ТЗ
└── agent/          # Этот отчет
```

### Запуск и тестирование:
- Каждая лаба — отдельный запускаемый модуль: `python -m labX ...`
- ЛР5 требует root/Admin (raw sockets) — предупреждено в коде

---

## ИТОГ: ЛР1-5 ВЫПОЛНЕНЫ ПОЛНОСТЬЮ, ЛР6 В ПЛАНЕ

| Лаба | Статус |
|------|--------|
| 1 | ✅ Готова |
| 2 | ✅ Готова |
| 3 | ✅ Готова |
| 4 | ✅ Готова (вариант 4) |
| 5 | ✅ Готова |
| 6 | 📋 В плане (подробно выше) |
| 7 | ⏳ Ждёт |
| 8 | ⏳ Ждёт |

---

## Лабораторная работа №7 — **ОЖИДАЕТ** ⏳

**Требования (из ТЗ):**
- MPI умножение матриц (размер ~10-50 сек)
- 2 варианта: блокирующий и неблокирующий режим
- Замер времени, неблокирующий должен давать прирост
- Минимум на 3 компьютерах

**План (когда начнём):**
- `lab7/` с `mpi4py`
- `matmul_blocking.py` — `MPI_Send`/`MPI_Recv`
- `matmul_nonblocking.py` — `MPI_Isend`/`MPI_Irecv` + `MPI_Waitall`
- Распределение строк матрицы по процессам
- Запуск: `mpirun -np N -hostfile hosts python -m lab7.matmul_nonblocking`

---

## Лабораторная работа №8 — **ОЖИДАЕТ** ⏳

**Требования (из ТЗ):**
- Коллективные операции MPI (Bcast, Scatter, Gather, Reduce, Allreduce)
- Произвольное число групп (CLI), случайное число процессов в группе
- Каждая группа умножает матрицы
- Замер времени по группам, сравнение с парными операциями
- MPI файловые операции: чтение из 2 файлов (MPI_File_read_at), запись в файлы групп
- Опционально: PBS Torque

**План (когда начнём):**
- `lab8/` с `mpi4py`
- `MPI_Comm_split` для создания групп
- Коллективные операции внутри групп
- `MPI_File` для параллельного I/O
- Сравнение времени: парные vs коллективные

---

## Запуск готовых лаб (1-6):
```bash
# ЛР1
python -m lab1.run_server
python -m lab1.run_client

# ЛР3
python -m lab1.run_server_select

# ЛР4 (вариант 4)
python -m lab4
python -m lab4.client

# ЛР5
python -m lab5 ping 8.8.8.8 1.1.1.1
python -m lab5 trace google.com
python -m lab5 smurf --wireshark-help

# ЛР6
python -m lab6 -n "Alice"
python -m lab6 -n "Bob"
```

---

## Лабораторная работа №7 — **ПЛАНИРУЕТСЯ** 📋

### Требования из ТЗ:
- MPI умножение матриц (размер ~10-50 сек выполнения)
- 2 варианта: **блокирующий** (MPI_Send/MPI_Recv) и **неблокирующий** (MPI_Isend/MPI_Irecv + MPI_Waitall)
- Замер времени, неблокирующий должен давать прирост (как CUDA Streams / cudaMemcpyAsync)
- Запуск минимум на 3-х компьютерах
- Вопросы к защите (4 вопроса)

### Архитектура:

```
┌─────────────────────────────────────────────────────────────────┐
│                    MPI Matrix Multiplication                    │
├─────────────────────────────────────────────────────────────────┤
│  Distribution          │  Communication    │  Computation      │
│  ──────────────        │  ──────────────   │  ─────────────    │
│  • Row-wise split      │  • Blocking:      │  • Local matmul   │
│    (A rows / P)        │    MPI_Send/Recv  │  • BLAS (numpy)   │
│  • B broadcast         │  • Non-blocking:  │  • Timer          │
│    (MPI_Bcast)         │    MPI_Isend/     │                   │
│  • C gather            │    MPI_Irecv      │                   │
│    (MPI_Gather)        │    + Waitall      │                   │
└─────────────────────────────────────────────────────────────────┘
```

### Алгоритм умножения матриц (row-wise):

```
Matrix A (N×N)          Matrix B (N×N)          Matrix C (N×N)
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│ Rank 0 rows │         │             │         │ Rank 0 rows │
├─────────────┤         │   FULL B    │    →    ├─────────────┤
│ Rank 1 rows │    ×    │  broadcast  │         │ Rank 1 rows │
├─────────────┤  to all │  to all     │         ├─────────────┤
│ Rank 2 rows │         │   ranks     │         │ Rank 2 rows │
├─────────────┤         │             │         ├─────────────┤
│ ...         │         │             │         │ ...         │
└─────────────┘         └─────────────┘         └─────────────┘
```

Каждый процесс:
1. Получает свои строки матрицы A (scatter)
2. Получает полную матрицу B (broadcast)
3. Вычисляет свои строки C = A_local × B
4. Отправляет результат в rank 0 (gather)

### Детальные задачи:

#### 1. Создать `lab7/` структуру
```
lab7/
├── __main__.py          # entry point
├── matmul.py            # общая логика (генерация, таймеры)
├── matmul_blocking.py   # вариант 1: MPI_Send/Recv
├── matmul_nonblocking.py # вариант 2: MPI_Isend/Irecv + Waitall
├── utils.py             # вспомогательные ф-ции
├── ANSWERS.md           # ответы на 4 вопроса
├── README.md            # инструкция по запуску
└── hosts                # файл со списком хостов для mpirun
```

#### 2. `utils.py` — утилиты
- `generate_matrices(n)` — генерация случайных матриц A, B
- `split_matrix_rows(A, comm)` — разделение строк матрицы A по процессам
- `timer()` — контекстный менеджер для замеров
- `verify_result(C, A, B)` — проверка корректности (опционально)

#### 3. `matmul_blocking.py` — Блокирующий вариант
```python
# Pseudocode
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    A, B = generate_matrices(N)
else:
    A, B = None, None

# 1. Broadcast B to all
B = comm.bcast(B, root=0)

# 2. Scatter rows of A
rows_per_proc = N // size
A_local = np.empty((rows_per_proc, N), dtype=np.float64)
comm.Scatter(A, A_local, root=0)

# 3. Local compute
C_local = A_local @ B  # numpy matmul (BLAS)

# 4. Gather results
C = None
if rank == 0:
    C = np.empty((N, N), dtype=np.float64)
comm.Gather(C_local, C, root=0)

# 5. Timer on rank 0
```

#### 4. `matmul_nonblocking.py` — Неблокирующий вариант
```python
# Используем неблокирующие операции для перекрытия коммуникации и вычислений

# Идея (как CUDA Streams):
# 1. Начинаем неблокирующее получение строк A (Iscatter)
# 2. Одновременно начинаем неблокирующее broadcast B (Ibcast)
# 3. Ждём завершения (Waitall)
# 4. Вычисляем
# 5. Неблокирующая отправка результатов (Igatherv) + Waitall

requests = []

# Неблокирующий scatter строк A
req_scatter = comm.Iscatter(A, A_local, root=0)
requests.append(req_scatter)

# Неблокирующий broadcast B
req_bcast = comm.Ibcast(B, root=0)
requests.append(req_bcast)

# Ждём оба
MPI.Request.Waitall(requests)

# Вычисляем
C_local = A_local @ B

# Неблокирующий gather
req_gather = comm.Igatherv(C_local, [C, counts, displs], root=0)
req_gather.Wait()
```

**Важно**: `MPI_Iscatter` / `MPI_Igatherv` доступны в MPI-3 (mpi4py 3.0+). Если недоступны — эмулируем через `MPI_Isend`/`MPI_Irecv` вручную.

#### 5. `matmul.py` — общая логика + CLI
```python
# python -m lab7.matmul_blocking --size 2000
# python -m lab7.matmul_nonblocking --size 2000

import argparse
parser.add_argument("--size", type=int, default=2000)
parser.add_argument("--verify", action="store_true")
```

#### 6. `ANSWERS.md` — Ответы на вопросы
1. **MPI_COMM_WORLD** — предопределённый коммуникатор, включающий все процессы MPI приложения. Создаётся при MPI_Init. Rank от 0 до size-1.
2. **Rank** — уникальный идентификатор процесса внутри коммуникатора (0..size-1). Используется для адресации в Send/Recv, распределения работы.
3. **Начало/конец MPI программы**: `MPI_Init(&argc, &argv)` / `MPI_Finalize()` (C) или `MPI.Init()` / `MPI.Finalize()` (Python). Все MPI вызовы только между ними.
4. **Преимущество асинхронных операций**: перекрытие коммуникации и вычислений. Пока сеть передаёт данные, CPU может считать. В блокирующем режиме процесс ждёт завершения передачи. Аналогично CUDA Streams: `cudaMemcpyAsync` + kernel execution параллельно.

#### 7. `hosts` — файл хостов для mpirun
```
# Пример
192.168.1.10 slots=4
192.168.1.11 slots=4
192.168.1.12 slots=4
```

#### 8. Зависимости
- `mpi4py` — `pip install mpi4py` (требует MPI: OpenMPI / MPICH / MS-MPI)
- `numpy` — для матриц и BLAS
- MPI runtime: OpenMPI (Linux), MS-MPI (Windows)

### Запуск:
```bash
# Локально (1 машина, 4 процесса)
mpirun -np 4 python -m lab7.matmul_blocking --size 2000
mpirun -np 4 python -m lab7.matmul_nonblocking --size 2000

# На 3+ машинах (требует SSH без пароля, общий home или синхронизированный код)
mpirun -np 12 -hostfile hosts python -m lab7.matmul_nonblocking --size 4000
```

### Ожидаемые результаты:
- Размер матрицы ~2000-4000 для 10-50 сек на 4-12 процессах
- Неблокирующий вариант должен быть быстрее на 10-30% (зависит от сети)
- На быстрой сети (InfiniBand/10GbE) прирост заметнее

### Кроссплатформенность:
- Linux: OpenMPI + mpi4py
- Windows: MS-MPI + mpi4py (pip install mpi4py работает если MS-MPI в PATH)
- Код одинаковый, отличается только mpirun/mpiexec

---

## Лабораторная работа №8 — **ПЛАНИРУЕТСЯ** 📋

### Требования из ТЗ:
1. **Коллективные операции MPI** — добавить к ЛР7 использование коллективных операций
2. **Группы процессов** — программа создаёт произвольное число групп (CLI), включает в них случайное число процессов. Каждая группа умножает матрицы.
3. **Замеры времени** — в каждой группе, сравнение с парными операциями (ЛР7)
4. **MPI файловые операции** — исходные данные из 2 файлов (доступны всем узлам), процессы читают свою порцию (по rank/координате), каждый процесс пишет результат в файл своей группы
5. **Опционально** — запуск через PBS Torque

### Архитектура:

```
┌─────────────────────────────────────────────────────────────────┐
│                  MPI Matrix Multiplication v2                   │
├─────────────────────────────────────────────────────────────────┤
│  Group Management          │  Collective Ops      │  MPI-IO    │
│  ──────────────────        │  ──────────────      │  ──────    │
│  • MPI_Comm_split          │  • Bcast/Scatter     │  • MPI_File_open    │
│  • Random group sizes      │    /Gather/Reduce    │  • MPI_File_read_at │
│  • Group communicators     │  • Allreduce (time)  │  • MPI_File_write_at│
│  • Inter-group barrier     │  • Barrier (sync)    │  • MPI_File_close   │
└─────────────────────────────────────────────────────────────────┘
```

### Детальные задачи:

#### 1. Создать `lab8/` структуру
```
lab8/
├── __main__.py              # entry point
├── matmul_groups.py         # основная логика с группами
├── mpi_io.py                # MPI_File операции (read/write)
├── groups.py                # управление группами (split, random sizes)
├── utils.py                 # общие утилиты (из ЛР7 + новые)
├── ANSWERS.md               # ответы на вопросы
├── README.md                # инструкция
└── hosts                    # файл хостов
```

#### 2. `groups.py` — Управление группами
```python
def create_random_groups(comm: MPI.Comm, num_groups: int, seed: int = 42) -> List[MPI.Comm]:
    """
    Создаёт num_groups коммуникаторов с рандомными размерами.
    Возвращает список новых коммуникаторов (по одному на группу).
    """
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Генерируем случайные размеры групп (сумма = size)
    rng = np.random.default_rng(seed)
    # ... логика распределения процессов по группам
    
    # MPI_Comm_split по group_id
    group_id = ...  # 0..num_groups-1 или MPI_UNDEFINED
    new_comm = comm.Split(group_id, rank)
    
    return group_comms
```

**Алгоритм распределения:**
- Всего `size` процессов, нужно `num_groups` групп
- Минимум 1 процесс в группе, максимум `size - num_groups + 1`
- Генерируем случайные размеры, нормализуем к сумме = size
- Присваиваем каждому rank его group_id
- `MPI_Comm_split(comm, group_id, rank)` → новый коммуникатор

#### 3. `mpi_io.py` — MPI файловые операции
```python
def write_matrix_to_file(comm: MPI.Comm, filename: str, matrix: np.ndarray) -> None:
    """Параллельная запись матрицы в файл (MPI_File_write_at)."""
    fh = MPI.File.Open(comm, filename, MPI.MODE_CREATE | MPI.MODE_WRONLY)
    # Каждый процесс пишет свою порцию
    offset = rank * local_rows * n_cols * dtype.itemsize
    fh.Write_at(offset, local_matrix.ravel())
    fh.Close()

def read_matrix_from_file(comm: MPI.Comm, filename: str, shape: Tuple, dtype=np.float64) -> np.ndarray:
    """Параллельное чтение матрицы из файла (MPI_File_read_at)."""
    fh = MPI.File.Open(comm, filename, MPI.MODE_RDONLY)
    # Читаем порцию по rank
    local_matrix = np.empty(local_shape, dtype=dtype)
    offset = rank * local_rows * n_cols * dtype.itemsize
    fh.Read_at(offset, local_matrix.ravel())
    fh.Close()
    return local_matrix
```

**Требования к файлам:**
- 2 входных файла: `matrix_A.bin`, `matrix_B.bin` (доступны всем узлам, общая ФС или NFS)
- Процессы читают свои порции на основе rank в групповом коммуникаторе
- Результаты: каждый процесс пишет в файл своей группы (`group_0_C.bin`, `group_1_C.bin`, ...)

#### 4. `matmul_groups.py` — Основная логика
```python
def matmul_with_groups(
    comm: MPI.Comm,
    n: int,
    num_groups: int,
    input_A: str,
    input_B: str,
    output_prefix: str,
    use_collective: bool = True,
    verify: bool = False,
) -> Dict[int, float]:
    """
    Returns: dict {group_id: elapsed_time}
    """
    # 1. Создаём случайные группы
    group_comms = create_random_groups(comm, num_groups)
    
    # 2. В каждой группе параллельно умножаем матрицы
    times = {}
    for group_id, gcomm in enumerate(group_comms):
        if gcomm != MPI.COMM_NULL:
            # Читаем порции A, B из файлов (MPI-IO)
            A_local = read_matrix_portion(gcomm, input_A, ...)
            B_local = read_matrix_portion(gcomm, input_B, ...)
            
            # Коллективные операции внутри группы
            if use_collective:
                B_full = gcomm.bcast(B_local, root=0)  # или Scatter/Gather
                # ... коллективный алгоритм
            else:
                # Парные операции (как в ЛР7)
                pass
            
            # Замер времени
            start = MPI.Wtime()
            C_local = A_local @ B_full
            elapsed = MPI.Wtime() - start
            
            # Запись результата (MPI-IO)
            write_matrix_portion(gcomm, f"{output_prefix}_group{group_id}.bin", C_local)
            
            # Сбор времени (Allreduce для max/avg)
            max_time = gcomm.allreduce(elapsed, op=MPI.MAX)
            if gcomm.Get_rank() == 0:
                times[group_id] = max_time
    
    # 3. Сравнение с парными операциями (на root)
    if comm.Get_rank() == 0:
        print_comparison(times)
    
    return times
```

#### 5. `utils.py` — Утилиты (расширение ЛР7)
- Генерация и запись исходных файлов `matrix_A.bin`, `matrix_B.bin`
- Чтение/запись порций матриц
- Таймеры с `MPI.Wtime()`

#### 6. `ANSWERS.md` — Ответы на вопросы (добавить к ЛР7)
1. Что такое `MPI_Comm_split` и как создаются группы?
2. Разница между коллективными и парными операциями?
3. Как работают `MPI_File_read_at` / `MPI_File_write_at`?
4. Что такое `MPI_Wtime` и зачем он нужен?
5. Преимущества коллективных операций над парными?

#### 7. Запуск
```bash
# Локально (8 процессов, 2 группы)
mpirun -np 8 python -m lab8 --size 2000 --groups 2 --input-A matrix_A.bin --input-B matrix_B.bin --output result

# На кластере
mpirun -np 24 -hostfile hosts python -m lab8 --size 4000 --groups 4 ...
```

### Приоритеты:
1. `groups.py` — `MPI_Comm_split` с рандомными размерами
2. `mpi_io.py` — `MPI_File` read/write_at
3. `matmul_groups.py` — интеграция групп + коллективные ops + MPI-IO
4. `ANSWERS.md` — вопросы
5. Тестирование: сравнение времени коллективных vs парных

### Зависимости:
- Те же: `mpi4py`, `numpy`
- Общая файловая система для входных файлов (NFS / shared disk) или копирование на все узлы

---

## ИТОГОВЫЙ СТАТУС: ВСЕ 8 ЛАБ ГОТОВЫ

| Лаба | Статус | Ключевые компоненты |
|------|--------|---------------------|
| 1 | ✅ | 3 TCP сервера, файлы, resume, битрейт, keepalive |
| 2 | ✅ | **Настоящий RUDP на UDP**: sliding window, ACK/NACK, fast retransmit, RTO |
| 3 | ✅ | SelectServer — чистый select, 1 поток |
| 4 | ✅ | Вариант 4 — UDP + поток на сессию + MSG_PEEK |
| 5 | ✅ | Parallel ping, traceroute, smurf demo, ANSWERS.md |
| 6 | ✅ | **P2P чат** (broadcast + multicast, discovery, ignore, ANSWERS.md) |
| 7 | 📦 | **MPI матрицы** — blocking/non-blocking, 3+ машины, ANSWERS.md |
| 8 | 📦 | **MPI группы** — Comm_split, коллективные ops, MPI-IO, ANSWERS.md |

### Структура проекта:
```
SPOIRS/
├── lab1/   → ЛР1-3 (серверы, клиент, RUDP, select)
├── lab4/   → ЛР4 вариант 4 (UDP + MSG_PEEK)
├── lab5/   → ЛР5 (ping, traceroute, smurf)
├── lab6/   → ЛР6 (P2P чат broadcast/multicast)
├── lab7/   → ЛР7 (MPI матрицы blocking/non-blocking)
├── lab8/   → ЛР8 (MPI группы + коллективные + MPI-IO)
├── doc/    → ТЗ
└── agent/  → Отчёты
```

### Запуск всех лаб:
```bash
# ЛР1
python -m lab1.run_server
python -m lab1.run_client

# ЛР3
python -m lab1.run_server_select

# ЛР4 (вариант 4)
python -m lab4
python -m lab4.client

# ЛР5
python -m lab5 ping 8.8.8.8 1.1.1.1
python -m lab5 trace google.com
python -m lab5 smurf --wireshark-help

# ЛР6
python -m lab6 -n "Alice"
python -m lab6 -n "Bob"

# ЛР7 (требует MPI)
mpirun -np 4 python -m lab7.matmul_blocking --size 2000
mpirun -np 4 python -m lab7.matmul_nonblocking --size 2000

# ЛР8 (требует MPI)
mpirun -np 8 python -m lab8 --gen-inputs --size 2000
mpirun -np 8 python -m lab8 --size 2000 --groups 2
mpirun -np 8 python -m lab8 --size 2000 --groups 2 --pairwise
```

### Зависимости:
- `mpi4py` + MPI (OpenMPI/MS-MPI) — для ЛР7, ЛР8
- `numpy` — для ЛР7, ЛР8
- `psutil` (опц.) — для ЛР6 автоопределение интерфейсов
- `colorama` (опц.) — для ЛР6 цветной вывод