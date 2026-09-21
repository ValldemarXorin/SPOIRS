# Lab 7: MPI Matrix Multiplication (Blocking / Non-Blocking)

Логика из эталонного `lr7.py`: честный блокирующий режим и настоящий неблокирующий конвейер
(overlap: коммуникация перекрывается вычислениями, double buffering + prefetch).

## Требования
- Python 3.8+
- MPI: OpenMPI (Linux) / MS-MPI (Windows)
- `mpi4py` — `pip install mpi4py`
- `numpy` — `pip install numpy`

## Установка

### Linux (Ubuntu/Debian):
```bash
sudo apt-get install openmpi-bin libopenmpi-dev python3-dev
pip install mpi4py numpy
```

### Windows:
1. Установите MS-MPI: https://docs.microsoft.com/en-us/message-passing-interface/microsoft-mpi
2. Добавьте `C:\Program Files\Microsoft MPI\Bin` в PATH
3. `pip install mpi4py numpy`

## Запуск

### Локально (на 1 машине, 3+ процесса — обязательно):
```bash
# Блокирующий вариант
mpirun -np 4 python -m lab7.matmul_blocking --size 1400

# Неблокирующий конвейер
mpirun -np 4 python -m lab7.matmul_nonblocking --size 1400

# Сравнение обоих (замер обоих + прирост скорости)
mpirun -np 4 python -m lab7 compare --size 1400
```

### На кластере (3+ машины):
```bash
# Создайте файл hosts:
# node1 slots=4
# node2 slots=4
# node3 slots=4

mpirun -np 12 -hostfile hosts python -m lab7.matmul_nonblocking --size 1400
```

### Через entry point:
```bash
python -m lab7 blocking --size 1400
python -m lab7 nonblocking --size 1400
python -m lab7 compare --size 1400
```

## Аргументы командной строки

| Аргумент | Описание |
|----------|----------|
| `--size`, `-n` | Размер матрицы N×N (default: 1400) |

## Алгоритм

Матрица A режется на `NUM_CHUNKS = 6` блоков по строкам; B рассылается всем.

**1. Блокирующий режим** (`matmul_blocking.py`):
```
Rank 0: Send(B) всем -> для каждого чанка: Send(кусок A воркеру), Recv(кусок C от воркера)
Воркер: Recv(B) -> для каждого чанка: Recv(кусок A), C_chunk = A_chunk @ B, Send(C_chunk)
```
Передача и вычисления не перекрываются — каждый шаг блокирующий.

**2. Неблокирующий конвейер** (`matmul_nonblocking.py`):
```
Rank 0: Isend(B), Waitall -> для каждого чанка: Isend(кусок A), Irecv(кусок C), Waitall
Воркер: Irecv(B) -> prefetch первого чанка -> для каждого чанка:
        1) Irecv(следующий чанк)      // фоновый приём k+1
        2) C_chunk = A_chunk @ B      // вычисления параллельно с сетью
        3) Wait(предыдущий Isend)     // дожидаемся отправки k-1
        4) Isend(C_chunk)             // фоновая отправка ответа
        5) Wait(приём k+1), переключить буфер (double buffering)
```
Благодаря двойной буферизации и prefetch коммуникация следующего чанка
перекрывается с вычислением текущего — отсюда прирост скорости.

## Структура файлов

```
lab7/
├── __main__.py              # Entry point (blocking/nonblocking/compare)
├── matmul_blocking.py       # Блокирующий вариант (MPI_Send/Recv)
├── matmul_nonblocking.py    # Неблокирующий конвейер (MPI_Isend/Irecv)
├── ANSWERS.md               # Ответы на вопросы защиты
├── README.md                # Этот файл
└── hosts                    # Пример файла хостов для кластера
```

## Ответы на вопросы защиты
См. `ANSWERS.md`:
1. MPI_COMM_WORLD
2. Rank
3. MPI_Init / MPI_Finalize
4. Преимущество асинхронных операций (перекрытие comm + compute, как CUDA Streams)

## Примечания

- Требуется минимум **3 процесса** (`size < 3` → ошибка).
- `numpy` ограничен 1 потоком на процесс (`OMP_NUM_THREADS=1`), чтобы процессы не душили друг друга на ядрах CPU.
- Размер по умолчанию 1400×1400 (~10-25 сек на типичном CPU); при необходимости подберите 1200-1600.
- На маленьких размерах (например 300) прирост может быть отрицательным — это нормально, эффект появляется на реальных размерах.