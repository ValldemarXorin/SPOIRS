# Lab 7: MPI Matrix Multiplication

Программа для умножения матриц с использованием MPI (mpi4py).
Реализованы два варианта: блокирующий и неблокирующий (перекрытие коммуникации и вычислений).

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

### Локально (на 1 машине, 4 процесса):
```bash
# Блокирующий вариант
mpirun -np 4 python -m lab7.matmul_blocking --size 2000

# Неблокирующий вариант
mpirun -np 4 python -m lab7.matmul_nonblocking --size 2000

# С верификацией результата
mpirun -np 4 python -m lab7.matmul_blocking --size 2000 --verify
```

### Автоподбор размера под целевое время:
```bash
# Цель ~30 секунд
mpirun -np 4 python -m lab7.matmul_blocking --target-time 30
```

### На кластере (3+ машины):
```bash
# Создайте файл hosts:
# node1 slots=4
# node2 slots=4
# node3 slots=4

mpirun -np 12 -hostfile hosts python -m lab7.matmul_nonblocking --size 4000
```

### Через entry point:
```bash
# Блокирующий
python -m lab7 blocking --size 2000

# Неблокирующий
python -m lab7 nonblocking --size 2000

# Сравнение обоих
python -m lab7 compare --size 2000
```

## Аргументы командной строки

| Аргумент | Описание |
|----------|----------|
| `--size`, `-n` | Размер матрицы N×N (default: 2000) |
| `--verify`, `-v` | Проверить корректность результата |
| `--target-time`, `-t` | Автоподбор размера под целевое время (сек) |

## Алгоритм

1. **Rank 0** генерирует матрицы A(N×N) и B(N×N)
2. **Bcast B** — матрица B рассылается всем процессам
3. **Scatter A rows** — строки матрицы A делятся между процессами
4. **Local compute** — каждый процесс вычисляет свои строки C = A_local @ B
5. **Gather C** — результаты собираются в rank 0

## Ожидаемая производительность

| Размер | Процессы | Блокирующий | Неблокирующий | Прирост |
|--------|----------|-------------|---------------|---------|
| 2000×2000 | 4 | ~3-5 сек | ~2.5-4 сек | 10-20% |
| 4000×4000 | 8 | ~20-30 сек | ~18-25 сек | 10-30% |

*Зависит от сети: InfiniBand/10GbE даёт больший прирост неблокирующего варианта.*

## Структура файлов

```
lab7/
├── __main__.py              # Entry point (blocking/nonblocking/compare)
├── matmul_blocking.py       # Блокирующий вариант (MPI_Send/Recv коллективы)
├── matmul_nonblocking.py    # Неблокирующий (Ibcast/Iscatter/Igatherv + Waitall)
├── utils.py                 # Генерация матриц, таймеры, верификация
├── ANSWERS.md               # Ответы на 4 вопроса защиты
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

- Для работы неблокирующих коллективов (Ibcast, Iscatter, Igatherv) требуется **MPI-3** и **mpi4py 3.0+**
- На старых версиях используется fallback на ручные `MPI_Isend`/`MPI_Irecv`
- `numpy` использует BLAS (OpenBLAS/MKL) для локального умножения — основная нагрузка на CPU
- Коммуникация: Bcast O(log P), Scatter/Gather O(P) по времени