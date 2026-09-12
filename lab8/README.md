# Lab 8: MPI Matrix Multiplication with Groups, Collective Ops, and MPI-IO

Расширение Lab 7: умножение матриц в случайных группах процессов с использованием коллективных операций и параллельного MPI-IO.

## Возможности
- **Случайные группы** — `MPI_Comm_split` с рандомными размерами (сумма = общее число процессов)
- **Коллективные операции** — Bcast/Scatter/Gather/Allreduce внутри групп
- **Парные операции** — альтернативный режим с ручными Send/Recv
- **MPI-IO** — параллельное чтение/запись матриц (`MPI_File_read_at_all`/`Write_at_all`)
- **Замеры времени** — `MPI_Wtime` в каждой группе, сравнение collective vs pairwise

## Требования
- Python 3.8+
- MPI: OpenMPI (Linux) / MS-MPI (Windows)
- `mpi4py` — `pip install mpi4py`
- `numpy` — `pip install numpy`

## Установка
```bash
# Linux
sudo apt-get install openmpi-bin libopenmpi-dev python3-dev
pip install mpi4py numpy

# Windows
# 1. Установить MS-MPI
# 2. pip install mpi4py numpy
```

## Запуск

### Генерация входных файлов (обязательно перед первым запуском):
```bash
mpirun -np 8 python -m lab8 --gen-inputs --size 2000
```
Создаёт `matrix_A.bin` и `matrix_B.bin` через коллективный MPI-IO.

### Коллективный режим (по умолчанию):
```bash
# Локально (8 процессов, 2 группы)
mpirun -np 8 python -m lab8 --size 2000 --groups 2

# На кластере (24 процесса, 4 группы)
mpirun -np 24 -hostfile hosts python -m lab8 --size 4000 --groups 4
```

### Парный режим (для сравнения):
```bash
mpirun -np 8 python -m lab8 --size 2000 --groups 2 --pairwise
```

### Автоподбор размера под целевое время:
```bash
mpirun -np 8 python -m lab8 --target-time 30 --groups 2
```

## Аргументы командной строки

| Аргумент | Описание |
|----------|----------|
| `--size`, `-n` | Размер матрицы N×N (default: 2000) |
| `--groups`, `-g` | Число групп (default: 2, max: num_procs) |
| `--verify`, `-v` | Проверить результат (требует полные A, B в памяти) |
| `--collective`, `-c` | Использовать коллективные операции (default) |
| `--pairwise`, `-p` | Использовать парные Send/Recv |
| `--target-time`, `-t` | Автоподбор размера под время (сек) |
| `--gen-inputs` | Сгенерировать matrix_A.bin, matrix_B.bin |
| `--input-A` | Файл матрицы A (default: matrix_A.bin) |
| `--input-B` | Файл матрицы B (default: matrix_B.bin) |
| `--output`, `-o` | Префикс выходных файлов (default: result) |

## Выходные файлы
- `result_group0.bin`, `result_group1.bin`, ... — результаты по группам
- Каждый файл содержит полную матрицу C = A × B, собранную из частей группы

## Алгоритм

```
1. MPI_COMM_WORLD (size процессов)
        │
        ▼
2. MPI_Comm_split(color=group_id, key=rank)
        │
        ├──────────┬──────────┬──────────┐
        ▼          ▼          ▼          ▼
      Group 0    Group 1    Group 2    Group 3
      (size0)    (size1)    (size2)    (size3)
        │          │          │          │
        ▼          ▼          ▼          ▼
3. MPI-IO read: A_local, B  (коллективное чтение)
        │
        ▼
4. Коллективные ops / Парные ops:
   • Bcast B
   • Scatter A rows (или ручная раздача)
   • Local C_local = A_local @ B
   • Gatherv C / ручной сбор
        │
        ▼
5. MPI_Wtime замер в каждой группе
        │
        ▼
6. MPI-IO write: result_groupX.bin
        │
        ▼
7. Сравнение времени на rank 0
```

## Структура файлов

```
lab8/
├── __main__.py              # Entry point
├── matmul_groups.py         # Основная логика групп
├── groups.py                # MPI_Comm_split с рандомными размерами
├── mpi_io.py                # MPI_File read_at_all/write_at_all
├── utils.py                 # Генерация, таймеры, верификация, сравнение
├── ANSWERS.md               # 5 вопросов защиты
├── README.md                # Этот файл
└── hosts                    # Пример файла хостов
```

## Ответы на вопросы защиты
См. `ANSWERS.md`:
1. MPI_Comm_split — создание групп
2. Коллективные vs парные операции
3. MPI_File_read_at / write_at
4. MPI_Wtime
5. Преимущества коллективных операций

## Примечания
- **MPI-IO** требует общей файловой системы (NFS, Lustre, GPFS) или одинаковых путей на всех узлах
- **Коллективные операции** (`_all` версии) эффективнее — MPI агрегирует I/O
- **Случайные группы** — seed=42 для воспроизводимости
- **Размер группы ≥ 1** — гарантируется при распределении