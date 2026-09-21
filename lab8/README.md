# Lab 8: MPI Groups + MPI-IO (Matrix Multiplication in Random Groups)

Логика из эталонного `lr8.py`: случайное деление процессов на группы (`MPI_Comm_split`),
параллельное чтение матриц из общих файлов (`MPI_File.Read_at_all`), коллективный
`Bcast` внутри групп, локальное умножение и параллельная запись результата
(`MPI_File.Write_at_all`) + замер последовательных парных операций для сравнения.

## Возможности
- **Случайные группы** — `MPI_Comm_split`, в каждой группе минимум 1 процесс
- **MPI-IO** — параллельное чтение среза A из общего файла и запись результата по группам
- **Коллективные операции** — `Bcast` матрицы B внутри группы, `reduce(MAX)` времени
- **Сравнение с парными операциями** — замер последовательных `Send/Recv` на `COMM_WORLD`

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

```bash
# Локально (8 процессов, 2 группы)
mpirun -np 8 python -m lab8 --dim 1200 --groups 2

# На кластере (24 процесса, 4 группы)
mpirun -np 24 -hostfile hosts python -m lab8 --dim 1200 --groups 4
```

## Аргументы командной строки

| Аргумент | Описание |
|----------|----------|
| `--groups` | Количество формируемых групп (default: 2) |
| `--dim` | Размерность матриц (default: 1200) |

## Алгоритм

```
1. MPI_COMM_WORLD (size процессов)
        │
        ▼
2. Генерация общих файлов shared_matrix_A.bin / shared_matrix_B.bin (rank 0)
        │
        ▼
3. Случайное распределение по группам (каждая >=1 процесс)
        │
        ▼
4. MPI_Comm_split(color=group_id, key=rank)
        │
        ▼
5. В каждой группе:
   • MPI_File.Read_at_all(offset) — каждый процесс читает свой срез строк A
   • Bcast(B, root=0) — рассылка B внутри группы
   • C_sub = A_sub @ B           — локальное умножение
   • MPI_File.Write_at_all()     — параллельная запись в result_group_<id>.bin
   • reduce(MAX) времени по группе
        │
        ▼
6. Замер последовательных парных операций (Send/Recv) для сравнения
        │
        ▼
7. Итог + проверка файлов на диске (размер, первые числа)
```

## Выходные файлы
- `shared_matrix_A.bin`, `shared_matrix_B.bin` — общие входные матрицы
- `result_group_0.bin`, `result_group_1.bin`, ... — результаты по группам
- В конце `main` печатает размер файлов и первые числа из них

## Структура файлов

```
lab8/
├── __main__.py              # Entry point (imports matmul_groups.main)
├── matmul_groups.py         # Вся логика lr8.py (группы + MPI-IO + сравнение)
├── ANSWERS.md               # Ответы на вопросы защиты
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
- **Количество процессов >= количество групп** — иначе ошибка
- `numpy` ограничен 1 потоком на процесс (`OMP_NUM_THREADS=1`)
- Файлы создаются в рабочей директории запуска