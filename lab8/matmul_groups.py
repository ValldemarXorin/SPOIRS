"""
Lab 8: MPI Groups + MPI-IO + collective ops (logic from lr8.py).

Случайное деление процессов на группы (MPI_Comm_split), параллельное чтение
среза матрицы A из общего файла (MPI_File.Read_at_all), Bcast B внутри группы,
локальное умножение, параллельная запись результата (Write_at_all) + замер
последовательных парных операций для сравнения.
"""

import os
# Ограничиваем NumPy 1 потоком на процесс, чтобы они не душили друг друга на ядрах CPU
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

import sys
import time
import argparse
import numpy as np
try:
    from mpi4py import MPI
except (ImportError, OSError, RuntimeError) as _mpi_err:
    raise ImportError(
        'mpi4py/MPI runtime не найден. Установите MPI:\n'
        '  Linux:  sudo apt-get install openmpi-bin libopenmpi-dev python3-dev && pip install mpi4py\n'
        '  Windows: MS-MPI (https://docs.microsoft.com/en-us/message-passing-interface/microsoft-mpi) + pip install mpi4py\n'
        f'Ошибка: {_mpi_err}'
    ) from _mpi_err

comm = MPI.COMM_WORLD
global_rank = comm.Get_rank()
global_size = comm.Get_size()


def generate_shared_files(n: int, file_a: str, file_b: str):
    """Генерация бинарных файлов матриц на мастер-узле."""
    if global_rank == 0:
        np.random.seed(42)
        A = np.random.rand(n, n).astype(np.float64)
        B = np.random.rand(n, n).astype(np.float64)
        A.tofile(file_a)
        B.tofile(file_b)
        print(f"[I/O] Сгенерированы файлы матриц: {file_a} и {file_b} ({n}x{n})")
    comm.Barrier()


def assign_random_groups(num_groups: int) -> int:
    """Случайно распределяет процессы по группам (каждая группа получит >=1 процесс)."""
    if global_rank == 0:
        # Гарантируем, что в каждой группе есть минимум 1 процесс
        mapping = list(range(num_groups))
        # Оставшиеся процессы распределяем случайно
        remaining = global_size - num_groups
        if remaining > 0:
            mapping.extend(np.random.randint(0, num_groups, size=remaining).tolist())
        np.random.shuffle(mapping)
    else:
        mapping = None

    # Рассылаем распределение всем процессам
    mapping = comm.bcast(mapping, root=0)
    return mapping[global_rank]


def compute_chunk_offsets(n: int, grank: int, gsize: int):
    """Вычисляет количество строк и байтовое смещение для текущего процесса."""
    counts = [n // gsize + (1 if i < (n % gsize) else 0) for i in range(gsize)]
    displs = [sum(counts[:i]) for i in range(gsize)]
    my_rows = counts[grank]
    byte_offset = displs[grank] * n * 8  # float64 = 8 байт
    return my_rows, byte_offset


def run_group_matrix_multiplication(group_comm, group_id: int, n: int, file_a: str, file_b: str):
    """Выполняет матричное умножение внутри группы с использованием MPI-IO и коллективных операций."""
    grank = group_comm.Get_rank()
    gsize = group_comm.Get_size()

    my_rows, byte_offset = compute_chunk_offsets(n, grank, gsize)

    group_comm.Barrier()
    t_start = MPI.Wtime()

    # 1. Параллельное чтение среза матрицы A напрямую из общего файла
    A_sub = np.empty((my_rows, n), dtype=np.float64)
    fh_a = MPI.File.Open(group_comm, file_a, MPI.MODE_RDONLY)
    fh_a.Read_at_all(byte_offset, A_sub)
    fh_a.Close()

    # 2. Коллективная рассылка матрицы B внутри группы (Bcast)
    B = np.empty((n, n), dtype=np.float64)
    if grank == 0:
        fh_b = MPI.File.Open(MPI.COMM_SELF, file_b, MPI.MODE_RDONLY)
        fh_b.Read(B)
        fh_b.Close()
    group_comm.Bcast(B, root=0)

    # 3. Локальные вычисления
    C_sub = np.dot(A_sub, B)

    # 4. Параллельная запись результата каждым процессом в файл своей группы
    out_file = f"result_group_{group_id}.bin"
    fh_out = MPI.File.Open(group_comm, out_file, MPI.MODE_CREATE | MPI.MODE_WRONLY)
    fh_out.Write_at_all(byte_offset, C_sub)
    fh_out.Close()

    group_comm.Barrier()
    t_group = MPI.Wtime() - t_start

    # Максимальное время среди процессов группы
    max_t = group_comm.reduce(t_group, op=MPI.MAX, root=0)
    if grank == 0:
        print(f"  • [Группа {group_id}] Процессов: {gsize:2d} | Время вычислений + I/O: {max_t:.4f} сек -> Файл: {out_file}")
    return max_t


def run_point_to_point_benchmark(n: int) -> float:
    """Быстрый замер парных операций (Send/Recv) на глобальном коммуникаторе для сравнения."""
    if global_rank == 0:
        A = np.random.rand(n, n).astype(np.float64)
        B = np.random.rand(n, n).astype(np.float64)
        C = np.empty((n, n), dtype=np.float64)
        t0 = time.time()
        for w in range(1, global_size):
            comm.Send(B, dest=w, tag=90)
            comm.Send(A[0:10, :], dest=w, tag=91)
            comm.Recv(C[0:10, :], source=w, tag=92)
        return time.time() - t0
    else:
        B = np.empty((n, n), dtype=np.float64)
        A_sub = np.empty((10, n), dtype=np.float64)
        comm.Recv(B, source=0, tag=90)
        comm.Recv(A_sub, source=0, tag=91)
        C_sub = np.dot(A_sub, B)
        comm.Send(C_sub, dest=0, tag=92)
        return 0.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--groups", type=int, default=2, help="Количество формируемых групп")
    parser.add_argument("--dim", type=int, default=1200, help="Размерность матриц")
    args = parser.parse_args()

    num_groups = args.groups
    n = args.dim

    if global_size < num_groups:
        if global_rank == 0:
            print(f"Ошибка: Количество процессов ({global_size}) должно быть >= количества групп ({num_groups})!")
        sys.exit(1)

    file_a = "shared_matrix_A.bin"
    file_b = "shared_matrix_B.bin"

    # Создание общих файлов
    generate_shared_files(n, file_a, file_b)

    # 1. Случайное деление на группы (MPI_Comm_split)
    my_group = assign_random_groups(num_groups)
    group_comm = comm.Split(color=my_group, key=global_rank)

    if global_rank == 0:
        print(f"\nЗапуск параллельных вычислений в {num_groups} группах...")

    # 2. Выполнение вычислений и параллельного вывода
    run_group_matrix_multiplication(group_comm, my_group, n, file_a, file_b)
    group_comm.Free()

    # 3. Замер парных операций для сравнения по методичке
    comm.Barrier()
    t_p2p = run_point_to_point_benchmark(n)

    if global_rank == 0:
        print("\n================== ИТОГИ СРАВНЕНИЯ ЛР №8 ==================")
        print(f"1. Время последовательных парных операций (Send/Recv): {t_p2p:.4f} сек")
        print("2. Коллективные операции + MPI File I/O позволили параллельно")
        print("   обработать общие файлы без перегрузки мастер-узла.")
        print("==========================================================")

        # Очистка файлов
        print("\n[ПРОВЕРКА ФАЙЛОВ НА ДИСКЕ]:")
        for g in range(num_groups):
            out_file = f"result_group_{g}.bin"
            if os.path.exists(out_file):
                size_mb = os.path.getsize(out_file) / (1024 * 1024)
                # Читаем первые 3 числа из бинарного файла, чтобы доказать, что там результат
                sample_data = np.fromfile(out_file, dtype=np.float64, count=3)
                print(f"  ✔ Файл {out_file} существует! Размер: {size_mb:.2f} МБ | Первые числа: {sample_data}")


if __name__ == "__main__":
    main()