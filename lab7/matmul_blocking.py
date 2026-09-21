"""
MPI Matrix Multiplication - Blocking Version (logic from lr7.py).
Пошаговая блокирующая передача чанков: Send(B), затем по NUM_CHUNKS шагов
Send(кусок A) -> Recv(кусок C) для каждого воркера.
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

DEFAULT_N = 1400      # Размерность матрицы (подберите 1200-1600 под скорость CPU)
NUM_CHUNKS = 6        # На сколько блоков дробится задача каждого воркера


def init_matrices(n: int):
    np.random.seed(42)
    return np.random.rand(n, n).astype(np.float64), np.random.rand(n, n).astype(np.float64)


def matmul_blocking(comm: MPI.Comm, n: int) -> float:
    """Блокирующий режим. Возвращает время на rank 0."""
    rank = comm.Get_rank()
    size = comm.Get_size()
    workers = size - 1
    total_rows = n
    rows_per_worker = total_rows // workers

    if rank == 0:
        A, B = init_matrices(n)
        start_t = time.time()

        # Рассылка B
        for w in range(1, size):
            comm.Send(B, dest=w, tag=1)

        # Пошаговая блокирующая передача чанков
        C = np.empty((n, n), dtype=np.float64)
        chunk_rows = rows_per_worker // NUM_CHUNKS

        for ch in range(NUM_CHUNKS):
            for w in range(1, size):
                w_offset = (w - 1) * rows_per_worker
                r_start = w_offset + ch * chunk_rows
                r_end = (w_offset + rows_per_worker) if ch == NUM_CHUNKS - 1 else (r_start + chunk_rows)
                # Блокирующая отправка куска
                comm.Send(A[r_start:r_end, :], dest=w, tag=2)
                # Блокирующий прием результата
                comm.Recv(C[r_start:r_end, :], source=w, tag=3)

        elapsed = time.time() - start_t
        return elapsed
    else:
        B = np.empty((n, n), dtype=np.float64)
        comm.Recv(B, source=0, tag=1)

        my_rows = rows_per_worker
        chunk_rows = my_rows // NUM_CHUNKS

        for ch in range(NUM_CHUNKS):
            r_count = (my_rows - ch * chunk_rows) if ch == NUM_CHUNKS - 1 else chunk_rows
            A_chunk = np.empty((r_count, n), dtype=np.float64)
            # Ждем данные
            comm.Recv(A_chunk, source=0, tag=2)
            # Считаем
            C_chunk = np.dot(A_chunk, B)
            # Ждем отправку
            comm.Send(C_chunk, dest=0, tag=3)

        return 0.0


def main():
    parser = argparse.ArgumentParser(description="MPI Matrix Multiplication - Blocking (lr7)")
    parser.add_argument("--size", "-n", type=int, default=DEFAULT_N, help="Matrix size N×N")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if size < 3:
        if rank == 0:
            print("Ошибка: Требуется запуск минимум на 3-х процессах!")
        sys.exit(1)

    if rank == 0:
        print("=" * 60)
        print("MPI Matrix Multiplication - BLOCKING VERSION")
        print("=" * 60)
        print(f"Matrix size: {args.size}×{args.size}")
        print(f"Processes: {size}")
        print("-" * 60)

    elapsed = matmul_blocking(comm, args.size)

    if rank == 0:
        print(f"Блокирующий режим: {elapsed:.4f} сек")
        print("=" * 60)


if __name__ == "__main__":
    main()