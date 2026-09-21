"""
MPI Matrix Multiplication - Non-Blocking Pipeline (logic from lr7.py).
Настоящий неблокирующий конвейер (overlap): Isend/Irecv, double buffering,
prefetch следующего чанка пока считаем текущий.
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


def matmul_nonblocking(comm: MPI.Comm, n: int) -> float:
    """Неблокирующий конвейер (overlap). Возвращает время на rank 0."""
    rank = comm.Get_rank()
    size = comm.Get_size()
    workers = size - 1
    rows_per_worker = n // workers

    if rank == 0:
        A, B = init_matrices(n)
        start_t = time.time()

        # Асинхронная рассылка B
        reqs_b = [comm.Isend(B, dest=w, tag=10) for w in range(1, size)]
        MPI.Request.Waitall(reqs_b)

        C = np.empty((n, n), dtype=np.float64)
        chunk_rows = rows_per_worker // NUM_CHUNKS

        # Конвейерная отправка и сбор
        for ch in range(NUM_CHUNKS):
            send_reqs = []
            recv_reqs = []
            for w in range(1, size):
                w_offset = (w - 1) * rows_per_worker
                r_start = w_offset + ch * chunk_rows
                r_end = (w_offset + rows_per_worker) if ch == NUM_CHUNKS - 1 else (r_start + chunk_rows)

                s_req = comm.Isend(A[r_start:r_end, :], dest=w, tag=20 + ch)
                r_req = comm.Irecv(C[r_start:r_end, :], source=w, tag=40 + ch)
                send_reqs.append(s_req)
                recv_reqs.append(r_req)

            MPI.Request.Waitall(send_reqs)
            MPI.Request.Waitall(recv_reqs)

        elapsed = time.time() - start_t
        return elapsed
    else:
        B = np.empty((n, n), dtype=np.float64)
        req_b = comm.Irecv(B, source=0, tag=10)
        req_b.Wait()

        my_rows = rows_per_worker
        chunk_rows = my_rows // NUM_CHUNKS

        # Буферы двойной буферизации (Double Buffering)
        r_count_0 = chunk_rows
        curr_A = np.empty((r_count_0, n), dtype=np.float64)

        # Предварительная выборка первого чанка (Prefetch)
        req_recv = comm.Irecv(curr_A, source=0, tag=20)
        req_recv.Wait()

        prev_send_req = None

        for ch in range(NUM_CHUNKS):
            # 1. Запускаем фоновый прием СЛЕДУЮЩЕГО чанка k+1 (пока считаем чанк k)
            if ch + 1 < NUM_CHUNKS:
                next_count = (my_rows - (ch + 1) * chunk_rows) if (ch + 1) == NUM_CHUNKS - 1 else chunk_rows
                next_A = np.empty((next_count, n), dtype=np.float64)
                next_recv_req = comm.Irecv(next_A, source=0, tag=20 + ch + 1)

            # 2. ВЫЧИСЛЕНИЯ на CPU (параллельно с сетевой передачей!)
            C_chunk = np.dot(curr_A, B)

            # 3. Ждем завершения фоновой отправки ПРЕДЫДУЩЕГО ответа
            if prev_send_req is not None:
                prev_send_req.Wait()

            # 4. Запускаем фоновую отправку текущего ответа в память
            prev_send_req = comm.Isend(C_chunk, dest=0, tag=40 + ch)

            # 5. Переключаем буферы на следующий шаг
            if ch + 1 < NUM_CHUNKS:
                next_recv_req.Wait()
                curr_A = next_A

        if prev_send_req is not None:
            prev_send_req.Wait()

        return 0.0


def main():
    parser = argparse.ArgumentParser(description="MPI Matrix Multiplication - Non-Blocking (lr7)")
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
        print("MPI Matrix Multiplication - NON-BLOCKING VERSION")
        print("=" * 60)
        print(f"Matrix size: {args.size}×{args.size}")
        print(f"Processes: {size}")
        print("-" * 60)

    elapsed = matmul_nonblocking(comm, args.size)

    if rank == 0:
        print(f"Неблокирующий конвейер: {elapsed:.4f} сек")
        print("=" * 60)


if __name__ == "__main__":
    main()