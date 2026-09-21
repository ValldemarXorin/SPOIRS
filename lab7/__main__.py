"""Lab 7 MPI Matrix Multiplication - Entry point (blocking/nonblocking/compare)."""

import sys
import time
import argparse
try:
    from mpi4py import MPI
except (ImportError, OSError, RuntimeError) as _mpi_err:
    raise ImportError(
        'mpi4py/MPI runtime не найден. Установите MPI:\n'
        '  Linux:  sudo apt-get install openmpi-bin libopenmpi-dev python3-dev && pip install mpi4py\n'
        '  Windows: MS-MPI (https://docs.microsoft.com/en-us/message-passing-interface/microsoft-mpi) + pip install mpi4py\n'
        f'Ошибка: {_mpi_err}'
    ) from _mpi_err

from lab7.matmul_blocking import DEFAULT_N, NUM_CHUNKS
from lab7.matmul_blocking import matmul_blocking
from lab7.matmul_nonblocking import matmul_nonblocking


def _require3(rank: int, size: int) -> None:
    if size < 3:
        if rank == 0:
            print("Ошибка: Требуется запуск минимум на 3-х процессах!")
        sys.exit(1)


def _print_compare(n: int, size: int, t_block: float, t_pipeline: float) -> None:
    speedup = ((t_block - t_pipeline) / t_block) * 100.0
    print("\n================== ИТОГ ЛР №7 ==================")
    print(f"Размер матриц: {n}x{n} | Процессов: {size} | Чанков: {NUM_CHUNKS}")
    print(f"1. Блокирующий режим:        {t_block:.4f} сек")
    print(f"2. Неблокирующий конвейер:   {t_pipeline:.4f} сек")
    print(f"Реальный прирост скорости:    {speedup:.2f}%")
    print("================================================")


def main():
    parser = argparse.ArgumentParser(description="Lab 7: MPI Matrix Multiplication")
    subparsers = parser.add_subparsers(dest="mode", help="Mode")

    block_parser = subparsers.add_parser("blocking", help="Blocking version (MPI_Send/Recv)")
    block_parser.add_argument("--size", "-n", type=int, default=DEFAULT_N)

    nb_parser = subparsers.add_parser("nonblocking", help="Non-blocking pipeline (MPI_Isend/Irecv)")
    nb_parser.add_argument("--size", "-n", type=int, default=DEFAULT_N)

    cmp_parser = subparsers.add_parser("compare", help="Run both and compare (speedup %)")
    cmp_parser.add_argument("--size", "-n", type=int, default=DEFAULT_N)

    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if args.mode == "blocking":
        _require3(rank, size)
        if rank == 0:
            print("=" * 60)
            print("MPI Matrix Multiplication - BLOCKING VERSION")
            print("=" * 60)
            print(f"Matrix size: {args.size}×{args.size}")
            print(f"Processes: {size}")
            print("-" * 60)
        t = matmul_blocking(comm, args.size)
        if rank == 0:
            print(f"Блокирующий режим: {t:.4f} сек")
            print("=" * 60)

    elif args.mode == "nonblocking":
        _require3(rank, size)
        if rank == 0:
            print("=" * 60)
            print("MPI Matrix Multiplication - NON-BLOCKING VERSION")
            print("=" * 60)
            print(f"Matrix size: {args.size}×{args.size}")
            print(f"Processes: {size}")
            print("-" * 60)
        t = matmul_nonblocking(comm, args.size)
        if rank == 0:
            print(f"Неблокирующий конвейер: {t:.4f} сек")
            print("=" * 60)

    elif args.mode == "compare":
        _require3(rank, size)
        if rank == 0:
            print("Замер блокирующего режима...")
        comm.Barrier()
        t_block = matmul_blocking(comm, args.size)
        if rank == 0:
            print("Пауза для стабилизации кэша процессора (1с)...")
        time.sleep(1.0)
        comm.Barrier()
        t_pipeline = matmul_nonblocking(comm, args.size)
        if rank == 0:
            _print_compare(args.size, size, t_block, t_pipeline)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()