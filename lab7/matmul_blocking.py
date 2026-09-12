"""
MPI Matrix Multiplication - Blocking Version.
Uses MPI_Send/Recv (via Scatter, Bcast, Gather collectives).
"""

import sys
import argparse
import numpy as np
from mpi4py import MPI

from lab7.utils import (
    generate_matrices,
    split_matrix_rows,
    scatter_matrix_rows,
    gather_matrix_rows,
    timer,
    verify_result,
    print_matrix_info,
    get_optimal_size_for_time,
)


def matmul_blocking(comm: MPI.Comm, n: int, verify: bool = False) -> float:
    """
    Blocking matrix multiplication using MPI collectives.
    Returns elapsed time on rank 0.
    """
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Step 1: Generate matrices on root
    if rank == 0:
        print(f"[Rank {rank}] Generating {n}×{n} matrices...")
        with timer(comm, "Matrix generation"):
            A, B = generate_matrices(n)
        print_matrix_info("A", A)
        print_matrix_info("B", B)
    else:
        A = None
        B = None

    # Step 2: Broadcast matrix B to all processes
    with timer(comm, "Bcast B"):
        B = comm.bcast(B, root=0)

    # Step 3: Scatter rows of A
    local_rows, rows_per_proc, displs = split_matrix_rows(A if rank == 0 else np.empty((0, n)), comm)

    with timer(comm, "Scatter A rows"):
        A_local = scatter_matrix_rows(comm, A, local_rows, n)

    if rank == 0:
        print(f"[Rank {rank}] Local A shape: {A_local.shape}")
        print(f"[Rank {rank}] B shape: {B.shape}")
        print(f"[Rank {rank}] Rows per proc: {rows_per_proc}, Displs: {displs}")

    # Step 4: Local matrix multiplication
    with timer(comm, "Local matmul (A_local @ B)"):
        C_local = A_local @ B

    # Step 5: Gather results
    with timer(comm, "Gather C"):
        C = gather_matrix_rows(comm, C_local, rows_per_proc, displs, n)

    # Step 6: Verification (optional)
    elapsed = 0.0
    if rank == 0 and C is not None:
        print_matrix_info("C", C)

        if verify:
            with timer(comm, "Verification"):
                verify_result(C, A, B)

        elapsed = 0.0  # Timer context manager prints time

    return elapsed


def main():
    parser = argparse.ArgumentParser(description="MPI Matrix Multiplication - Blocking")
    parser.add_argument("--size", "-n", type=int, default=2000, help="Matrix size N×N")
    parser.add_argument("--verify", "-v", action="store_true", help="Verify result")
    parser.add_argument("--target-time", "-t", type=float, help="Target time in seconds (auto-size)")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    n = args.size
    if args.target_time:
        n = get_optimal_size_for_time(args.target_time, comm)

    if rank == 0:
        print("=" * 60)
        print("MPI Matrix Multiplication - BLOCKING VERSION")
        print("=" * 60)
        print(f"Matrix size: {n}×{n}")
        print(f"Processes: {comm.Get_size()}")
        print(f"Verify: {args.verify}")
        print("-" * 60)

    # Run multiplication
    elapsed = matmul_blocking(comm, n, args.verify)

    if rank == 0:
        print("-" * 60)
        print(f"Total time (rank 0): {elapsed:.4f} s")
        print("=" * 60)


if __name__ == "__main__":
    main()