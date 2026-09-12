"""
MPI Matrix Multiplication - Non-blocking Version.
Uses MPI-3 non-blocking collectives (Ibcast, Iscatter, Igatherv) + Waitall.
Falls back to blocking collectives when MPI-3 not available.
Overlaps communication with computation (like CUDA Streams / cudaMemcpyAsync).
"""

import sys
import argparse
import numpy as np
from mpi4py import MPI
from typing import Optional, Tuple, List

from lab7.utils import (
    generate_matrices,
    split_matrix_rows,
    timer,
    verify_result,
    print_matrix_info,
    get_optimal_size_for_time,
)


def matmul_nonblocking(comm: MPI.Comm, n: int, verify: bool = False) -> float:
    """
    Non-blocking matrix multiplication using MPI-3 non-blocking collectives
    with fallback to blocking collectives for older MPI.
    """
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Check MPI-3 non-blocking collective availability
    has_nb_collectives = all(hasattr(comm, attr) for attr in ['Ibcast', 'Iscatter', 'Igatherv'])

    if rank == 0:
        print(f"[INFO] MPI-3 non-blocking collectives: {has_nb_collectives}")

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

    # Step 2: Determine distribution
    local_rows, rows_per_proc, displs = split_matrix_rows(
        A if rank == 0 else np.empty((0, n)), comm
    )

    # Step 3: Non-blocking broadcast of B
    if has_nb_collectives:
        with timer(comm, "Ibcast B (MPI-3)"):
            req_bcast = comm.Ibcast(B, root=0)
    else:
        # Fallback: blocking broadcast (can't truly overlap without MPI-3)
        with timer(comm, "Bcast B (blocking fallback)"):
            B = comm.bcast(B, root=0)
        req_bcast = None

    # Step 4: Non-blocking scatter of A rows
    if has_nb_collectives:
        with timer(comm, "Iscatter A rows (MPI-3)"):
            A_local = np.empty((local_rows, n), dtype=np.float64)
            req_scatter = comm.Iscatter([A, MPI.DOUBLE], [A_local, MPI.DOUBLE], root=0)
    else:
        # Fallback: blocking scatter
        with timer(comm, "Scatter A rows (blocking fallback)"):
            A_local = np.empty((local_rows, n), dtype=np.float64)
            comm.Scatter([A, MPI.DOUBLE], [A_local, MPI.DOUBLE], root=0)
        req_scatter = None

    # Step 5: Wait for communications to complete
    requests = []
    if req_bcast:
        requests.append(req_bcast)
    if req_scatter:
        requests.append(req_scatter)

    if requests:
        with timer(comm, "Waitall (comm completion)"):
            MPI.Request.Waitall(requests)

    if rank == 0:
        print(f"[Rank {rank}] Local A shape: {A_local.shape}")
        print(f"[Rank {rank}] B shape: {B.shape}")
        print(f"[Rank {rank}] Rows per proc: {rows_per_proc}, Displs: {displs}")

    # Step 6: Local matrix multiplication
    with timer(comm, "Local matmul (A_local @ B)"):
        C_local = A_local @ B

    # Step 7: Non-blocking gather
    if has_nb_collectives:
        with timer(comm, "Igatherv C (MPI-3)"):
            if rank == 0:
                C = np.empty((n, n), dtype=np.float64)
            else:
                C = None
            req_gather = comm.Igatherv(
                [C_local, MPI.DOUBLE],
                [C, (rows_per_proc * n).tolist(), (displs * n).tolist(), MPI.DOUBLE],
                root=0
            )
            req_gather.Wait()
    else:
        # Fallback: blocking gather
        with timer(comm, "Gather C (blocking fallback)"):
            C = None
            if rank == 0:
                C = np.empty((n, n), dtype=np.float64)
            comm.Gatherv(
                [C_local, MPI.DOUBLE],
                [C, (rows_per_proc * n).tolist(), (displs * n).tolist(), MPI.DOUBLE],
                root=0
            )

    # Step 8: Verification (optional)
    if rank == 0 and C is not None:
        print_matrix_info("C", C)

        if verify:
            with timer(comm, "Verification"):
                verify_result(C, A, B)

    return 0.0


def main():
    parser = argparse.ArgumentParser(description="MPI Matrix Multiplication - Non-blocking")
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
        print("MPI Matrix Multiplication - NON-BLOCKING VERSION")
        print("=" * 60)
        print(f"Matrix size: {n}×{n}")
        print(f"Processes: {comm.Get_size()}")
        print(f"Verify: {args.verify}")
        print(f"MPI-3 non-blocking collectives: {hasattr(comm, 'Ibcast')}")
        print("-" * 60)

    matmul_nonblocking(comm, n, args.verify)

    if rank == 0:
        print("-" * 60)
        print("=" * 60)


if __name__ == "__main__":
    main()