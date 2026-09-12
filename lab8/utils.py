"""Utilities for Lab 8: Matrix generation, timing, verification, file ops."""

import numpy as np
import time
from contextlib import contextmanager
from typing import Tuple, Optional, Dict, List
from mpi4py import MPI


def generate_matrices(n: int, dtype=np.float64, seed: int = 42) -> Tuple[np.ndarray, np.ndarray]:
    """Generate two random matrices A and B of size n×n."""
    rng = np.random.default_rng(seed)
    A = rng.random((n, n), dtype=dtype)
    B = rng.random((n, n), dtype=dtype)
    return A, B


@contextmanager
def timer(comm: MPI.Comm, label: str = "Operation"):
    """Context manager for timing MPI operations (measures on rank 0)."""
    rank = comm.Get_rank()
    if rank == 0:
        start = time.perf_counter()
        yield
        end = time.perf_counter()
        elapsed = end - start
        print(f"[TIMER] {label}: {elapsed:.4f} s ({elapsed*1000:.2f} ms)")
    else:
        yield


def verify_result(
    C: np.ndarray,
    A: np.ndarray,
    B: np.ndarray,
    tolerance: float = 1e-10,
) -> bool:
    """Verify matrix multiplication result C = A @ B."""
    expected = A @ B
    diff = np.abs(C - expected)
    max_diff = np.max(diff)
    rel_error = max_diff / (np.max(np.abs(expected)) + 1e-15)

    print(f"[VERIFY] Max abs diff: {max_diff:.2e}, Rel error: {rel_error:.2e}")

    if rel_error < tolerance:
        print("[VERIFY] PASSED")
        return True
    else:
        print("[VERIFY] FAILED")
        return False


def print_matrix_info(name: str, matrix: np.ndarray) -> None:
    """Print matrix shape and stats."""
    print(f"[{name}] Shape: {matrix.shape}, dtype: {matrix.dtype}, "
          f"min: {np.min(matrix):.4f}, max: {np.max(matrix):.4f}, "
          f"mean: {np.mean(matrix):.4f}")


def get_optimal_size_for_time(target_seconds: float, comm: MPI.Comm) -> int:
    """Estimate matrix size for target execution time."""
    rank = comm.Get_rank()
    size = comm.Get_size()

    base_n = 2000
    base_time = 3.0
    base_procs = 4

    scaled_time = base_time * (base_procs / size)
    ratio = (target_seconds / scaled_time) ** (1/3)
    n = int(base_n * ratio)
    n = max(size, (n // size) * size)

    if rank == 0:
        print(f"[INFO] Estimated matrix size for ~{target_seconds}s: {n}×{n}")

    return n


def write_matrices_to_files(
    comm: MPI.Comm,
    filename_A: str,
    filename_B: str,
    n: int,
    seed: int = 42,
) -> None:
    """
    Generate matrices on rank 0 and write to files using MPI-IO.
    All processes participate in collective write.
    """
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Calculate local rows
    base_rows = n // size
    remainder = n % size
    local_rows = base_rows + (1 if rank < remainder else 0)
    
    if rank == 0:
        print(f"[Rank 0] Generating {n}×{n} matrices...")
        rng = np.random.default_rng(seed)
        A = rng.random((n, n), dtype=np.float64)
        B = rng.random((n, n), dtype=np.float64)
    else:
        A = None
        B = None
    
    # Scatter A rows
    A_local = np.empty((local_rows, n), dtype=np.float64)
    comm.Scatter([A, MPI.DOUBLE] if A is not None else None, 
                 [A_local, MPI.DOUBLE], root=0)
    
    # Broadcast B
    B = comm.bcast(B, root=0)
    
    # Write to files using MPI-IO (collective)
    from lab8.mpi_io import write_matrix_to_file
    write_matrix_to_file(comm, filename_A, A_local)
    write_matrix_to_file(comm, filename_B, B)


def read_matrices_for_group(
    gcomm: MPI.Comm,
    filename_A: str,
    filename_B: str,
    n: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """Read matrix portions for a group communicator."""
    from lab8.mpi_io import read_matrix_from_file
    return (
        read_matrix_from_file(gcomm, filename_A, (n, n)),
        read_matrix_from_file(gcomm, filename_B, (n, n)),
    )


def write_result_for_group(
    gcomm: MPI.Comm,
    filename: str,
    C_local: np.ndarray,
) -> None:
    """Write group's result matrix to file."""
    from lab8.mpi_io import write_matrix_to_file
    write_matrix_to_file(gcomm, filename, C_local)


def compare_times_blocking_vs_collective(
    comm: MPI.Comm,
    blocking_times: List[float],
    collective_times: List[float],
) -> None:
    """Print comparison of blocking vs collective operation times."""
    rank = comm.Get_rank()
    if rank == 0:
        print("\n" + "=" * 60)
        print("PERFORMANCE COMPARISON: Blocking vs Collective")
        print("=" * 60)
        print(f"{'Group':<8} {'Blocking (s)':<15} {'Collective (s)':<15} {'Speedup':<10}")
        print("-" * 60)
        for i, (bt, ct) in enumerate(zip(blocking_times, collective_times)):
            speedup = bt / ct if ct > 0 else 0
            print(f"{i:<8} {bt:<15.4f} {ct:<15.4f} {speedup:<10.2f}x")
        print("=" * 60)


# For backward compatibility with Lab 7 style
split_matrix_rows = lambda matrix, comm: (0, np.array([]), np.array([]))  # Placeholder