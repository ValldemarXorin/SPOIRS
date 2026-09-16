"""Utilities for MPI matrix multiplication: generation, timing, verification."""

import numpy as np
import time
from contextlib import contextmanager
from typing import Tuple, Optional
try:
    from mpi4py import MPI
except (ImportError, OSError, RuntimeError) as _mpi_err:
    raise ImportError(
        'mpi4py/MPI runtime не найден. Установите MPI:\\n'
        '  Linux:  sudo apt-get install openmpi-bin libopenmpi-dev python3-dev && pip install mpi4py\\n'
        '  Windows: MS-MPI (https://docs.microsoft.com/en-us/message-passing-interface/microsoft-mpi) + pip install mpi4py\\n'
        f'Ошибка: {_mpi_err}'
    ) from _mpi_err


def generate_matrices(n: int, dtype=np.float64, seed: int = 42) -> Tuple[np.ndarray, np.ndarray]:
    """Generate two random matrices A and B of size n×n."""
    rng = np.random.default_rng(seed)
    A = rng.random((n, n), dtype=dtype)
    B = rng.random((n, n), dtype=dtype)
    return A, B


def split_matrix_rows(matrix: np.ndarray, comm: MPI.Comm) -> Tuple[np.ndarray, int, int]:
    """
    Split matrix rows across processes.
    Returns: (local_rows, rows_per_proc, displs)
    """
    rank = comm.Get_rank()
    size = comm.Get_size()
    n = matrix.shape[0]

    # Calculate rows per process (handle uneven division)
    base_rows = n // size
    remainder = n % size

    rows_per_proc = np.full(size, base_rows, dtype=int)
    rows_per_proc[:remainder] += 1

    displs = np.zeros(size, dtype=int)
    displs[1:] = np.cumsum(rows_per_proc)[:-1]

    local_rows = rows_per_proc[rank]
    return local_rows, rows_per_proc, displs


def scatter_matrix_rows(
    comm: MPI.Comm,
    matrix: Optional[np.ndarray],
    local_rows: int,
    n_cols: int,
    dtype=np.float64,
) -> np.ndarray:
    """
    Scatter matrix rows from root to all processes.
    Uses Scatterv for uneven distribution.
    """
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        # Calculate distribution
        base_rows = matrix.shape[0] // size
        remainder = matrix.shape[0] % size
        sendcounts = np.full(size, base_rows * n_cols, dtype=int)
        sendcounts[:remainder] += n_cols
        displs = np.zeros(size, dtype=int)
        displs[1:] = np.cumsum(sendcounts)[:-1]
        sendbuf = matrix.ravel()
    else:
        sendbuf = None
        sendcounts = None
        displs = None

    recvbuf = np.empty(local_rows * n_cols, dtype=dtype)

    comm.Scatterv([sendbuf, sendcounts, displs, MPI.DOUBLE], recvbuf, root=0)

    return recvbuf.reshape(local_rows, n_cols)


def gather_matrix_rows(
    comm: MPI.Comm,
    local_matrix: np.ndarray,
    rows_per_proc: np.ndarray,
    displs: np.ndarray,
    n_cols: int,
    dtype=np.float64,
) -> Optional[np.ndarray]:
    """
    Gather matrix rows from all processes to root.
    Uses Gatherv for uneven distribution.
    """
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        total_rows = int(np.sum(rows_per_proc))
        recvbuf = np.empty(total_rows * n_cols, dtype=dtype)
    else:
        recvbuf = None

    sendbuf = local_matrix.ravel()
    sendcount = local_matrix.size

    comm.Gatherv(sendbuf, [recvbuf, rows_per_proc * n_cols, displs * n_cols, MPI.DOUBLE], root=0)

    if rank == 0:
        return recvbuf.reshape(-1, n_cols)
    return None


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
    """
    Estimate matrix size for target execution time.
    Rough heuristic based on O(N^3) complexity.
    """
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Baseline: 2000x2000 on 4 procs ~ 2-5 seconds
    # Scale: time ~ N^3 / size
    base_n = 2000
    base_time = 3.0  # seconds
    base_procs = 4

    # Adjust for process count
    scaled_time = base_time * (base_procs / size)

    # Solve for N: target / scaled = (N/base)^3
    ratio = (target_seconds / scaled_time) ** (1/3)
    n = int(base_n * ratio)

    # Round to multiple of size for even distribution
    n = max(size, (n // size) * size)

    if rank == 0:
        print(f"[INFO] Estimated matrix size for ~{target_seconds}s: {n}×{n}")

    return n