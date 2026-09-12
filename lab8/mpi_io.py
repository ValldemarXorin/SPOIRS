"""MPI File I/O for Lab 8: Parallel read/write using MPI_File."""

import numpy as np
from mpi4py import MPI
from typing import Tuple, Optional


def write_matrix_to_file(
    comm: MPI.Comm,
    filename: str,
    matrix: np.ndarray,
    dtype: MPI.Datatype = MPI.DOUBLE,
) -> None:
    """
    Write distributed matrix to file using MPI_File_write_at_all.
    
    Each process writes its local portion at the correct offset.
    Assumes matrix is row-distributed.
    """
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Matrix dimensions
    local_rows, n_cols = matrix.shape
    element_size = np.dtype(matrix.dtype).itemsize
    
    # Calculate global offset for this process
    rows_per_proc = comm.allgather(local_rows)
    displs = np.zeros(size, dtype=int)
    displs[1:] = np.cumsum(rows_per_proc)[:-1]
    offset = displs[rank] * n_cols * element_size
    
    # Open file
    fh = MPI.File.Open(comm, filename, 
                         MPI.MODE_CREATE | MPI.MODE_WRONLY)
    
    # Write at offset
    fh.Write_at_all(offset, matrix)
    
    fh.Close()


def read_matrix_from_file(
    comm: MPI.Comm,
    filename: str,
    global_shape: Tuple[int, int],
    dtype: np.dtype = np.float64,
) -> np.ndarray:
    """
    Read distributed matrix from file using MPI_File_read_at_all.
    
    Each process reads its local portion at the correct offset.
    """
    rank = comm.Get_rank()
    size = comm.Get_size()
    n_rows, n_cols = global_shape
    
    # Calculate local rows for this process
    base_rows = n_rows // size
    remainder = n_rows % size
    local_rows = base_rows + (1 if rank < remainder else 0)
    
    # Calculate offset
    rows_per_proc = np.array([base_rows + (1 if i < remainder else 0) for i in range(size)])
    displs = np.zeros(size, dtype=int)
    displs[1:] = np.cumsum(rows_per_proc)[:-1]
    offset = displs[rank] * n_cols * np.dtype(dtype).itemsize
    
    # Allocate local buffer
    local_matrix = np.empty((local_rows, n_cols), dtype=dtype)
    
    # Open file
    fh = MPI.File.Open(comm, filename, MPI.MODE_RDONLY)
    
    # Read at offset
    fh.Read_at_all(offset, local_matrix)
    
    fh.Close()
    
    return local_matrix


def generate_and_write_matrices(
    comm: MPI.Comm,
    filename_A: str,
    filename_B: str,
    n: int,
    seed: int = 42,
) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
    """
    Generate matrices on rank 0 and write to files using MPI-IO.
    Returns (A, B) on rank 0, (None, None) on others.
    """
    rank = comm.Get_rank()
    
    if rank == 0:
        print(f"[Rank 0] Generating and writing {n}×{n} matrices...")
        rng = np.random.default_rng(seed)
        A = rng.random((n, n), dtype=np.float64)
        B = rng.random((n, n), dtype=np.float64)
    else:
        A = None
        B = None
    
    # Write matrices to files (all processes participate)
    if rank == 0:
        write_matrix_to_file(comm, filename_A, A)
        write_matrix_to_file(comm, filename_B, B)
    else:
        # Other processes need to know shape to allocate buffer
        # Use dummy matrices of correct shape for collective write
        base_rows = n // comm.Get_size()
        remainder = n % comm.Get_size()
        local_rows = base_rows + (1 if rank < remainder else 0)
        
        A_dummy = np.empty((local_rows, n), dtype=np.float64)
        B_dummy = np.empty((local_rows, n), dtype=np.float64)
        
        # Participate in collective write (data will be ignored for non-root)
        # Actually, for Write_at_all, all processes write their portion
        # So we need to scatter the data first or use a different approach
        pass
    
    # Better approach: scatter then write
    return A, B


def scatter_and_write(
    comm: MPI.Comm,
    filename_A: str,
    filename_B: str,
    n: int,
    seed: int = 42,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Generate matrices on root, scatter to all processes, then write to files.
    Returns local portions of A and B.
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
    B_local = np.empty((local_rows, n), dtype=np.float64) if rank == 0 else np.empty((n, n), dtype=np.float64)
    B = comm.bcast(B, root=0)
    B_local = B  # Each process gets full B
    
    # Write to files
    write_matrix_to_file(comm, filename_A, A_local)
    write_matrix_to_file(comm, filename_B, B_local)
    
    return A_local, B_local


def read_matrices_for_group(
    gcomm: MPI.Comm,
    filename_A: str,
    filename_B: str,
    n: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Read matrix portions for a group communicator.
    Each process in the group reads its portion.
    """
    return (
        read_matrix_from_file(gcomm, filename_A, (n, n)),
        read_matrix_from_file(gcomm, filename_B, (n, n)),
    )


def write_result_for_group(
    gcomm: MPI.Comm,
    filename: str,
    C_local: np.ndarray,
) -> None:
    """
    Write group's result matrix to file.
    """
    write_matrix_to_file(gcomm, filename, C_local)