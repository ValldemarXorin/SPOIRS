"""Group management for Lab 8: MPI_Comm_split with random group sizes."""

import numpy as np
from mpi4py import MPI
from typing import List, Tuple
from dataclasses import dataclass


@dataclass
class GroupInfo:
    group_id: int
    comm: MPI.Comm
    size: int
    rank: int
    is_member: bool


def create_random_groups(
    comm: MPI.Comm,
    num_groups: int,
    seed: int = 42,
) -> List[GroupInfo]:
    """
    Create random process groups using MPI_Comm_split.
    
    Args:
        comm: Parent communicator (usually MPI.COMM_WORLD)
        num_groups: Number of groups to create
        seed: Random seed for reproducibility
    
    Returns:
        List of GroupInfo for all groups (including non-member groups as COMM_NULL)
    """
    rank = comm.Get_rank()
    size = comm.Get_size()

    if num_groups <= 0:
        num_groups = 1
    if num_groups > size:
        num_groups = size

    # Generate random group sizes that sum to 'size'
    rng = np.random.default_rng(seed)
    
    # Ensure each group has at least 1 process
    # Start with 1 process per group
    group_sizes = np.ones(num_groups, dtype=int)
    remaining = size - num_groups
    
    if remaining > 0:
        # Distribute remaining processes randomly
        extra = rng.multinomial(remaining, np.ones(num_groups) / num_groups)
        group_sizes += extra

    # Assign group_id to each rank
    group_ids = np.zeros(size, dtype=int)
    idx = 0
    for gid, gsize in enumerate(group_sizes):
        group_ids[idx:idx + gsize] = gid
        idx += gsize

    # Shuffle assignment for randomness (but keep sizes)
    rng.shuffle(group_ids)

    my_group_id = group_ids[rank]

    # Split communicator
    # Use group_id as color, rank as key for ordering within group
    new_comm = comm.Split(my_group_id, rank)

    # Gather group info on all ranks
    all_group_ids = comm.allgather(my_group_id)
    all_ranks = comm.allgather(rank)

    # Build GroupInfo list
    groups = []
    for gid in range(num_groups):
        # Find ranks in this group
        member_ranks = [r for r, g in zip(all_ranks, all_group_ids) if g == gid]
        gsize = len(member_ranks)
        
        if rank in member_ranks:
            # This rank is a member
            # Get the group communicator (already created above)
            groups.append(GroupInfo(
                group_id=gid,
                comm=new_comm,
                size=gsize,
                rank=new_comm.Get_rank(),
                is_member=True,
            ))
        else:
            # Not a member of this group
            groups.append(GroupInfo(
                group_id=gid,
                comm=MPI.COMM_NULL,
                size=gsize,
                rank=-1,
                is_member=False,
            ))

    return groups


def print_group_info(groups: List[GroupInfo], comm: MPI.Comm) -> None:
    """Print group configuration on rank 0."""
    rank = comm.Get_rank()
    if rank == 0:
        print(f"[GROUP CONFIG] Total groups: {len(groups)}")
        for g in groups:
            if g.is_member:
                print(f"  Group {g.group_id}: {g.size} processes, ranks: {comm.allgather(rank) if g.comm != MPI.COMM_NULL else 'N/A'}")
            else:
                print(f"  Group {g.group_id}: {g.size} processes (not member)")


def get_my_group(groups: List[GroupInfo]) -> GroupInfo:
    """Get the GroupInfo for the current process's group."""
    for g in groups:
        if g.is_member:
            return g
    # Should not happen if num_groups <= size
    return GroupInfo(-1, MPI.COMM_NULL, 0, -1, False)