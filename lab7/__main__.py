"""Lab 7 MPI Matrix Multiplication - Entry point."""

import sys
import subprocess
from mpi4py import MPI


def run_blocking(args):
    """Run blocking version."""
    cmd = [sys.executable, "-m", "lab7.matmul_blocking"] + args
    subprocess.run(cmd)


def run_nonblocking(args):
    """Run non-blocking version."""
    cmd = [sys.executable, "-m", "lab7.matmul_nonblocking"] + args
    subprocess.run(cmd)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Lab 7: MPI Matrix Multiplication")
    subparsers = parser.add_subparsers(dest="mode", help="Mode")

    # Blocking subcommand
    block_parser = subparsers.add_parser("blocking", help="Blocking version (MPI_Send/Recv)")
    block_parser.add_argument("--size", "-n", type=int, default=2000)
    block_parser.add_argument("--verify", "-v", action="store_true")
    block_parser.add_argument("--target-time", "-t", type=float)

    # Non-blocking subcommand
    nb_parser = subparsers.add_parser("nonblocking", help="Non-blocking version (MPI_Isend/Irecv)")
    nb_parser.add_argument("--size", "-n", type=int, default=2000)
    nb_parser.add_argument("--verify", "-v", action="store_true")
    nb_parser.add_argument("--target-time", "-t", type=float)

    # Compare subcommand
    cmp_parser = subparsers.add_parser("compare", help="Run both and compare")
    cmp_parser.add_argument("--size", "-n", type=int, default=2000)
    cmp_parser.add_argument("--verify", "-v", action="store_true")

    args = parser.parse_args()

    if args.mode == "blocking":
        run_blocking(sys.argv[2:])
    elif args.mode == "nonblocking":
        run_nonblocking(sys.argv[2:])
    elif args.mode == "compare":
        print("Running BLOCKING version...")
        run_blocking(["--size", str(args.size)] + (["--verify"] if args.verify else []))
        print("\n" + "="*60 + "\n")
        print("Running NON-BLOCKING version...")
        run_nonblocking(["--size", str(args.size)] + (["--verify"] if args.verify else []))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()