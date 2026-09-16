"""Lab 5 - ICMP Ping, Traceroute, Smurf - Entry point."""

import sys
import argparse

from lab5.ping import main as ping_main
from lab5.traceroute import main as trace_main
from lab5.smurf import main as smurf_main, demo_with_wireshark_instructions


def main():
    parser = argparse.ArgumentParser(description="Lab 5: ICMP Tools")
    subparsers = parser.add_subparsers(dest="command", help="Sub-command")

    # Ping subcommand
    ping_parser = subparsers.add_parser("ping", help="Parallel ping multiple hosts")
    ping_parser.add_argument("hosts", nargs="+", help="Hosts to ping")
    ping_parser.add_argument("-c", "--count", type=int, default=4)
    ping_parser.add_argument("-i", "--interval", type=float, default=1.0)
    ping_parser.add_argument("-t", "--timeout", type=float, default=2.0)
    ping_parser.add_argument("-s", "--size", type=int, default=56)
    ping_parser.add_argument("-w", "--workers", type=int, default=10)

    # Traceroute subcommand
    trace_parser = subparsers.add_parser("trace", help="Traceroute to host")
    trace_parser.add_argument("host", help="Target host")
    trace_parser.add_argument("-m", "--max-hops", type=int, default=30)
    trace_parser.add_argument("-q", "--probes", type=int, default=3)
    trace_parser.add_argument("-t", "--timeout", type=float, default=2.0)
    trace_parser.add_argument("-s", "--size", type=int, default=56)

    # Smurf subcommand
    smurf_parser = subparsers.add_parser("smurf", help="Smurf attack demo (educational)")
    smurf_parser.add_argument("--victim", help="Victim IP (spoofed source)")
    smurf_parser.add_argument("--broadcast", help="Broadcast IP")
    smurf_parser.add_argument("-c", "--count", type=int, default=5)
    smurf_parser.add_argument("-i", "--interval", type=float, default=0.2)
    smurf_parser.add_argument("-s", "--size", type=int, default=56)
    smurf_parser.add_argument("--wireshark-help", action="store_true")

    args = parser.parse_args()

    if args.command == "ping":
        # Temporarily replace sys.argv for ping_main
        sys.argv = ["ping"] + [str(x) for x in [
            "-c", args.count, "-i", args.interval, "-t", args.timeout,
            "-s", args.size, "-w", args.workers
        ] + args.hosts]
        ping_main()
    elif args.command == "trace":
        sys.argv = ["trace"] + [str(x) for x in [
            "-m", args.max_hops, "-q", args.probes, "-t", args.timeout,
            "-s", args.size, args.host
        ]]
        trace_main()
    elif args.command == "smurf":
        argv = ["smurf"]
        if args.victim:
            argv += ["--victim", args.victim]
        if args.broadcast:
            argv += ["--broadcast", args.broadcast]
        argv += ["-c", str(args.count), "-i", str(args.interval), "-s", str(args.size)]
        if args.wireshark_help:
            argv += ["--wireshark-help"]
        sys.argv = argv
        smurf_main()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()