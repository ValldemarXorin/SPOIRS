"""Traceroute implementation using ICMP Time Exceeded."""

import socket
import sys
import time
import struct
import random
from typing import List, Optional, Tuple
from dataclasses import dataclass

from lab5.icmp_utils import (
    ICMPPacket, ICMPType, ICMPCode,
    create_echo_request, calculate_checksum,
    create_raw_socket, send_icmp, recv_icmp,
    resolve_host, IPHeader
)


@dataclass
class HopResult:
    ttl: int
    ip: str
    hostname: Optional[str]
    rtts: List[Optional[float]]  # 3 probes per hop
    error: Optional[str] = None


class Traceroute:
    """ICMP-based traceroute."""

    def __init__(
        self,
        host: str,
        max_hops: int = 30,
        probes: int = 3,
        timeout: float = 2.0,
        payload_size: int = 56,
    ):
        self.host = host
        self.target_ip = resolve_host(host)
        self.max_hops = max_hops
        self.probes = probes
        self.timeout = timeout
        self.payload_size = payload_size
        self.identifier = random.randint(1, 65535)
        self.sequence = 0
        self.sock = create_raw_socket()
        self.sock.settimeout(timeout)

    def run(self) -> List[HopResult]:
        """Run traceroute."""
        print(f"traceroute to {self.host} ({self.target_ip}), "
              f"{self.max_hops} hops max, {self.probes} probes per hop")

        # Windows raw ICMP sockets ignore IP_TTL (and IPPROTO_RAW/IP_HDRINCL
        # is blocked without extra privileges) -> TTL-based hop discovery fails.
        if sys.platform == "win32":
            print("[WARN] Windows raw ICMP sockets ignore IP_TTL: TTL-based "
                  "traceroute may not show hops. Linux is recommended; "
                  "ping still works on Windows.")

        results = []

        for ttl in range(1, self.max_hops + 1):
            hop_rtts = []
            hop_ip = None
            hop_hostname = None
            reached_target = False

            for probe in range(self.probes):
                self.sequence += 1
                packet = create_echo_request(self.identifier, self.sequence, self.payload_size)

                # Set TTL
                self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_TTL, ttl)

                send_time = time.time()
                if not send_icmp(self.sock, packet, (self.target_ip, 0)):
                    hop_rtts.append(None)
                    continue

                # Wait for response
                result = recv_icmp(self.sock, self.timeout)
                rtt = None

                if result:
                    icmp_pkt, src_ip, ip_hdr = result
                    rtt = (time.time() - send_time) * 1000

                    if icmp_pkt.type == ICMPType.TIME_EXCEEDED and icmp_pkt.code == ICMPCode.TTL_EXCEEDED:
                        # Intermediate router
                        hop_ip = src_ip
                        try:
                            hop_hostname = socket.gethostbyaddr(src_ip)[0]
                        except socket.herror:
                            pass

                    elif icmp_pkt.type == ICMPType.ECHO_REPLY and src_ip == self.target_ip:
                        # Reached target
                        hop_ip = src_ip
                        reached_target = True

                    elif icmp_pkt.type == ICMPType.DEST_UNREACH:
                        hop_ip = src_ip
                        reached_target = True

                hop_rtts.append(rtt)

                time.sleep(0.1)  # Small delay between probes

            results.append(HopResult(
                ttl=ttl,
                ip=hop_ip or "*",
                hostname=hop_hostname,
                rtts=hop_rtts,
            ))

            # Print hop result
            self._print_hop(results[-1])

            if reached_target:
                break

        return results

    def _print_hop(self, hop: HopResult):
        """Print single hop result."""
        if hop.ip == "*":
            print(f" {hop.ttl:2d}  * * *")
            return

        rtt_strs = []
        for rtt in hop.rtts:
            if rtt is None:
                rtt_strs.append("*")
            else:
                rtt_strs.append(f"{rtt:.2f} ms")

        host_info = hop.ip
        if hop.hostname:
            host_info = f"{hop.hostname} ({hop.ip})"

        print(f" {hop.ttl:2d}  {host_info}  {'  '.join(rtt_strs)}")

    def close(self):
        self.sock.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="ICMP Traceroute")
    parser.add_argument("host", help="Target host")
    parser.add_argument("-m", "--max-hops", type=int, default=30, help="Max hops")
    parser.add_argument("-q", "--probes", type=int, default=3, help="Probes per hop")
    parser.add_argument("-t", "--timeout", type=float, default=2.0, help="Timeout per probe")
    parser.add_argument("-s", "--size", type=int, default=56, help="Payload size")
    args = parser.parse_args()

    tracer = Traceroute(
        host=args.host,
        max_hops=args.max_hops,
        probes=args.probes,
        timeout=args.timeout,
        payload_size=args.size,
    )
    try:
        tracer.run()
    finally:
        tracer.close()


if __name__ == "__main__":
    main()