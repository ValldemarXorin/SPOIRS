"""ICMP utilities: checksum, packet building, parsing."""

import struct
import socket
import time
import random
import os
from typing import Tuple, Optional, List
from dataclasses import dataclass
from enum import IntEnum


class ICMPType(IntEnum):
    ECHO_REPLY = 0
    DEST_UNREACH = 3
    SRC_QUENCH = 4
    REDIRECT = 5
    ECHO_REQUEST = 8
    TIME_EXCEEDED = 11
    PARAM_PROBLEM = 12
    TIMESTAMP = 13
    TIMESTAMP_REPLY = 14
    INFO_REQUEST = 15
    INFO_REPLY = 16


class ICMPCode(IntEnum):
    # Destination Unreachable codes
    NET_UNREACH = 0
    HOST_UNREACH = 1
    PROTO_UNREACH = 2
    PORT_UNREACH = 3
    FRAG_NEEDED = 4
    SRC_ROUTE_FAILED = 5

    # Time Exceeded codes
    TTL_EXCEEDED = 0
    FRAG_REASSEMBLY_EXCEEDED = 1


@dataclass
class ICMPPacket:
    type: int
    code: int
    checksum: int
    id: int
    seq: int
    payload: bytes

    def pack(self) -> bytes:
        """Pack ICMP packet with correct checksum."""
        # Header without checksum
        header = struct.pack("!BBHHH", self.type, self.code, 0, self.id, self.seq)
        packet = header + self.payload
        checksum = calculate_checksum(packet)
        # Re-pack with correct checksum
        header = struct.pack("!BBHHH", self.type, self.code, checksum, self.id, self.seq)
        return header + self.payload

    @staticmethod
    def parse(data: bytes) -> "ICMPPacket":
        """Parse ICMP packet from raw bytes."""
        if len(data) < 8:
            raise ValueError("ICMP packet too small")
        type_, code, checksum, id_, seq = struct.unpack("!BBHHH", data[:8])
        payload = data[8:]
        return ICMPPacket(type_, code, checksum, id_, seq, payload)


@dataclass
class IPHeader:
    version: int
    ihl: int
    tos: int
    total_length: int
    id: int
    flags: int
    fragment_offset: int
    ttl: int
    protocol: int
    checksum: int
    src_ip: str
    dst_ip: str
    options: bytes = b""

    @staticmethod
    def parse(data: bytes) -> "IPHeader":
        if len(data) < 20:
            raise ValueError("IP header too small")
        version_ihl = data[0]
        version = version_ihl >> 4
        ihl = version_ihl & 0x0F
        header_len = ihl * 4
        if len(data) < header_len:
            raise ValueError("IP header truncated")

        tos, total_length, id_, flags_frag, ttl, protocol, checksum, src, dst = struct.unpack(
            "!BBHHHBBHII", data[:20]
        )
        flags = (flags_frag >> 13) & 0x7
        fragment_offset = flags_frag & 0x1FFF

        src_ip = socket.inet_ntoa(struct.pack("!I", src))
        dst_ip = socket.inet_ntoa(struct.pack("!I", dst))
        options = data[20:header_len] if header_len > 20 else b""

        return IPHeader(version, ihl, tos, total_length, id_, flags,
                       fragment_offset, ttl, protocol, checksum, src_ip, dst_ip, options)


@dataclass
class PingResult:
    host: str
    seq: int
    rtt: Optional[float] = None
    reply_type: Optional[int] = None
    reply_code: Optional[int] = None
    error: Optional[str] = None


def calculate_checksum(data: bytes) -> int:
    """Calculate Internet checksum (RFC 1071)."""
    if len(data) % 2:
        data += b"\x00"
    s = sum(struct.unpack("!%dH" % (len(data) // 2), data))
    s = (s >> 16) + (s & 0xFFFF)
    s += s >> 16
    return ~s & 0xFFFF


def create_echo_request(identifier: int, sequence: int, payload_size: int = 56) -> ICMPPacket:
    """Create ICMP Echo Request packet with timestamp in payload."""
    timestamp = struct.pack("!d", time.time())
    payload = timestamp + os.urandom(max(0, payload_size - len(timestamp)))
    return ICMPPacket(
        type=ICMPType.ECHO_REQUEST,
        code=0,
        checksum=0,
        id=identifier & 0xFFFF,
        seq=sequence & 0xFFFF,
        payload=payload,
    )


def create_raw_socket() -> socket.socket:
    """Create raw socket for ICMP. Requires root/Admin privileges."""
    try:
        # Linux
        sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
    except (AttributeError, OSError):
        # Windows - need IPPROTO_IP with IP_HDRINCL
        sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_IP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
    sock.settimeout(1.0)
    return sock


def send_icmp(sock: socket.socket, packet: ICMPPacket, dest_addr: Tuple[str, int]) -> bool:
    """Send ICMP packet."""
    try:
        sock.sendto(packet.pack(), dest_addr)
        return True
    except OSError as e:
        print(f"Send error: {e}")
        return False


def recv_icmp(sock: socket.socket, timeout: float = 1.0) -> Optional[Tuple[ICMPPacket, str, IPHeader]]:
    """Receive ICMP packet with IP header."""
    try:
        sock.settimeout(timeout)
        data, addr = sock.recvfrom(65536)
        src_ip = addr[0]

        # Parse IP header
        ip_header = IPHeader.parse(data)

        # ICMP starts after IP header
        icmp_data = data[ip_header.ihl * 4:]
        icmp_packet = ICMPPacket.parse(icmp_data)

        return icmp_packet, src_ip, ip_header

    except socket.timeout:
        return None
    except OSError:
        return None
    except Exception as e:
        print(f"Recv error: {e}")
        return None


def resolve_host(host: str) -> str:
    """Resolve hostname to IP address."""
    try:
        return socket.gethostbyname(host)
    except socket.gaierror:
        return host


def get_random_id() -> int:
    """Generate random identifier for ICMP packets."""
    return random.randint(1, 65535)