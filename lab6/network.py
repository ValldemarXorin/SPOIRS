"""Network layer: local IP detection, broadcast calc, sockets (Windows+Linux safe).

Follows the logic of the reference implementation (lr6.py):
- local IP is taken from the interface used to reach the default route;
- mobile hotspots (172.20.10.x, iPhone) use a /28 network, so the broadcast
  address is computed specially;
- ONE receive socket (bound + joined to multicast) receives both broadcast
  and multicast; one separate send socket does broadcast/multicast sends.
"""

import socket
import struct
from typing import Optional, Tuple

DEFAULT_PORT = 50050
MULTICAST_GROUP = "239.255.0.1"


def get_local_ip() -> str:
    """Detect the IP of the interface used to reach the default route."""
    temp_s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        temp_s.connect(("8.8.8.8", 80))
        return temp_s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        temp_s.close()


def calc_broadcast(ip: str) -> str:
    """Compute the subnet broadcast address.

    Mobile hotspots (e.g. iPhone 172.20.10.x) use /28, so the plain
    "x.y.z.255" formula would point outside the subnet — broadcast is
    172.20.10.15 instead.
    """
    if ip.startswith("172.20.10."):
        return "172.20.10.15"
    if ip == "127.0.0.1":
        return "255.255.255.255"
    parts = ip.split(".")
    return f"{parts[0]}.{parts[1]}.{parts[2]}.255"


def create_recv_socket(port: int) -> socket.socket:
    """One bound socket receiving both broadcast and multicast datagrams."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Linux/macOS: allow several instances on one host.
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except socket.error:
            pass
    sock.bind(("", port))
    sock.settimeout(0.5)
    return sock


def create_send_socket() -> socket.socket:
    """Separate socket for broadcast/multicast/unicast sends."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    return sock


def join_multicast(sock: socket.socket, group: str) -> bool:
    """Join a multicast group on the receive socket."""
    try:
        mreq = struct.pack("4s4s", socket.inet_aton(group), socket.inet_aton("0.0.0.0"))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        return True
    except OSError:
        return False


def leave_multicast(sock: socket.socket, group: str) -> bool:
    """Leave a multicast group on the receive socket."""
    try:
        mreq = struct.pack("4s4s", socket.inet_aton(group), socket.inet_aton("0.0.0.0"))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
        return True
    except OSError:
        return False


class NetworkManager:
    """High-level network manager for P2P chat."""

    def __init__(self, port: int = DEFAULT_PORT, multicast_group: str = MULTICAST_GROUP):
        self.port = port
        self.multicast_group = multicast_group
        self.ip: str = "127.0.0.1"
        self.broadcast_ip: str = "255.255.255.255"
        self.recv_sock: Optional[socket.socket] = None
        self.send_sock: Optional[socket.socket] = None
        self.in_multicast_group: bool = False

    def initialize(self) -> bool:
        """Detect local IP/broadcast and create sockets."""
        self.ip = get_local_ip()
        self.broadcast_ip = calc_broadcast(self.ip)
        self.recv_sock = create_recv_socket(self.port)
        self.send_sock = create_send_socket()
        self.join_multicast()
        return True

    def join_multicast(self) -> bool:
        if join_multicast(self.recv_sock, self.multicast_group):
            self.in_multicast_group = True
        return self.in_multicast_group

    def leave_multicast(self) -> None:
        if self.in_multicast_group and self.recv_sock:
            leave_multicast(self.recv_sock, self.multicast_group)
            self.in_multicast_group = False

    def get_sockets(self) -> Tuple[Optional[socket.socket], Optional[socket.socket]]:
        return self.recv_sock, self.send_sock

    def get_broadcast_addr(self) -> Tuple[str, int]:
        return (self.broadcast_ip, self.port)

    def get_multicast_addr(self) -> Tuple[str, int]:
        return (self.multicast_group, self.port)

    def send(self, data: bytes, mode: str) -> bool:
        """Send datagram in the given mode (BROADCAST/MULTICAST)."""
        if mode == "BROADCAST":
            self.send_sock.sendto(data, (self.broadcast_ip, self.port))
            return True
        elif mode == "MULTICAST" and self.in_multicast_group:
            self.send_sock.sendto(data, (self.multicast_group, self.port))
            return True
        return False

    def send_unicast(self, data: bytes, ip: str) -> bool:
        """Send a unicast datagram to a specific peer."""
        try:
            self.send_sock.sendto(data, (ip, self.port))
            return True
        except OSError:
            return False

    def cleanup(self) -> None:
        self.leave_multicast()
        if self.recv_sock:
            self.recv_sock.close()
        if self.send_sock:
            self.send_sock.close()