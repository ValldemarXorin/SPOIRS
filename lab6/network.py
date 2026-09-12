"""Network utilities: interface detection, broadcast/multicast sockets."""

import socket
import struct
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


@dataclass
class InterfaceInfo:
    name: str
    ip: str
    netmask: str
    broadcast: str
    is_loopback: bool = False
    is_up: bool = True


def get_interfaces() -> List[InterfaceInfo]:
    """Get all IPv4 interfaces with IP, netmask, and broadcast address."""
    interfaces = []

    if HAS_PSUTIL:
        return _get_interfaces_psutil()
    else:
        return _get_interfaces_socket()


def _get_interfaces_psutil() -> List[InterfaceInfo]:
    """Use psutil for cross-platform interface detection."""
    interfaces = []
    for name, addrs in psutil.net_if_addrs().items():
        ipv4_addrs = [a for a in addrs if a.family == socket.AF_INET]
        if not ipv4_addrs:
            continue

        for addr in ipv4_addrs:
            ip = addr.address
            netmask = addr.netmask or "255.255.255.0"
            broadcast = addr.broadcast or _calc_broadcast(ip, netmask)
            is_loopback = ip.startswith("127.")

            interfaces.append(InterfaceInfo(
                name=name,
                ip=ip,
                netmask=netmask,
                broadcast=broadcast,
                is_loopback=is_loopback,
            ))
    return interfaces


def _get_interfaces_socket() -> List[InterfaceInfo]:
    """Fallback: use socket/getaddrinfo for interface detection."""
    interfaces = []
    try:
        hostname = socket.gethostname()
        addrs = socket.getaddrinfo(hostname, None, socket.AF_INET)
        seen = set()
        for _, _, _, _, sockaddr in addrs:
            ip = sockaddr[0]
            if ip in seen or ip.startswith("127."):
                continue
            seen.add(ip)
            netmask = "255.255.255.0"
            broadcast = _calc_broadcast(ip, netmask)
            interfaces.append(InterfaceInfo(
                name="auto",
                ip=ip,
                netmask=netmask,
                broadcast=broadcast,
                is_loopback=False,
            ))
    except Exception:
        pass
    return interfaces


def _calc_broadcast(ip: str, netmask: str) -> str:
    """Calculate broadcast address from IP and netmask."""
    ip_int = struct.unpack("!I", socket.inet_aton(ip))[0]
    mask_int = struct.unpack("!I", socket.inet_aton(netmask))[0]
    bcast_int = ip_int | (~mask_int & 0xFFFFFFFF)
    return socket.inet_ntoa(struct.pack("!I", bcast_int))


def select_interface(
    interfaces: List[InterfaceInfo],
    preferred_name: Optional[str] = None,
    preferred_ip: Optional[str] = None,
) -> Optional[InterfaceInfo]:
    """Select best interface: by name, by IP, or first non-loopback."""
    if not interfaces:
        return None

    if preferred_name:
        for iface in interfaces:
            if iface.name == preferred_name:
                return iface

    if preferred_ip:
        for iface in interfaces:
            if iface.ip == preferred_ip:
                return iface

    for iface in interfaces:
        if not iface.is_loopback:
            return iface

    return interfaces[0]


def create_broadcast_socket(
    port: int,
    bind_ip: str = "0.0.0.0",
    buffer_size: int = 65536,
) -> socket.socket:
    """Create UDP socket with SO_BROADCAST enabled."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
        target = buffer_size
        while target >= 65536:
            try:
                sock.setsockopt(socket.SOL_SOCKET, opt, target)
                break
            except OSError:
                target //= 2

    sock.bind((bind_ip, port))
    sock.setblocking(False)
    return sock


def create_multicast_socket(
    port: int,
    multicast_group: str = "239.255.0.1",
    bind_ip: str = "0.0.0.0",
    ttl: int = 2,
    loop: bool = True,
    buffer_size: int = 65536,
    interface_ip: Optional[str] = None,
) -> socket.socket:
    """Create UDP socket joined to multicast group."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
        target = buffer_size
        while target >= 65536:
            try:
                sock.setsockopt(socket.SOL_SOCKET, opt, target)
                break
            except OSError:
                target //= 2

    sock.bind((bind_ip, port))

    # Join multicast group
    group_bytes = socket.inet_aton(multicast_group)
    if interface_ip:
        iface_bytes = socket.inet_aton(interface_ip)
        mreq = group_bytes + iface_bytes
    else:
        mreq = group_bytes + socket.inet_aton("0.0.0.0")

    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    # Set TTL
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

    # Loopback (receive own packets)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1 if loop else 0)

    # Set outgoing interface for multicast
    if interface_ip:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(interface_ip))

    sock.setblocking(False)
    return sock


def leave_multicast_group(
    sock: socket.socket,
    multicast_group: str = "239.255.0.1",
    interface_ip: Optional[str] = None,
) -> None:
    """Leave multicast group."""
    group_bytes = socket.inet_aton(multicast_group)
    if interface_ip:
        iface_bytes = socket.inet_aton(interface_ip)
        mreq = group_bytes + iface_bytes
    else:
        mreq = group_bytes + socket.inet_aton("0.0.0.0")
    try:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
    except OSError:
        pass


class NetworkManager:
    """High-level network manager for P2P chat."""

    def __init__(
        self,
        port: int = 50000,
        multicast_group: str = "239.255.0.1",
        interface_name: Optional[str] = None,
        interface_ip: Optional[str] = None,
    ):
        self.port = port
        self.multicast_group = multicast_group
        self.interface_name = interface_name
        self.interface_ip = interface_ip

        self.interface: Optional[InterfaceInfo] = None
        self.broadcast_sock: Optional[socket.socket] = None
        self.multicast_sock: Optional[socket.socket] = None

    def initialize(self) -> bool:
        """Detect interface and create sockets."""
        interfaces = get_interfaces()
        if not interfaces:
            print("No network interfaces found")
            return False

        self.interface = select_interface(interfaces, self.interface_name, self.interface_ip)
        if not self.interface:
            print("No suitable interface found")
            return False

        print(f"Using interface: {self.interface.name} ({self.interface.ip}/{self.interface.netmask})")
        print(f"Broadcast address: {self.interface.broadcast}")
        print(f"Multicast group: {self.multicast_group}")

        self.broadcast_sock = create_broadcast_socket(self.port, "0.0.0.0")
        self.multicast_sock = create_multicast_socket(
            self.port,
            self.multicast_group,
            "0.0.0.0",
            interface_ip=self.interface.ip,
        )
        return True

    def get_sockets(self) -> Tuple[Optional[socket.socket], Optional[socket.socket]]:
        return self.broadcast_sock, self.multicast_sock

    def get_broadcast_addr(self) -> Tuple[str, int]:
        return (self.interface.broadcast, self.port) if self.interface else ("255.255.255.255", self.port)

    def get_multicast_addr(self) -> Tuple[str, int]:
        return (self.multicast_group, self.port)

    def cleanup(self) -> None:
        if self.multicast_sock:
            leave_multicast_group(self.multicast_sock, self.multicast_group, self.interface.ip if self.interface else None)
            self.multicast_sock.close()
        if self.broadcast_sock:
            self.broadcast_sock.close()