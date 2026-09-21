"""Network utilities (logic from lr6.py): local IP/mask/broadcast and sockets."""

import socket
import struct
from typing import Tuple

DEFAULT_PORT = 50050
MULTICAST_GROUP = "239.255.0.1"


def get_local_interfaces() -> Tuple[str, str, str]:
    """Определяет локальный IP-адрес, маску и broadcast подсети."""
    temp_s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        temp_s.connect(("8.8.8.8", 80))
        local_ip = temp_s.getsockname()[0]
    except Exception:
        local_ip = "127.0.0.1"
    finally:
        temp_s.close()

    # Для мобильных точек доступа 172.20.10.x используется маска /28 (255.255.255.240)
    if local_ip.startswith("172.20.10."):
        netmask = "255.255.255.240"
        broadcast_ip = "172.20.10.15"
    elif local_ip != "127.0.0.1":
        parts = local_ip.split(".")
        netmask = "255.255.255.0"
        broadcast_ip = f"{parts[0]}.{parts[1]}.{parts[2]}.255"
    else:
        netmask = "255.0.0.0"
        broadcast_ip = "255.255.255.255"

    return local_ip, netmask, broadcast_ip


def create_recv_socket(port: int) -> socket.socket:
    """Сокет приема: один, принимает и broadcast, и multicast."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Linux/macOS поддерживают SO_REUSEPORT
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except socket.error:
            pass

    sock.bind(("", port))
    return sock


def create_send_socket() -> socket.socket:
    """Сокет передачи: broadcast + multicast TTL."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    return sock


def join_multicast(sock: socket.socket, group: str) -> bool:
    """Подключение сокета к группе Multicast."""
    try:
        mreq = struct.pack("4s4s", socket.inet_aton(group), socket.inet_aton("0.0.0.0"))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        return True
    except OSError:
        return False


def leave_multicast(sock: socket.socket, group: str) -> bool:
    """Выход сокета из группы Multicast."""
    try:
        mreq = struct.pack("4s4s", socket.inet_aton(group), socket.inet_aton("0.0.0.0"))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
        return True
    except OSError:
        return False