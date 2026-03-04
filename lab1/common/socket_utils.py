"""Утилиты для работы с сокетами — Windows + Linux."""

import socket
import select
import sys
import time
from typing import Optional


def create_server_socket(host: str, port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Для Windows также устанавливаем SO_EXCLUSIVEADDRUSE
    if sys.platform == "win32":
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        except:
            pass
    _configure_keepalive(sock)
    sock.bind((host, port))
    sock.listen(5)
    return sock


def create_client_socket() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _configure_keepalive(sock)
    return sock


def _configure_keepalive(sock: socket.socket) -> None:
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    except:
        pass

    if sys.platform == "linux":
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        except (AttributeError, OSError):
            pass
    elif sys.platform == "darwin":
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, 30)
        except (AttributeError, OSError):
            pass
    # Windows имеет свои настройки keepalive через SIO_KEEPALIVE_VALS
    elif sys.platform == "win32":
        try:
            # Включаем keepalive
            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 30000, 10000))
        except:
            pass


def create_udp_socket(buf_size: int = 8 * 1024 * 1024) -> socket.socket:
    """UDP сокет с увеличенными буферами."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Увеличиваем буферы поэтапно
    for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
        target = buf_size
        while target >= 256 * 1024:
            try:
                sock.setsockopt(socket.SOL_SOCKET, opt, target)
                break
            except OSError:
                target //= 2

    # Для Windows устанавливаем неблокирующий режим
    if sys.platform == "win32":
        sock.setblocking(False)

    return sock


def recv_until(sock: socket.socket, terminator: bytes,
               timeout: float = None) -> Optional[bytes]:
    buffer = b""
    original_timeout = sock.gettimeout()
    try:
        sock.settimeout(timeout)
        while terminator not in buffer:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            buffer += chunk
    except (socket.timeout, OSError):
        return None
    finally:
        sock.settimeout(original_timeout)
    return buffer


def recv_exact(sock: socket.socket, size: int,
               timeout: float = None) -> Optional[bytes]:
    buffer = b""
    original_timeout = sock.gettimeout()
    try:
        sock.settimeout(timeout)
        while len(buffer) < size:
            chunk = sock.recv(min(size - len(buffer), 65536))
            if not chunk:
                return None
            buffer += chunk
    except (socket.timeout, OSError):
        return None
    finally:
        sock.settimeout(original_timeout)
    return buffer


def send_all(sock: socket.socket, data: bytes) -> bool:
    try:
        sock.sendall(data)
        return True
    except (socket.error, BrokenPipeError, OSError):
        return False


def is_socket_ready(sock: socket.socket, timeout: float = 0) -> bool:
    ready, _, _ = select.select([sock], [], [], timeout)
    return len(ready) > 0


def get_peer_info(sock: socket.socket) -> str:
    try:
        addr = sock.getpeername()
        return f"{addr[0]}:{addr[1]}"
    except socket.error:
        return "unknown"