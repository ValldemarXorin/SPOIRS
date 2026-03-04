"""Утилиты для работы с сокетами — кроссплатформенные (Windows + Linux)."""

import socket
import select
import sys
from typing import Optional


def create_server_socket(host: str, port: int) -> socket.socket:
    """Создаёт серверный TCP сокет."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    _configure_keepalive(sock)
    sock.bind((host, port))
    sock.listen(5)
    return sock


def create_client_socket() -> socket.socket:
    """Создаёт клиентский TCP сокет."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _configure_keepalive(sock)
    return sock


def _configure_keepalive(sock: socket.socket) -> None:
    """Включает SO_KEEPALIVE — кроссплатформенно."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

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
    # Windows: keepalive defaults are fine, or use SIO_KEEPALIVE_VALS via ioctl


def create_udp_socket(buf_size: int = 16 * 1024 * 1024) -> socket.socket:
    """Создаёт UDP сокет с увеличенными буферами."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
        try:
            sock.setsockopt(socket.SOL_SOCKET, opt, buf_size)
        except OSError:
            pass
    return sock


def recv_until(sock: socket.socket, terminator: bytes,
               timeout: float = None) -> Optional[bytes]:
    """Принимает данные до встречи terminator по TCP."""
    buffer = b""
    sock.settimeout(timeout)
    try:
        while terminator not in buffer:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            buffer += chunk
    except socket.timeout:
        return None
    except OSError:
        return None
    return buffer


def recv_exact(sock: socket.socket, size: int,
               timeout: float = None) -> Optional[bytes]:
    """Принимает ровно size байт."""
    buffer = b""
    sock.settimeout(timeout)
    try:
        while len(buffer) < size:
            remaining = size - len(buffer)
            chunk = sock.recv(min(remaining, 65536))
            if not chunk:
                return None
            buffer += chunk
    except socket.timeout:
        return None
    except OSError:
        return None
    return buffer


def send_all(sock: socket.socket, data: bytes) -> bool:
    """Отправляет все данные, гарантируя полную отправку."""
    try:
        sock.sendall(data)
        return True
    except (socket.error, BrokenPipeError, OSError):
        return False


def is_socket_ready(sock: socket.socket, timeout: float = 0) -> bool:
    """Проверяет, готов ли сокет для чтения."""
    ready, _, _ = select.select([sock], [], [], timeout)
    return len(ready) > 0


def get_peer_info(sock: socket.socket) -> str:
    """Возвращает строковое представление адреса пира."""
    try:
        addr = sock.getpeername()
        return f"{addr[0]}:{addr[1]}"
    except socket.error:
        return "unknown"