"""Кроссплатформенные утилиты для работы с сокетами."""

import socket
import select
import sys
from typing import Optional, Tuple


def create_server_socket(host: str, port: int) -> socket.socket:
    """Создаёт и настраивает серверный сокет."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    configure_keepalive(sock)
    sock.bind((host, port))
    sock.listen(1)
    return sock


def create_client_socket() -> socket.socket:
    """Создаёт и настраивает клиентский сокет."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    configure_keepalive(sock)
    return sock


def configure_keepalive(sock: socket.socket) -> None:
    """Настраивает SO_KEEPALIVE для обнаружения разрыва соединения."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    
    # Платформозависимые настройки keepalive
    if sys.platform == 'linux':
        # Время до первого keepalive пакета (секунды)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
        # Интервал между keepalive пакетами (секунды)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
        # Количество попыток до признания соединения разорванным
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
    elif sys.platform == 'darwin':  # macOS
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, 30)
    # Windows настраивается через реестр или SIO_KEEPALIVE_VALS


def recv_until(sock: socket.socket, terminator: bytes, 
               timeout: float = None) -> Optional[bytes]:
    """
    Читает данные из сокета до получения терминатора.
    Решает проблему границ сообщений в TCP.
    """
    buffer = b''
    sock.settimeout(timeout)
    
    try:
        while terminator not in buffer:
            chunk = sock.recv(1024)
            if not chunk:
                return None  # Соединение закрыто
            buffer += chunk
    except socket.timeout:
        return None
    
    return buffer


def recv_exact(sock: socket.socket, size: int, 
               timeout: float = None) -> Optional[bytes]:
    """Читает ровно size байт из сокета."""
    buffer = b''
    sock.settimeout(timeout)
    
    try:
        while len(buffer) < size:
            remaining = size - len(buffer)
            chunk = sock.recv(min(remaining, 8192))
            if not chunk:
                return None
            buffer += chunk
    except socket.timeout:
        return None
    
    return buffer


def send_all(sock: socket.socket, data: bytes) -> bool:
    """Отправляет все данные, гарантируя полную отправку."""
    try:
        sock.sendall(data)
        return True
    except (socket.error, BrokenPipeError):
        return False


def is_socket_ready(sock: socket.socket, timeout: float = 0) -> bool:
    """Проверяет, есть ли данные для чтения в сокете."""
    ready, _, _ = select.select([sock], [], [], timeout)
    return len(ready) > 0


def get_peer_info(sock: socket.socket) -> str:
    """Возвращает информацию о подключённом клиенте."""
    try:
        addr = sock.getpeername()
        return f"{addr[0]}:{addr[1]}"
    except socket.error:
        return "unknown"