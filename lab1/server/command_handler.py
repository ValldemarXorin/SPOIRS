"""Обработчики команд сервера (Non-blocking)."""

import time
from datetime import datetime
from typing import Optional, TYPE_CHECKING
from common.protocol import (
    Command, CommandType, Response,
    format_response
)
from common.socket_utils import send_all
from common.rudp import RUDPSocket
from server.file_manager import FileManager

if TYPE_CHECKING:
    import socket


class CommandHandler:
    """Обработчик команд сервера."""

    def __init__(self, file_manager: FileManager):
        self.file_manager = file_manager
        self.handlers = {
            CommandType.ECHO: self.handle_echo,
            CommandType.TIME: self.handle_time,
            CommandType.UPLOAD: self.handle_upload,
            CommandType.DOWNLOAD: self.handle_download,
            CommandType.RESUME_UPLOAD: self.handle_resume_upload,
            CommandType.RESUME_DOWNLOAD: self.handle_resume_download,
        }

    def execute(self, command: Command,
                tcp_sock: Optional['socket.socket'],
                udp_sock: Optional['socket.socket'],
                udp_addr: Optional[tuple]) -> Response:

        handler = self.handlers.get(command.type)
        if handler:
            return handler(command, tcp_sock, udp_sock, udp_addr)
        return Response(False, f"Unknown command: {command.raw.strip()}")

    def handle_echo(self, command: Command, *args) -> Response:
        text = command.args[0] if command.args else ""
        return Response(True, text)

    def handle_time(self, command: Command, *args) -> Response:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return Response(True, current_time)

    def handle_upload(self, command: Command, tcp_sock, udp_sock, udp_addr) -> Response:
        if len(command.args) < 2:
            return Response(False, "Usage: UPLOAD <filename> <size>")

        filename = command.args[0]
        try:
            file_size = int(command.args[1])
        except ValueError:
            return Response(False, "Invalid file size")

        return self._init_upload(command.protocol, tcp_sock, udp_sock, udp_addr,
                                 filename, file_size, offset=0)

    def handle_resume_upload(self, command: Command, tcp_sock, udp_sock, udp_addr) -> Response:
        if len(command.args) < 3:
            return Response(False, "Usage: RESUME_UPLOAD <filename> <offset> <size>")

        filename = command.args[0]
        try:
            offset = int(command.args[1])
            remaining_size = int(command.args[2])
        except ValueError:
            return Response(False, "Invalid offset or size")

        return self._init_upload(command.protocol, tcp_sock, udp_sock, udp_addr,
                                 filename, remaining_size, offset)

    def _init_upload(self, protocol: str, tcp_sock, udp_sock, udp_addr,
                     filename: str, size: int, offset: int) -> Response:
        """Инициализирует сессию загрузки, но НЕ блокирует поток."""

        client_id = f"{udp_addr[0]}:{udp_addr[1]}" if protocol == 'UDP' else str(tcp_sock.fileno())

        # Если сессия уже есть, закрываем старую
        self.file_manager.close_session(client_id)

        session = self.file_manager.create_session(
            filename, size, client_id, is_upload=True, sock=tcp_sock
        )

        if not session:
            return Response(False, "Failed to create session/open file")

        session.transferred = offset
        if offset > 0 and session.file_handle:
            session.file_handle.seek(offset)

        # Отправляем подтверждение готовности
        if protocol == 'UDP':
            rudp = RUDPSocket(udp_sock, udp_addr)
            pkt = rudp._pack_packet(0, 3, b"OK READY\n")  # 3=CMD
            # Отправляем несколько раз для надёжности
            for _ in range(3):
                try:
                    udp_sock.sendto(pkt, udp_addr)
                except Exception:
                    pass
                time.sleep(0.005)
        else:
            send_all(tcp_sock, b"READY\n")

        return Response(True, "READY (Upload Started)")

    def handle_download(self, command: Command, tcp_sock, udp_sock, udp_addr) -> Response:
        if len(command.args) < 1:
            return Response(False, "Usage: DOWNLOAD <filename>")
        filename = command.args[0]
        return self._init_download(command.protocol, tcp_sock, udp_sock, udp_addr, filename, offset=0)

    def handle_resume_download(self, command: Command, tcp_sock, udp_sock, udp_addr) -> Response:
        if len(command.args) < 2:
            return Response(False, "Usage: RESUME_DOWNLOAD <filename> <offset>")
        filename = command.args[0]
        try:
            offset = int(command.args[1])
        except ValueError:
            return Response(False, "Invalid offset")
        return self._init_download(command.protocol, tcp_sock, udp_sock, udp_addr, filename, offset)

    def _init_download(self, protocol: str, tcp_sock, udp_sock, udp_addr,
                       filename: str, offset: int) -> Response:
        """Инициализирует сессию скачивания."""

        if not self.file_manager.file_exists(filename):
            return Response(False, "File not found")

        file_path = self.file_manager.get_file_path(filename)
        file_size = self.file_manager.get_file_size(filename)
        remaining = file_size - offset

        client_id = f"{udp_addr[0]}:{udp_addr[1]}" if protocol == 'UDP' else str(tcp_sock.fileno())

        self.file_manager.close_session(client_id)

        session = self.file_manager.create_session(
            filename, remaining, client_id, is_upload=False, sock=tcp_sock
        )

        if not session:
            return Response(False, "Failed to open file")

        if offset > 0 and session.file_handle:
            session.file_handle.seek(offset)

        info_msg = f"FILE {remaining}"

        if protocol == 'UDP':
            rudp = RUDPSocket(udp_sock, udp_addr)
            pkt = rudp._pack_packet(0, 3, f"OK {info_msg}\n".encode())
            # Отправляем несколько раз для надёжности
            for _ in range(3):
                try:
                    udp_sock.sendto(pkt, udp_addr)
                except Exception:
                    pass
                time.sleep(0.005)
        else:
            send_all(tcp_sock, f"{info_msg}\n".encode())

        return Response(True, f"Started download {info_msg}")
