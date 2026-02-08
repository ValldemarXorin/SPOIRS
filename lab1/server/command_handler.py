"""Обработчики команд сервера."""

import time
from datetime import datetime
from typing import Callable, Dict, Optional, TYPE_CHECKING
from common.protocol import (
    Command, CommandType, Response,
    BUFFER_SIZE, format_response
)
from common.socket_utils import recv_exact, send_all
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

        return self._receive_file(command.protocol, tcp_sock, udp_sock, udp_addr,
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

        return self._receive_file(command.protocol, tcp_sock, udp_sock, udp_addr,
                                  filename, remaining_size, offset)

    def _receive_file(self, protocol: str, tcp_sock, udp_sock, udp_addr,
                      filename: str, size: int, offset: int) -> Response:

        client_id = f"{udp_addr[0]}:{udp_addr[1]}" if protocol == 'UDP' else "tcp_client"
        session = self.file_manager.create_session(
            filename, size, client_id, is_upload=True
        )
        session.transferred = offset

        # 1. Send READY
        if protocol == 'UDP':
            rudp = RUDPSocket(udp_sock, udp_addr)
            # Ответ на команду отправляется как CMD пакет
            pkt = rudp._pack_packet(0, 3, b"OK READY\n")  # 3=CMD
            udp_sock.sendto(pkt, udp_addr)
        else:
            send_all(tcp_sock, b"READY\n")

        # 2. Receive Data
        mode = 'ab' if offset > 0 else 'wb'
        received = 0
        try:
            with open(session.temp_path, mode) as f:
                if protocol == 'UDP':
                    # Для UDP сервер входит в режим приема файла
                    # Внимание: это блокирует основной поток на время передачи.
                    # Для полноценного сервера нужно выделять поток или non-blocking state machine.
                    # В рамках ЛР допустима блокировка на время трансфера.
                    rudp = RUDPSocket(udp_sock, udp_addr)
                    received = rudp.recv_stream(f)
                else:
                    received = self._receive_data_tcp(tcp_sock, f, size, session)
        except IOError as e:
            self.file_manager.remove_session(filename, client_id)
            return Response(False, f"File write error: {e}")

        if received == size:
            self.file_manager.complete_session(filename, client_id)
            bitrate = self.file_manager.calculate_bitrate(session)
            formatted = self.file_manager.format_bitrate(bitrate)
            return Response(True, f"Received {received} bytes. Bitrate: {formatted}")
        else:
            return Response(False, f"Transfer incomplete: {received}/{size}")

    def _receive_data_tcp(self, sock, file, total_size: int, session) -> int:
        received = 0
        while received < total_size:
            chunk_size = min(BUFFER_SIZE, total_size - received)
            data = recv_exact(sock, chunk_size, timeout=60)
            if data is None:
                break
            file.write(data)
            received += len(data)
            session.transferred += len(data)
        return received

    def handle_download(self, command: Command, tcp_sock, udp_sock, udp_addr) -> Response:
        if len(command.args) < 1:
            return Response(False, "Usage: DOWNLOAD <filename>")
        filename = command.args[0]
        return self._send_file(command.protocol, tcp_sock, udp_sock, udp_addr, filename, offset=0)

    def handle_resume_download(self, command: Command, tcp_sock, udp_sock, udp_addr) -> Response:
        if len(command.args) < 2:
            return Response(False, "Usage: RESUME_DOWNLOAD <filename> <offset>")
        filename = command.args[0]
        try:
            offset = int(command.args[1])
        except ValueError:
            return Response(False, "Invalid offset")
        return self._send_file(command.protocol, tcp_sock, udp_sock, udp_addr, filename, offset)

    def _send_file(self, protocol: str, tcp_sock, udp_sock, udp_addr,
                   filename: str, offset: int) -> Response:

        if not self.file_manager.file_exists(filename):
            return Response(False, "File not found")

        file_path = self.file_manager.get_file_path(filename)
        file_size = self.file_manager.get_file_size(filename)
        remaining = file_size - offset

        client_id = f"{udp_addr[0]}:{udp_addr[1]}" if protocol == 'UDP' else "tcp_client"
        session = self.file_manager.create_session(
            filename, remaining, client_id, is_upload=False
        )

        # 1. Send Header Info
        info_msg = f"FILE {remaining}"
        if protocol == 'UDP':
            rudp = RUDPSocket(udp_sock, udp_addr)
            # Шлем подтверждение команды + инфо
            pkt = rudp._pack_packet(0, 3, f"OK {info_msg}\n".encode())
            udp_sock.sendto(pkt, udp_addr)
            # Небольшая пауза, чтобы клиент успел переключиться в режим приема
            time.sleep(0.1)
        else:
            send_all(tcp_sock, f"{info_msg}\n".encode())

        # 2. Send Data
        sent = 0
        try:
            with open(file_path, 'rb') as f:
                f.seek(offset)
                if protocol == 'UDP':
                    rudp = RUDPSocket(udp_sock, udp_addr)
                    rudp.send_stream(f, remaining)
                    sent = remaining  # Assuming success
                else:
                    sent = self._send_data_tcp(tcp_sock, f, remaining, session)
        except IOError as e:
            return Response(False, f"File read error: {e}")

        bitrate = self.file_manager.calculate_bitrate(session)
        formatted = self.file_manager.format_bitrate(bitrate)
        self.file_manager.remove_session(filename, client_id)

        return Response(True, f"Sent {sent} bytes. Bitrate: {formatted}")

    def _send_data_tcp(self, sock, file, total_size: int, session) -> int:
        sent = 0
        while sent < total_size:
            chunk_size = min(BUFFER_SIZE, total_size - sent)
            data = file.read(chunk_size)
            if not data:
                break
            if not send_all(sock, data):
                break
            sent += len(data)
            session.transferred += len(data)
        return sent