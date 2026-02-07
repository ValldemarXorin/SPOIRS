"""Обработчики команд сервера."""

import time
from datetime import datetime
from typing import Callable, Dict, TYPE_CHECKING

from common.protocol import (
    Command, CommandType, Response, 
    BUFFER_SIZE, format_response
)
from common.socket_utils import recv_exact, send_all
from server.file_manager import FileManager

if TYPE_CHECKING:
    import socket


class CommandHandler:
    """Обработчик команд сервера."""
    
    def __init__(self, file_manager: FileManager):
        self.file_manager = file_manager
        self.handlers: Dict[CommandType, Callable] = {
            CommandType.ECHO: self.handle_echo,
            CommandType.TIME: self.handle_time,
            CommandType.UPLOAD: self.handle_upload,
            CommandType.DOWNLOAD: self.handle_download,
            CommandType.RESUME_UPLOAD: self.handle_resume_upload,
            CommandType.RESUME_DOWNLOAD: self.handle_resume_download,
        }
    
    def execute(self, command: Command, client_sock: 'socket.socket', 
                client_addr: str) -> Response:
        """Выполняет команду и возвращает ответ."""
        handler = self.handlers.get(command.type)
        if handler:
            return handler(command, client_sock, client_addr)
        return Response(False, f"Unknown command: {command.raw.strip()}")
    
    def handle_echo(self, command: Command, 
                    client_sock: 'socket.socket', 
                    client_addr: str) -> Response:
        """Обрабатывает команду ECHO."""
        text = command.args[0] if command.args else ""
        return Response(True, text)
    
    def handle_time(self, command: Command, 
                    client_sock: 'socket.socket', 
                    client_addr: str) -> Response:
        """Обрабатывает команду TIME."""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return Response(True, current_time)
    
    def handle_upload(self, command: Command, 
                      client_sock: 'socket.socket', 
                      client_addr: str) -> Response:
        """Обрабатывает загрузку файла на сервер."""
        if len(command.args) < 2:
            return Response(False, "Usage: UPLOAD <filename> <size>")
        
        filename = command.args[0]
        try:
            file_size = int(command.args[1])
        except ValueError:
            return Response(False, "Invalid file size")
        
        return self._receive_file(client_sock, client_addr, 
                                  filename, file_size, offset=0)
    
    def handle_resume_upload(self, command: Command, 
                             client_sock: 'socket.socket', 
                             client_addr: str) -> Response:
        """Обрабатывает докачку файла на сервер."""
        if len(command.args) < 3:
            return Response(False, "Usage: RESUME_UPLOAD <filename> <offset> <size>")
        
        filename = command.args[0]
        try:
            offset = int(command.args[1])
            remaining_size = int(command.args[2])
        except ValueError:
            return Response(False, "Invalid offset or size")
        
        return self._receive_file(client_sock, client_addr, 
                                  filename, remaining_size, offset)
    
    def _receive_file(self, client_sock: 'socket.socket', 
                      client_addr: str, filename: str, 
                      size: int, offset: int) -> Response:
        """Принимает файл от клиента."""
        session = self.file_manager.create_session(
            filename, size, client_addr, is_upload=True
        )
        session.transferred = offset
        
        # Отправляем подтверждение готовности
        send_all(client_sock, b"READY\n")
        
        mode = 'ab' if offset > 0 else 'wb'
        try:
            with open(session.temp_path, mode) as f:
                received = self._receive_data(client_sock, f, size, session)
        except IOError as e:
            self.file_manager.remove_session(filename, client_addr)
            return Response(False, f"File write error: {e}")
        
        if received == size:
            self.file_manager.complete_session(filename, client_addr)
            bitrate = self.file_manager.calculate_bitrate(session)
            formatted = self.file_manager.format_bitrate(bitrate)
            return Response(True, f"Received {received} bytes. Bitrate: {formatted}")
        else:
            return Response(False, f"Transfer incomplete: {received}/{size}")
    
    def _receive_data(self, sock: 'socket.socket', file, 
                      total_size: int, session) -> int:
        """Читает данные из сокета и записывает в файл."""
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
    
    def handle_download(self, command: Command, 
                        client_sock: 'socket.socket', 
                        client_addr: str) -> Response:
        """Обрабатывает скачивание файла с сервера."""
        if len(command.args) < 1:
            return Response(False, "Usage: DOWNLOAD <filename>")
        
        filename = command.args[0]
        return self._send_file(client_sock, client_addr, filename, offset=0)
    
    def handle_resume_download(self, command: Command, 
                               client_sock: 'socket.socket', 
                               client_addr: str) -> Response:
        """Обрабатывает докачку файла с сервера."""
        if len(command.args) < 2:
            return Response(False, "Usage: RESUME_DOWNLOAD <filename> <offset>")
        
        filename = command.args[0]
        try:
            offset = int(command.args[1])
        except ValueError:
            return Response(False, "Invalid offset")
        
        return self._send_file(client_sock, client_addr, filename, offset)
    
    def _send_file(self, client_sock: 'socket.socket', 
                   client_addr: str, filename: str, 
                   offset: int) -> Response:
        """Отправляет файл клиенту."""
        if not self.file_manager.file_exists(filename):
            send_all(client_sock, b"ERROR File not found\n")
            return Response(False, "File not found")
        
        file_path = self.file_manager.get_file_path(filename)
        file_size = self.file_manager.get_file_size(filename)
        remaining = file_size - offset
        
        # Отправляем информацию о файле
        send_all(client_sock, f"FILE {remaining}\n".encode())
        
        session = self.file_manager.create_session(
            filename, remaining, client_addr, is_upload=False
        )
        
        try:
            with open(file_path, 'rb') as f:
                f.seek(offset)
                sent = self._send_data(client_sock, f, remaining, session)
        except IOError as e:
            return Response(False, f"File read error: {e}")
        
        bitrate = self.file_manager.calculate_bitrate(session)
        formatted = self.file_manager.format_bitrate(bitrate)
        self.file_manager.remove_session(filename, client_addr)
        
        return Response(True, f"Sent {sent} bytes. Bitrate: {formatted}")
    
    def _send_data(self, sock: 'socket.socket', file, 
                   total_size: int, session) -> int:
        """Читает данные из файла и отправляет в сокет."""
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