"""Последовательный TCP сервер."""

import socket
import signal
import sys
from typing import Optional

from common.protocol import (
    Command, CommandType, parse_command, 
    format_response, COMMAND_TERMINATOR, Response
)
from common.socket_utils import (
    create_server_socket, recv_until, 
    send_all, get_peer_info
)
from server.command_handler import CommandHandler
from server.file_manager import FileManager


class TCPServer:
    """Последовательный TCP сервер для обработки команд и файлов."""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 9000):
        self.host = host
        self.port = port
        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.current_client: Optional[socket.socket] = None
        
        self.file_manager = FileManager()
        self.command_handler = CommandHandler(self.file_manager)
    
    def start(self) -> None:
        """Запускает сервер."""
        self._setup_signal_handlers()
        self.server_socket = create_server_socket(self.host, self.port)
        self.running = True
        
        print(f"Server started on {self.host}:{self.port}")
        print("Waiting for connections...")
        
        self._main_loop()
    
    def _setup_signal_handlers(self) -> None:
        """Настраивает обработчики сигналов для корректного завершения."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame) -> None:
        """Обрабатывает сигналы завершения."""
        print("\nShutting down server...")
        self.stop()
        sys.exit(0)
    
    def _main_loop(self) -> None:
        """Главный цикл обработки подключений."""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                try:
                    client_sock, client_addr = self.server_socket.accept()
                except socket.timeout:
                    continue
                
                self._handle_client(client_sock, client_addr)
                
            except socket.error as e:
                if self.running:
                    print(f"Socket error: {e}")
    
    def _handle_client(self, client_sock: socket.socket, 
                       client_addr: tuple) -> None:
        """Обрабатывает подключение клиента."""
        self.current_client = client_sock
        addr_str = f"{client_addr[0]}:{client_addr[1]}"
        print(f"Client connected: {addr_str}")
        
        try:
            self._process_commands(client_sock, addr_str)
        except Exception as e:
            print(f"Error handling client {addr_str}: {e}")
        finally:
            self._close_client(client_sock, addr_str)
    
    def _process_commands(self, client_sock: socket.socket, 
                          client_addr: str) -> None:
        """Обрабатывает команды от клиента."""
        send_all(client_sock, b"220 Welcome to File Server\n")
        
        while self.running:
            raw_data = recv_until(client_sock, COMMAND_TERMINATOR, timeout=300)
            if raw_data is None:
                print(f"Connection lost with {client_addr}")
                break
            
            command_str = raw_data.decode('utf-8', errors='ignore')
            command = parse_command(command_str)
            
            print(f"[{client_addr}] Command: {command.type.name}")
            
            if command.type == CommandType.QUIT:
                send_all(client_sock, b"221 Goodbye\n")
                break
            
            response = self.command_handler.execute(
                command, client_sock, client_addr
            )
            send_all(client_sock, format_response(response))
    
    def _close_client(self, client_sock: socket.socket, 
                      client_addr: str) -> None:
        """Закрывает соединение с клиентом."""
        try:
            client_sock.close()
        except socket.error:
            pass
        print(f"Client disconnected: {client_addr}")
        self.current_client = None
    
    def stop(self) -> None:
        """Останавливает сервер."""
        self.running = False
        if self.current_client:
            try:
                self.current_client.close()
            except socket.error:
                pass
        if self.server_socket:
            try:
                self.server_socket.close()
            except socket.error:
                pass