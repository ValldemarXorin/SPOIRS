"""Последовательный TCP/UDP сервер."""

import socket
import select
import signal
import sys
from typing import Optional
from common.protocol import (
    Command, CommandType, parse_command,
    format_response, COMMAND_TERMINATOR, Response
)
from common.socket_utils import (
    create_server_socket, recv_until,
    send_all
)
from common.rudp import RUDPSocket
from server.command_handler import CommandHandler
from server.file_manager import FileManager


class TCPServer:
    """Сервер для обработки команд и файлов (TCP + UDP)."""

    def __init__(self, host: str = '0.0.0.0', port: int = 9000):
        self.host = host
        self.port = port
        self.running = False
        self.server_socket_tcp: Optional[socket.socket] = None
        self.server_socket_udp: Optional[socket.socket] = None

        self.file_manager = FileManager()
        self.command_handler = CommandHandler(self.file_manager)

        self.inputs = []

    def start(self) -> None:
        """Запускает сервер."""
        self._setup_signal_handlers()

        # Init TCP
        self.server_socket_tcp = create_server_socket(self.host, self.port)
        self.server_socket_tcp.setblocking(False)
        self.inputs.append(self.server_socket_tcp)

        # Init UDP
        self.server_socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket_udp.bind((self.host, self.port))
        self.server_socket_udp.setblocking(False)
        self.inputs.append(self.server_socket_udp)

        # !!! ВАЖНОЕ ИСПРАВЛЕНИЕ: Включаем флаг работы сервера !!!
        self.running = True

        print(f"Server started on {self.host}:{self.port} (TCP & UDP)")
        print("Waiting for connections...")

        self._main_loop()

    def _setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame) -> None:
        print("\nShutting down server...")
        self.stop()
        sys.exit(0)

    def _main_loop(self) -> None:
        while self.running:
            try:
                # Select с таймаутом, чтобы можно было прервать цикл
                readable, _, exceptional = select.select(self.inputs, [], self.inputs, 1.0)

                for s in readable:
                    if s is self.server_socket_tcp:
                        self._handle_tcp_accept()
                    elif s is self.server_socket_udp:
                        self._handle_udp_packet()
                    else:
                        self._handle_tcp_data(s)

                for s in exceptional:
                    self._remove_client(s)

            except socket.error as e:
                # Игнорируем ошибки прерывания вызова, если сервер еще работает
                if self.running:
                    print(f"Socket error in main loop: {e}")

    def _handle_tcp_accept(self) -> None:
        try:
            client_sock, client_addr = self.server_socket_tcp.accept()
            print(f"TCP Client connected: {client_addr}")
            client_sock.setblocking(False)
            self.inputs.append(client_sock)
            send_all(client_sock, b"220 Welcome to File Server\n")
        except socket.error:
            pass

    def _handle_tcp_data(self, sock: socket.socket) -> None:
        try:
            raw_data = recv_until(sock, COMMAND_TERMINATOR, timeout=0.1)

            if raw_data is None:
                self._remove_client(sock)
                return

            command_str = raw_data.decode('utf-8', errors='ignore')
            command = parse_command(command_str, default_proto='TCP')

            self._process_command(command, sock, None)

        except Exception as e:
            print(f"Error handling TCP client: {e}")
            self._remove_client(sock)

    def _handle_udp_packet(self) -> None:
        try:
            rudp = RUDPSocket(self.server_socket_udp)
            cmd_text, addr = rudp.recv_command()

            if cmd_text:
                if cmd_text == "ACK_CMD":
                    return

                print(f"UDP Command from {addr}: {cmd_text}")
                command = parse_command(cmd_text, default_proto='UDP')
                self._process_command(command, None, addr)

        except socket.error:
            pass

    def _process_command(self, command: Command,
                         tcp_sock: Optional[socket.socket],
                         udp_addr: Optional[tuple]) -> None:

        client_id = f"{udp_addr[0]}:{udp_addr[1]}" if udp_addr else get_peer_info(tcp_sock)
        print(f"[{client_id}] Command: {command.type.name} via {command.protocol}")

        if command.type == CommandType.QUIT:
            if tcp_sock:
                send_all(tcp_sock, b"221 Goodbye\n")
                self._remove_client(tcp_sock)
            return

        # Запуск обработчика
        response = self.command_handler.execute(
            command, tcp_sock, self.server_socket_udp, udp_addr
        )

        # Отправка ответа
        if command.protocol == 'TCP' and tcp_sock:
            send_all(tcp_sock, format_response(response))
        elif command.protocol == 'UDP' and udp_addr:
            # Для команд типа ECHO, TIME или ERROR отправляем ответ как пакет CMD
            # (Для UPLOAD/DOWNLOAD ответы уже ушли в процессе передачи)
            if command.type not in (CommandType.UPLOAD, CommandType.DOWNLOAD, CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD):
                rudp = RUDPSocket(self.server_socket_udp, udp_addr)
                pkt = rudp._pack_packet(0, 3, format_response(response))  # 3=PacketType.CMD
                self.server_socket_udp.sendto(pkt, udp_addr)

    def _remove_client(self, sock: socket.socket) -> None:
        if sock in self.inputs:
            self.inputs.remove(sock)
        try:
            sock.close()
        except:
            pass

    def stop(self) -> None:
        self.running = False
        for s in self.inputs:
            try:
                s.close()
            except:
                pass


def get_peer_info(sock: socket.socket) -> str:
    try:
        addr = sock.getpeername()
        return f"{addr[0]}:{addr[1]}"
    except:
        return "unknown"