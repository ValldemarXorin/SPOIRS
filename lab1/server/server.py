"""Мультиплексированный TCP/UDP сервер (Single Thread)."""

import socket
import select
import signal
import sys
import time
from typing import Optional, List, Dict

from common.protocol import (
    Command, CommandType, parse_command,
    format_response, COMMAND_TERMINATOR, Response,
    PacketType, UDP_PAYLOAD_SIZE, UDP_HEADER_SIZE
)
from common.socket_utils import (
    create_server_socket, recv_until,
    send_all, get_peer_info
)
from common.rudp import RUDPSocket
from server.command_handler import CommandHandler
from server.file_manager import FileManager

# Размер порции данных за одну итерацию цикла
# Для TCP: 32KB достаточно мелко, чтобы интерфейс не фризился
# Для UDP это ~1-2 пакета burst
TCP_CHUNK_SIZE = 32 * 1024
UDP_BURST_SIZE = 10 # Пакетов за раз

class TCPServer:
    """Сервер с I/O мультиплексированием."""

    def __init__(self, host: str = '0.0.0.0', port: int = 9000):
        self.host = host
        self.port = port
        self.running = False

        self.server_socket_tcp: Optional[socket.socket] = None
        self.server_socket_udp: Optional[socket.socket] = None

        self.file_manager = FileManager()
        self.command_handler = CommandHandler(self.file_manager)

        # Списки для select
        self.inputs = []  # Sockets to read from
        self.outputs = [] # Sockets to write to

        # Буферы для TCP команд (т.к. команды могут приходить кусками)
        self.tcp_buffers: Dict[int, bytes] = {}

    def start(self) -> None:
        self._setup_signal_handlers()

        # Init TCP
        self.server_socket_tcp = create_server_socket(self.host, self.port)
        self.server_socket_tcp.setblocking(False)
        self.inputs.append(self.server_socket_tcp)

        # Init UDP
        self.server_socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            buff_size = 20 * 1024 * 1024 # Большой буфер для UDP
            self.server_socket_udp.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buff_size)
            self.server_socket_udp.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buff_size)
        except: pass
        self.server_socket_udp.bind((self.host, self.port))
        self.server_socket_udp.setblocking(False)
        self.inputs.append(self.server_socket_udp)

        self.running = True
        print(f"Multiplexed Server started on {self.host}:{self.port}")

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
                # 1. Prepare output list based on active downloads
                self.outputs = []
                # Ищем TCP сессии, которые находятся в режиме SENDING (Download)
                for client_id, session in list(self.file_manager.sessions.items()):
                    if not session.is_upload and session.sock: # TCP Download
                         if session.sock in self.inputs: # Socket is valid
                             self.outputs.append(session.sock)

                # 2. Select (Non-blocking or minimal timeout)
                # Таймаут мал, чтобы UDP очередь отправки работала быстро
                readable, writable, exceptional = select.select(self.inputs, self.outputs, self.inputs, 0.01)

                # 3. Handle Reads
                for s in readable:
                    if s is self.server_socket_tcp:
                        self._handle_tcp_accept()
                    elif s is self.server_socket_udp:
                        self._handle_udp_read()
                    else:
                        self._handle_tcp_read(s)

                # 4. Handle Writes (TCP Downloads)
                for s in writable:
                    self._handle_tcp_write(s)

                # 5. Handle Exceptions
                for s in exceptional:
                    self._remove_client(s)

                # 6. Process UDP Downloads (Queue processing)
                self._process_udp_downloads()

            except socket.error as e:
                if self.running: print(f"Loop error: {e}")
            except Exception as e:
                if self.running: print(f"Critical error: {e}")

    # --- TCP HANDLING ---

    def _handle_tcp_accept(self) -> None:
        try:
            client_sock, client_addr = self.server_socket_tcp.accept()
            print(f"TCP Connect: {client_addr}")
            client_sock.setblocking(False)
            self.inputs.append(client_sock)
            self.tcp_buffers[client_sock.fileno()] = b""
            send_all(client_sock, b"220 Welcome\n")
        except: pass

    def _handle_tcp_read(self, sock: socket.socket) -> None:
        """Обработка входящих данных TCP (Команда или Файл)."""
        client_id = str(sock.fileno())
        session = self.file_manager.get_session(client_id)

        # A. Если активна сессия Upload - читаем кусок файла
        if session and session.is_upload:
            try:
                # Читаем не все сразу, а чанк
                chunk = sock.recv(TCP_CHUNK_SIZE)
                if not chunk:
                    self._remove_client(sock)
                    return

                session.file_handle.write(chunk)
                session.transferred += len(chunk)

                if session.transferred >= session.total_size:
                    self.file_manager.complete_session(client_id)
                    bitrate = self.file_manager.calculate_bitrate(session)
                    msg = f"Received {session.transferred} bytes. {self.file_manager.format_bitrate(bitrate)}"
                    send_all(sock, format_response(Response(True, msg)))
                    print(f"TCP Upload finished: {client_id}")
            except socket.error:
                self._remove_client(sock)
            return

        # B. Если сессии нет или это не Upload - читаем команды
        try:
            data = sock.recv(4096)
            if not data:
                self._remove_client(sock)
                return

            buf = self.tcp_buffers.get(sock.fileno(), b"") + data

            if COMMAND_TERMINATOR in buf:
                line, rest = buf.split(COMMAND_TERMINATOR, 1)
                self.tcp_buffers[sock.fileno()] = rest

                cmd_str = line.decode('utf-8', errors='ignore')
                command = parse_command(cmd_str, 'TCP')

                # Запускаем обработчик (он только инициализирует сессию)
                response = self.command_handler.execute(command, sock, None, None)

                # Если это не команда начала трансфера, сразу шлем ответ
                # Если трансфер (UPLOAD/DOWNLOAD), хендлер уже послал READY/FILE
                if command.type not in (CommandType.UPLOAD, CommandType.DOWNLOAD,
                                      CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD):
                    send_all(sock, format_response(response))
            else:
                self.tcp_buffers[sock.fileno()] = buf

        except socket.error:
            self._remove_client(sock)

    def _handle_tcp_write(self, sock: socket.socket) -> None:
        """Отправка куска файла клиенту (Download)."""
        client_id = str(sock.fileno())
        session = self.file_manager.get_session(client_id)

        if session and not session.is_upload and session.file_handle:
            try:
                chunk = session.file_handle.read(TCP_CHUNK_SIZE)
                if chunk:
                    sock.send(chunk) # Используем send, а не send_all, чтобы не блокировать
                    session.transferred += len(chunk)
                else:
                    # EOF
                    self.file_manager.complete_session(client_id)
                    print(f"TCP Download finished: {client_id}")
            except socket.error:
                self._remove_client(sock)

    # --- UDP HANDLING ---

    def _handle_udp_read(self) -> None:
        """Чтение UDP пакета."""
        try:
            # Читаем ОДИН пакет (или несколько в цикле, если нужно быстрее освободить буфер)
            # Но для мультиплексирования лучше по одному
            pkt, addr = self.server_socket_udp.recvfrom(65536)
            client_id = f"{addr[0]}:{addr[1]}"

            rudp = RUDPSocket(self.server_socket_udp)
            seq, p_type, data = rudp._unpack_header(pkt)

            # A. COMMAND Packet
            if p_type == PacketType.CMD.value:
                # Send ACK
                ack = rudp._pack_packet(seq, PacketType.CMD.value, b"ACK_CMD")
                self.server_socket_udp.sendto(ack, addr)

                msg = data.decode(errors='ignore')
                if msg.startswith("OK ") or msg.startswith("ERROR "): return # Ignore ACKs to self

                print(f"UDP CMD from {client_id}: {msg}")
                command = parse_command(msg, 'UDP')

                # Execute returns Response.
                # Handlers for UPLOAD/DOWNLOAD initiate session and send initial packet inside handler.
                response = self.command_handler.execute(command, None, self.server_socket_udp, addr)

                if command.type not in (CommandType.UPLOAD, CommandType.DOWNLOAD,
                                      CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD):
                     resp_pkt = rudp._pack_packet(0, PacketType.CMD.value, format_response(response))
                     self.server_socket_udp.sendto(resp_pkt, addr)

            # B. DATA Packet (Upload to Server)
            elif p_type == PacketType.DATA.value:
                session = self.file_manager.get_session(client_id)
                if session and session.is_upload:
                    # Простая логика: пишем в файл
                    # В реальном RUDP тут нужно проверять seq и буферизировать
                    # Для ЛР: пишем и обновляем transferred
                    session.file_handle.write(data)
                    session.transferred += len(data)

                    # Send ACK occasionally
                    session.expected_seq = seq # Track last seq
                    # Ack strategy: every N packets
                    # Просто шлем ACK на текущий пакет (клиент сам разберется с децимацией)
                    # ack = rudp._pack_packet(seq + 1, PacketType.ACK.value, b"")
                    # self.server_socket_udp.sendto(ack, addr)
                    # Оптимизация: клиент сам шлет данные быстро, ACK шлем редко или полагаемся на timeout клиента?
                    # В нашем RUDP клиенте (rudp.py) клиент ждет ACK.
                    # Сервер должен слать ACK.
                    ack = rudp._pack_packet(seq + 1, PacketType.ACK.value, b"")
                    self.server_socket_udp.sendto(ack, addr)

            # C. FIN Packet (End of Upload)
            elif p_type == PacketType.FIN.value:
                session = self.file_manager.get_session(client_id)
                if session and session.is_upload:
                    ack = rudp._pack_packet(seq + 1, PacketType.ACK.value, b"")
                    self.server_socket_udp.sendto(ack, addr)

                    self.file_manager.complete_session(client_id)
                    print(f"UDP Upload finished: {client_id}")

            # D. ACK Packet (Download from Server)
            elif p_type == PacketType.ACK.value:
                 # Клиент подтвердил получение данных
                 # Можно сдвигать окно, но в простой реализации "Burst" мы просто шлем дальше
                 pass

        except BlockingIOError: pass
        except socket.error: pass

    def _process_udp_downloads(self) -> None:
        """Итерация по активным UDP скачиваниям и отправка Burst."""
        rudp = RUDPSocket(self.server_socket_udp)

        for client_id, session in list(self.file_manager.sessions.items()):
            if not session.is_upload and not session.sock: # UDP Download
                addr_parts = client_id.split(':')
                addr = (addr_parts[0], int(addr_parts[1]))

                # Burst send
                for _ in range(UDP_BURST_SIZE):
                    if session.transferred >= session.total_size:
                        # Send FIN
                        fin = rudp._pack_packet(session.next_seq_num, PacketType.FIN.value, b"")
                        self.server_socket_udp.sendto(fin, addr)
                        # Ждем немного (в реале нужен wait_ack state) и закрываем
                        # Для ЛР считаем выполненным
                        self.file_manager.complete_session(client_id)
                        print(f"UDP Download finished: {client_id}")
                        break

                    try:
                        chunk = session.file_handle.read(UDP_PAYLOAD_SIZE)
                        if not chunk: break

                        pkt = rudp._pack_packet(session.next_seq_num, PacketType.DATA.value, chunk)
                        self.server_socket_udp.sendto(pkt, addr)

                        session.next_seq_num += 1
                        session.transferred += len(chunk)
                    except:
                        break

    def _remove_client(self, sock: socket.socket) -> None:
        if sock in self.inputs: self.inputs.remove(sock)
        if sock in self.outputs: self.outputs.remove(sock)
        if sock.fileno() in self.tcp_buffers: del self.tcp_buffers[sock.fileno()]

        # Закрываем сессию если была
        self.file_manager.close_session(str(sock.fileno()))
        try: sock.close()
        except: pass

    def stop(self) -> None:
        self.running = False
        for s in self.inputs:
            try: s.close()
            except: pass