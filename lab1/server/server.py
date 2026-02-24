"""Мультиплексированный TCP/UDP сервер (Single Thread)."""

import socket
import select
import signal
import sys
import time
from typing import Optional, Dict

from common.protocol import (
    Command, CommandType, parse_command,
    format_response, COMMAND_TERMINATOR, Response,
    PacketType, UDP_PAYLOAD_SIZE, UDP_WINDOW_SIZE, UDP_TIMEOUT
)

from common.socket_utils import (
    create_server_socket, send_all
)

from common.rudp import RUDPSocket
from server.command_handler import CommandHandler
from server.file_manager import FileManager

TCP_CHUNK_SIZE = 32 * 1024

# Кол-во UDP пакетов для отправки за одну итерацию цикла download
UDP_BURST_SIZE = 32

# Сколько UDP пакетов читаем из сокета за одну итерацию
UDP_READ_BATCH = 256

# Ограничиваем реальное окно на сервере (чтобы не заливать сеть и не жечь память)
UDP_DOWNLOAD_WINDOW = 256

# Upload: сколько пакетов вперёд буферизуем out-of-order
UDP_UPLOAD_WINDOW = 2048

# FIN resend
UDP_FIN_RESEND_INTERVAL = 0.2
UDP_FIN_MAX_TRIES = 50


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

        self.inputs: list = []
        self.outputs: list = []
        self.tcp_buffers: Dict[int, bytes] = {}

        # общий RUDP-объект для упаковки/распаковки заголовков
        self._rudp: Optional[RUDPSocket] = None

    # ---------------- Запуск / остановка ---------------- #

    def start(self) -> None:
        self._setup_signal_handlers()

        # TCP
        self.server_socket_tcp = create_server_socket(self.host, self.port)
        self.server_socket_tcp.setblocking(False)
        self.inputs.append(self.server_socket_tcp)

        # UDP
        self.server_socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            buff_size = 50 * 1024 * 1024
            self.server_socket_udp.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buff_size)
            self.server_socket_udp.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buff_size)
        except Exception:
            pass

        self.server_socket_udp.bind((self.host, self.port))
        self.server_socket_udp.setblocking(False)
        self.inputs.append(self.server_socket_udp)

        self._rudp = RUDPSocket(self.server_socket_udp)

        self.running = True

        try:
            real_ip = socket.gethostbyname(socket.gethostname())
        except socket.error:
            real_ip = '127.0.0.1'

        display_host = real_ip if self.host in ('0.0.0.0', '') else self.host
        print(f"Server started on {display_host}:{self.port}")
        print(f"Listening on all interfaces (0.0.0.0:{self.port})")

        self._main_loop()

    def _setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame) -> None:
        print("\nShutting down server...")
        self.stop()
        sys.exit(0)

    def stop(self) -> None:
        self.running = False
        for s in self.inputs:
            try:
                s.close()
            except Exception:
                pass

    # ---------------- Главный цикл ---------------- #

    def _main_loop(self) -> None:
        while self.running:
            try:
                # формируем список сокетов на запись (TCP download)
                self.outputs = []
                for client_id, session in list(self.file_manager.sessions.items()):
                    if not session.is_upload and session.sock:
                        if session.sock in self.inputs:
                            self.outputs.append(session.sock)

                readable, writable, exceptional = select.select(
                    self.inputs, self.outputs, self.inputs, 0.005
                )

                for s in readable:
                    if s is self.server_socket_tcp:
                        self._handle_tcp_accept()
                    elif s is self.server_socket_udp:
                        self._handle_udp_read_batch()
                    else:
                        self._handle_tcp_read(s)

                for s in writable:
                    self._handle_tcp_write(s)

                for s in exceptional:
                    self._remove_client(s)

                # обработка всех активных UDP-download сессий
                self._process_udp_downloads()

            except socket.error as e:
                if self.running:
                    print(f"Loop error: {e}")
            except Exception as e:
                if self.running:
                    print(f"Critical error: {e}")

    # ---------------- TCP часть ---------------- #

    def _handle_tcp_accept(self) -> None:
        try:
            client_sock, client_addr = self.server_socket_tcp.accept()
            print(f"TCP Connect: {client_addr}")
            client_sock.setblocking(False)
            self.inputs.append(client_sock)
            self.tcp_buffers[client_sock.fileno()] = b""
            send_all(client_sock, b"220 Welcome\n")
        except Exception:
            pass

    def _handle_tcp_read(self, sock: socket.socket) -> None:
        client_id = str(sock.fileno())
        session = self.file_manager.get_session(client_id)

        # активный TCP upload
        if session and session.is_upload:
            try:
                chunk = sock.recv(TCP_CHUNK_SIZE)
                if not chunk:
                    self._remove_client(sock)
                    return

                session.file_handle.write(chunk)
                session.transferred += len(chunk)

                if session.transferred >= session.total_size:
                    bitrate = self.file_manager.calculate_bitrate(session)
                    msg = (
                        f"Received {session.transferred} bytes. "
                        f"{self.file_manager.format_bitrate(bitrate)}"
                    )

                    self.file_manager.complete_session(client_id)
                    send_all(sock, format_response(Response(True, msg)))
                    print(f"TCP Upload finished: {client_id}")

            except socket.error:
                self._remove_client(sock)
            return

        # команда
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
                response = self.command_handler.execute(command, sock, None, None)

                if command.type not in (
                    CommandType.UPLOAD, CommandType.DOWNLOAD,
                    CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD
                ):
                    send_all(sock, format_response(response))
                else:
                    self.tcp_buffers[sock.fileno()] = buf
            else:
                self.tcp_buffers[sock.fileno()] = buf

        except socket.error:
            self._remove_client(sock)

    def _handle_tcp_write(self, sock: socket.socket) -> None:
        client_id = str(sock.fileno())
        session = self.file_manager.get_session(client_id)

        if session and not session.is_upload and session.file_handle:
            try:
                chunk = session.file_handle.read(TCP_CHUNK_SIZE)
                if chunk:
                    sock.send(chunk)
                    session.transferred += len(chunk)
                else:
                    self.file_manager.complete_session(client_id)
                    print(f"TCP Download finished: {client_id}")
            except socket.error:
                self._remove_client(sock)

    # ---------------- UDP часть ---------------- #

    def _handle_udp_read_batch(self) -> None:
        """Читает до UDP_READ_BATCH UDP пакетов за одну итерацию."""

        if self.server_socket_udp is None or self._rudp is None:
            return

        rudp = self._rudp

        for _ in range(UDP_READ_BATCH):
            try:
                pkt, addr = self.server_socket_udp.recvfrom(65536)
            except (BlockingIOError, socket.error):
                break

            client_id = f"{addr[0]}:{addr[1]}"
            seq, p_type, data = rudp._unpack_header(pkt)

            # ----- A. Команды по UDP -----
            if p_type == PacketType.CMD.value:
                ack = rudp._pack_packet(seq, PacketType.CMD.value, b"ACK_CMD")
                try:
                    self.server_socket_udp.sendto(ack, addr)
                except Exception:
                    pass

                msg = data.decode(errors='ignore')
                if msg.startswith("OK ") or msg.startswith("ERROR "):
                    continue

                print(f"UDP CMD from {client_id}: {msg}")
                command = parse_command(msg, 'UDP')
                response = self.command_handler.execute(command, None, self.server_socket_udp, addr)

                if command.type not in (
                    CommandType.UPLOAD, CommandType.DOWNLOAD,
                    CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD
                ):
                    resp_pkt = rudp._pack_packet(0, PacketType.CMD.value, format_response(response))
                    try:
                        self.server_socket_udp.sendto(resp_pkt, addr)
                    except Exception:
                        pass

            # ----- B. DATA (UDP upload) -----
            elif p_type == PacketType.DATA.value:
                session = self.file_manager.get_session(client_id)
                if session and session.is_upload:
                    try:
                        if seq == session.expected_seq:
                            session.file_handle.write(data)
                            session.transferred += len(data)
                            session.expected_seq += 1

                            while session.expected_seq in session.udp_recv_buffer:
                                buf = session.udp_recv_buffer.pop(session.expected_seq)
                                session.file_handle.write(buf)
                                session.transferred += len(buf)
                                session.expected_seq += 1

                        elif seq > session.expected_seq:
                            if seq < session.expected_seq + UDP_UPLOAD_WINDOW:
                                session.udp_recv_buffer.setdefault(seq, data)

                        ack_pkt = rudp._pack_packet(session.expected_seq, PacketType.ACK.value, b"")
                        self.server_socket_udp.sendto(ack_pkt, addr)

                    except Exception as e:
                        print(f"UDP DATA error: {e}")

            # ----- C. FIN (конец UDP upload) -----
            elif p_type == PacketType.FIN.value:
                session = self.file_manager.get_session(client_id)
                if session and session.is_upload:
                    try:
                        if seq == session.expected_seq:
                            ack = rudp._pack_packet(seq + 1, PacketType.ACK.value, b"")
                            for _ in range(3):
                                try:
                                    self.server_socket_udp.sendto(ack, addr)
                                except Exception:
                                    pass

                            bitrate = self.file_manager.calculate_bitrate(session)
                            print(
                                f"UDP Upload finished: {client_id} — "
                                f"{self.file_manager.format_bitrate(bitrate)}"
                            )
                            self.file_manager.complete_session(client_id)
                        else:
                            ack = rudp._pack_packet(session.expected_seq, PacketType.ACK.value, b"")
                            self.server_socket_udp.sendto(ack, addr)

                    except Exception as e:
                        print(f"UDP FIN error: {e}")

            # ----- D. ACK (UDP download) -----
            elif p_type == PacketType.ACK.value:
                session = self.file_manager.get_session(client_id)
                if not session:
                    continue

                if session.is_upload or session.sock is not None:
                    continue

                now = time.time()
                session.last_activity = now
                ack_seq = seq

                if session.udp_fin_sent and ack_seq == session.udp_fin_seq + 1:
                    session.udp_fin_acked = True
                    continue

                if ack_seq < session.window_base:
                    continue

                if ack_seq > session.next_seq_num:
                    ack_seq = session.next_seq_num

                if ack_seq > session.window_base:
                    for s in range(session.window_base, ack_seq):
                        session.udp_packets.pop(s, None)

                    session.window_base = ack_seq
                    session.udp_last_ack_time = now

    def _process_udp_downloads(self) -> None:
        """Надёжный UDP-download: sliding window + ретрансмит + FIN handshake."""

        if self.server_socket_udp is None or self._rudp is None:
            return

        rudp = self._rudp
        now = time.time()
        window_size = min(UDP_WINDOW_SIZE, UDP_DOWNLOAD_WINDOW)

        for client_id, session in list(self.file_manager.sessions.items()):
            if session.is_upload or session.sock is not None:
                continue

            if not session.file_handle:
                self.file_manager.close_session(client_id)
                continue

            try:
                host, port_s = client_id.split(':', 1)
                addr = (host, int(port_s))
            except Exception:
                self.file_manager.close_session(client_id)
                continue

            if session.udp_last_ack_time == 0.0:
                session.udp_last_ack_time = now

            # FIN stage
            if session.udp_eof and session.window_base >= session.next_seq_num:
                if not session.udp_fin_sent:
                    session.udp_fin_sent = True
                    session.udp_fin_seq = session.next_seq_num
                    session.udp_last_fin_time = 0.0
                    session.udp_fin_tries = 0

                if session.udp_fin_acked:
                    bitrate = self.file_manager.calculate_bitrate(session)
                    print(
                        f"UDP Download finished: {client_id} — "
                        f"{self.file_manager.format_bitrate(bitrate)}"
                    )
                    self.file_manager.complete_session(client_id)
                    continue

                if (now - session.udp_last_fin_time) >= UDP_FIN_RESEND_INTERVAL:
                    fin_pkt = rudp._pack_packet(session.udp_fin_seq, PacketType.FIN.value, b"")
                    try:
                        self.server_socket_udp.sendto(fin_pkt, addr)
                    except Exception:
                        pass

                    session.udp_last_fin_time = now
                    session.udp_fin_tries += 1

                    if session.udp_fin_tries >= UDP_FIN_MAX_TRIES:
                        print(f"UDP Download FIN timeout: {client_id}")
                        self.file_manager.close_session(client_id)

                continue

            # Send new data
            burst_left = UDP_BURST_SIZE
            while burst_left > 0 and session.next_seq_num < session.window_base + window_size:
                if session.transferred >= session.total_size:
                    session.udp_eof = True
                    break

                to_read = min(UDP_PAYLOAD_SIZE, session.total_size - session.transferred)
                if to_read <= 0:
                    session.udp_eof = True
                    break

                try:
                    chunk = session.file_handle.read(to_read)
                except Exception:
                    chunk = b""

                if not chunk:
                    session.udp_eof = True
                    break

                pkt = rudp._pack_packet(session.next_seq_num, PacketType.DATA.value, chunk)
                session.udp_packets[session.next_seq_num] = pkt

                try:
                    self.server_socket_udp.sendto(pkt, addr)
                except Exception:
                    session.udp_packets.pop(session.next_seq_num, None)
                    break

                session.next_seq_num += 1
                session.transferred += len(chunk)
                session.last_activity = now
                burst_left -= 1

            # Retransmit
            if (now - session.udp_last_ack_time) > UDP_TIMEOUT and session.udp_packets:
                resent = 0
                for s in range(session.window_base, session.next_seq_num):
                    if s in session.udp_packets:
                        try:
                            self.server_socket_udp.sendto(session.udp_packets[s], addr)
                        except Exception:
                            break
                        resent += 1
                        if resent >= UDP_BURST_SIZE:
                            break

                session.udp_last_ack_time = now

    # ---------------- Service ---------------- #

    def _remove_client(self, sock: socket.socket) -> None:
        if sock in self.inputs:
            self.inputs.remove(sock)
        if sock in self.outputs:
            self.outputs.remove(sock)
        if sock.fileno() in self.tcp_buffers:
            del self.tcp_buffers[sock.fileno()]

        self.file_manager.close_session(str(sock.fileno()))

        try:
            sock.close()
        except Exception:
            pass
