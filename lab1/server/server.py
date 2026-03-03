"""TCP/UDP сервер с UDP upload в основном цикле и быстрым RUDP."""

import socket
import select
import signal
import struct
import sys
import time
from datetime import datetime
from typing import Optional, Dict

from common.protocol import (
    CommandType,
    PacketType,
    parse_command,
    format_response,
    COMMAND_TERMINATOR,
)
from common.socket_utils import create_server_socket, send_all
from common.rudp import RUDPSocket
from server.command_handler import CommandHandler
from server.file_manager import FileManager

TCP_CHUNK = 64 * 1024
UDP_READ_BATCH = 1024
UDP_UPLOAD_WIN = 32768  # большое окно под агрессивный sender
_HDR = struct.Struct("!IB")


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


class TCPServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 9000):
        self.host = host
        self.port = port
        self.running = False

        self.server_socket_tcp: Optional[socket.socket] = None
        self.server_socket_udp: Optional[socket.socket] = None

        self.file_manager = FileManager()
        self.command_handler = CommandHandler(self.file_manager)

        self.inputs = []
        self.outputs = []
        self.tcp_buffers: Dict[int, bytes] = {}

        self._rudp: Optional[RUDPSocket] = None

    # ── lifecycle ──────────────────────────────────────────

    def start(self) -> None:
        self._setup_signals()

        # TCP
        self.server_socket_tcp = create_server_socket(self.host, self.port)
        self.server_socket_tcp.setblocking(False)
        self.inputs.append(self.server_socket_tcp)

        # UDP
        self.server_socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try:
                self.server_socket_udp.setsockopt(socket.SOL_SOCKET, opt, 16 * 1024 * 1024)
            except OSError:
                pass
        self.server_socket_udp.bind((self.host, self.port))
        self.server_socket_udp.setblocking(False)
        self.inputs.append(self.server_socket_udp)
        self._rudp = RUDPSocket(self.server_socket_udp)

        self.running = True
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except OSError:
            ip = "127.0.0.1"
        bind_ip = ip if self.host in ("0.0.0.0", "") else self.host
        print(f"[{_ts()}] ===== Server on {bind_ip}:{self.port} =====")

        self._loop()

    def _setup_signals(self) -> None:
        signal.signal(signal.SIGINT, lambda *_: (self.stop(), sys.exit(0)))
        signal.signal(signal.SIGTERM, lambda *_: (self.stop(), sys.exit(0)))

    def stop(self) -> None:
        self.running = False
        for s in self.inputs:
            try:
                s.close()
            except OSError:
                pass

    # ── main loop ─────────────────────────────────────────

    def _loop(self) -> None:
        while self.running:
            try:
                self.outputs = [
                    s.sock
                    for s in self.file_manager.sessions.values()
                    if not s.is_upload and s.sock is not None and s.sock in self.inputs
                ]

                readable, writable, exceptional = select.select(
                    self.inputs, self.outputs, self.inputs, 0.01
                )

                for s in readable:
                    if s is self.server_socket_tcp:
                        self._tcp_accept()
                    elif s is self.server_socket_udp:
                        self._udp_read()
                    else:
                        self._tcp_read(s)

                for s in writable:
                    self._tcp_write(s)

                for s in exceptional:
                    self._remove(s)
            except Exception:
                # не даём серверу упасть из‑за одной ошибки
                continue

    # ── TCP handlers ──────────────────────────────────────

    def _tcp_accept(self) -> None:
        try:
            client_sock, addr = self.server_socket_tcp.accept()
        except OSError:
            return

        print(f"[{_ts()}] TCP Connect: {addr[0]}:{addr[1]}")
        client_sock.setblocking(False)
        self.inputs.append(client_sock)
        self.tcp_buffers[client_sock.fileno()] = b""
        send_all(client_sock, b"220 Welcome\n")

    def _tcp_read(self, sock: socket.socket) -> None:
        cid = str(sock.fileno())
        session = self.file_manager.get_session(cid)

        # TCP upload data
        if session and session.is_upload:
            try:
                chunk = sock.recv(TCP_CHUNK)
            except OSError:
                self._remove(sock)
                return

            if not chunk:
                self._remove(sock)
                return

            session.file_handle.write(chunk)
            session.transferred += len(chunk)
            self._log(cid, session, "TCP Upload")

            if session.transferred >= session.total_size:
                bitrate = self.file_manager.calculate_bitrate(session)
                self.file_manager.complete_session(cid)
                msg = (
                    f"Received {session.transferred} bytes. "
                    f"{self.file_manager.format_bitrate(bitrate)}"
                )
                send_all(sock, format_response(self._ok(msg)))
                print(f"[{_ts()}] TCP Upload done ({self.file_manager.format_bitrate(bitrate)})")
            return

        # командный канал
        try:
            data = sock.recv(4096)
        except OSError:
            self._remove(sock)
            return

        if not data:
            self._remove(sock)
            return

        buf = self.tcp_buffers.get(sock.fileno(), b"") + data
        if COMMAND_TERMINATOR in buf:
            line, rest = buf.split(COMMAND_TERMINATOR, 1)
            self.tcp_buffers[sock.fileno()] = rest

            raw = line.decode("utf-8", errors="ignore")
            cmd = parse_command(raw, "TCP")
            print(f"[{_ts()}] TCP CMD [{cid}]: {raw.strip()}")

            resp = self.command_handler.execute(cmd, sock, None, None)
            transfer_cmds = (
                CommandType.UPLOAD,
                CommandType.DOWNLOAD,
                CommandType.RESUME_UPLOAD,
                CommandType.RESUME_DOWNLOAD,
            )
            if cmd.type not in transfer_cmds or not resp.success:
                send_all(sock, format_response(resp))
        else:
            self.tcp_buffers[sock.fileno()] = buf

    def _tcp_write(self, sock: socket.socket) -> None:
        cid = str(sock.fileno())
        session = self.file_manager.get_session(cid)
        if not (session and not session.is_upload and session.file_handle):
            return

        try:
            chunk = session.file_handle.read(TCP_CHUNK)
        except OSError:
            self._remove(sock)
            return

        if chunk:
            try:
                sock.send(chunk)
            except OSError:
                self._remove(sock)
                return
            session.transferred += len(chunk)
            self._log(cid, session, "TCP Download")
        else:
            bitrate = self.file_manager.calculate_bitrate(session)
            self.file_manager.complete_session(cid)
            print(f"[{_ts()}] TCP Download done ({self.file_manager.format_bitrate(bitrate)})")

    # ── UDP handlers ──────────────────────────────────────

    def _udp_read(self) -> None:
        if not self.server_socket_udp or not self._rudp:
            return

        rudp = self._rudp
        for _ in range(UDP_READ_BATCH):
            try:
                pkt, addr = self.server_socket_udp.recvfrom(65536)
            except (BlockingIOError, OSError):
                break

            cid = f"{addr[0]}:{addr[1]}"
            seq, ptype, data = rudp._unpack(pkt)

            # командный пакет
            if ptype == PacketType.CMD.value:
                rudp._send(rudp._pack(seq, PacketType.CMD.value, b"ACK_CMD"), addr)
                msg = data.decode(errors="ignore")
                if msg.startswith("OK") or msg.startswith("ERROR"):
                    continue

                cmd = parse_command(msg, "UDP")
                print(f"[{_ts()}] UDP CMD [{cid}]: {msg.strip()}")

                resp = self.command_handler.execute(cmd, None, self.server_socket_udp, addr)
                transfer_cmds = (
                    CommandType.UPLOAD,
                    CommandType.DOWNLOAD,
                    CommandType.RESUME_UPLOAD,
                    CommandType.RESUME_DOWNLOAD,
                )
                if cmd.type in transfer_cmds and not resp.success:
                    rudp._send(
                        rudp._pack(0, PacketType.CMD.value, format_response(resp)),
                        addr,
                    )
                continue

            # данные upload
            if ptype == PacketType.DATA.value:
                self._udp_handle_data(cid, seq, data)
            # FIN обрабатывает RUDP внутри recv_stream (для download)

    def _udp_handle_data(self, cid: str, seq: int, data: bytes) -> None:
        session = self.file_manager.get_session(cid)
        if not session or not session.is_upload:
            return

        if seq == session.expected_seq:
            session.file_handle.write(data)
            session.transferred += len(data)
            session.expected_seq += 1

            while session.expected_seq in session.udp_recv_buffer:
                d = session.udp_recv_buffer.pop(session.expected_seq)
                session.file_handle.write(d)
                session.transferred += len(d)
                session.expected_seq += 1

            self._log(cid, session, "UDP Upload")
        elif seq > session.expected_seq:
            if seq < session.expected_seq + UDP_UPLOAD_WIN:
                session.udp_recv_buffer.setdefault(seq, data)

    # ── utils ─────────────────────────────────────────────

    @staticmethod
    def _ok(msg: str):
        from common.protocol import Response

        return Response(True, msg)

    def _log(self, cid: str, session, op: str) -> None:
        if session.total_size <= 0:
            return
        pct = int(session.transferred / session.total_size * 100)
        last = getattr(session, "_last_pct", -10)
        if pct >= last + 10 or pct == 100:
            session._last_pct = pct
            print(f"[{_ts()}] {op}: {session.filename} [{cid}] — {pct}%")

    def _remove(self, sock: socket.socket) -> None:
        try:
            addr = sock.getpeername()
            print(f"[{_ts()}] Disconnected: {addr[0]}:{addr[1]}")
        except OSError:
            pass

        if sock in self.inputs:
            self.inputs.remove(sock)
        if sock in self.outputs:
            self.outputs.remove(sock)
        self.tcp_buffers.pop(sock.fileno(), None)
        self.file_manager.close_session(str(sock.fileno()))
        try:
            sock.close()
        except OSError:
            pass
