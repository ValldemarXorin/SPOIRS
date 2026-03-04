"""TCP/UDP сервер.

UDP upload: обработка DATA/ACK в select-цикле.
UDP download: send_stream через выделенный сокет в отдельном потоке.

Кроссплатформенный: Windows + Linux.
"""

import socket
import select
import signal
import struct
import sys
import time
import threading
from datetime import datetime
from typing import Optional, Dict, Tuple

from common.protocol import (
    CommandType, PacketType, parse_command, format_response,
    COMMAND_TERMINATOR, UDP_WINDOW_SIZE, Response,
)
from common.socket_utils import create_server_socket, send_all, create_udp_socket
from common.rudp import RUDPSocket, ConnectionLostError
from server.command_handler import CommandHandler
from server.file_manager import FileManager, TransferSession

TCP_CHUNK = 64 * 1024
UDP_READ_BATCH = 4096
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

    def start(self) -> None:
        self._setup_signals()

        self.server_socket_tcp = create_server_socket(self.host, self.port)
        self.server_socket_tcp.setblocking(False)
        self.inputs.append(self.server_socket_tcp)

        self.server_socket_udp = create_udp_socket()
        self.server_socket_udp.bind((self.host, self.port))
        self.server_socket_udp.setblocking(False)
        self.inputs.append(self.server_socket_udp)

        self.running = True
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except OSError:
            ip = "127.0.0.1"
        bind_ip = ip if self.host in ("0.0.0.0", "") else self.host
        print(f"[{_ts()}] ===== Server on {bind_ip}:{self.port} (TCP+UDP) =====")

        self._loop()

    def _setup_signals(self) -> None:
        signal.signal(signal.SIGINT, lambda *_: self._shutdown())
        signal.signal(signal.SIGTERM, lambda *_: self._shutdown())

    def _shutdown(self) -> None:
        self.running = False
        self.stop()
        sys.exit(0)

    def stop(self) -> None:
        self.running = False
        for s in list(self.inputs):
            try:
                s.close()
            except OSError:
                pass

    def _loop(self) -> None:
        while self.running:
            try:
                self.outputs = [
                    s.sock for s in self.file_manager.sessions.values()
                    if (not s.is_upload and s.sock is not None
                        and s.sock in self.inputs)
                ]

                readable, writable, exceptional = select.select(
                    self.inputs, self.outputs, self.inputs, 0.005
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

                self._udp_send_acks()

            except Exception as e:
                if self.running:
                    print(f"[{_ts()}] Loop error: {e}")

    # ── TCP ───────────────────────────────────────────────

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

        if session and session.is_upload:
            try:
                chunk = sock.recv(TCP_CHUNK)
            except OSError:
                self._remove(sock)
                return
            if not chunk:
                self._remove(sock)
                return
            try:
                session.file_handle.write(chunk)
            except (IOError, OSError):
                self._remove(sock)
                return
            session.transferred += len(chunk)
            session.last_activity = time.time()
            self._log(cid, session, "TCP Upload")

            if session.transferred >= session.total_size:
                bitrate = self.file_manager.calculate_bitrate(session)
                br_str = self.file_manager.format_bitrate(bitrate)
                self.file_manager.complete_session(cid)
                send_all(sock, format_response(
                    Response(True, f"Received {session.transferred} bytes. {br_str}")
                ))
                print(f"[{_ts()}] TCP Upload done ({br_str})")
            return

        try:
            data = sock.recv(4096)
        except OSError:
            self._remove(sock)
            return
        if not data:
            self._remove(sock)
            return

        fd = sock.fileno()
        buf = self.tcp_buffers.get(fd, b"") + data

        while COMMAND_TERMINATOR in buf:
            line, buf = buf.split(COMMAND_TERMINATOR, 1)
            raw = line.decode("utf-8", errors="ignore")
            cmd = parse_command(raw, "TCP")
            print(f"[{_ts()}] TCP CMD [{cid}]: {raw.strip()}")

            resp = self.command_handler.execute(cmd, sock, None, None)
            transfer_cmds = {CommandType.UPLOAD, CommandType.DOWNLOAD,
                             CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD}
            if cmd.type not in transfer_cmds or not resp.success:
                send_all(sock, format_response(resp))

        self.tcp_buffers[fd] = buf

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
                sent = sock.send(chunk)
                if sent < len(chunk):
                    session.file_handle.seek(-(len(chunk) - sent), 1)
                session.transferred += sent
            except OSError:
                self._remove(sock)
                return
            session.last_activity = time.time()
            self._log(cid, session, "TCP Download")
        else:
            bitrate = self.file_manager.calculate_bitrate(session)
            br_str = self.file_manager.format_bitrate(bitrate)
            self.file_manager.complete_session(cid)
            print(f"[{_ts()}] TCP Download done ({br_str})")

    # ── UDP ───────────────────────────────────────────────

    def _udp_read(self) -> None:
        if not self.server_socket_udp:
            return

        for _ in range(UDP_READ_BATCH):
            try:
                pkt, addr = self.server_socket_udp.recvfrom(65536)
            except (BlockingIOError, OSError):
                break

            if len(pkt) < 5:
                continue

            seq, ptype = _HDR.unpack_from(pkt)
            data = pkt[5:]
            cid = f"{addr[0]}:{addr[1]}"

            if ptype == PacketType.CMD.value:
                self._udp_handle_cmd(cid, seq, data, addr)
            elif ptype == PacketType.DATA.value:
                self._udp_handle_data(cid, seq, data, addr)
            elif ptype == PacketType.FIN.value:
                self._udp_handle_fin(cid, seq, addr)

    def _udp_handle_cmd(self, cid: str, seq: int, data: bytes,
                        addr: Tuple[str, int]) -> None:
        ack_pkt = _HDR.pack(seq, PacketType.CMD.value) + b"ACK_CMD"
        try:
            self.server_socket_udp.sendto(ack_pkt, addr)
        except OSError:
            pass

        msg = data.decode(errors="ignore")
        if msg.startswith("OK") or msg.startswith("ERROR") or msg == "ACK_CMD":
            return

        cmd = parse_command(msg, "UDP")
        print(f"[{_ts()}] UDP CMD [{cid}]: {msg.strip()}")

        resp = self.command_handler.execute(
            cmd, None, self.server_socket_udp, addr
        )

        transfer_cmds = {CommandType.UPLOAD, CommandType.DOWNLOAD,
                         CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD}

        if cmd.type in transfer_cmds:
            if not resp.success:
                err_pkt = _HDR.pack(0, PacketType.CMD.value) + format_response(resp)
                try:
                    self.server_socket_udp.sendto(err_pkt, addr)
                except OSError:
                    pass
            elif cmd.type in (CommandType.DOWNLOAD, CommandType.RESUME_DOWNLOAD):
                self._start_udp_download(cid, addr)
        else:
            resp_pkt = _HDR.pack(0, PacketType.CMD.value) + format_response(resp)
            try:
                self.server_socket_udp.sendto(resp_pkt, addr)
            except OSError:
                pass

    def _udp_handle_data(self, cid: str, seq: int, data: bytes,
                         addr: Tuple[str, int]) -> None:
        session = self.file_manager.get_session(cid)
        if not session or not session.is_upload:
            return

        session.last_activity = time.time()

        if seq == session.expected_seq:
            try:
                session.file_handle.write(data)
            except (IOError, OSError):
                return
            session.transferred += len(data)
            session.expected_seq += 1

            while session.expected_seq in session.udp_recv_buffer:
                d = session.udp_recv_buffer.pop(session.expected_seq)
                try:
                    session.file_handle.write(d)
                except (IOError, OSError):
                    return
                session.transferred += len(d)
                session.expected_seq += 1

            self._log(cid, session, "UDP Upload")

        elif seq > session.expected_seq:
            if seq < session.expected_seq + UDP_WINDOW_SIZE * 4:
                session.udp_recv_buffer.setdefault(seq, data)
            nack = _HDR.pack(session.expected_seq, PacketType.NACK.value)
            try:
                self.server_socket_udp.sendto(nack, addr)
            except OSError:
                pass

    def _udp_handle_fin(self, cid: str, seq: int,
                        addr: Tuple[str, int]) -> None:
        fin_ack = _HDR.pack(seq + 1, PacketType.ACK.value)
        for _ in range(5):
            try:
                self.server_socket_udp.sendto(fin_ack, addr)
            except OSError:
                pass

        session = self.file_manager.get_session(cid)
        if session and session.is_upload:
            bitrate = self.file_manager.calculate_bitrate(session)
            br_str = self.file_manager.format_bitrate(bitrate)
            self.file_manager.complete_session(cid)
            print(f"[{_ts()}] UDP Upload done: {session.filename} ({br_str})")

    def _udp_send_acks(self) -> None:
        """Частые ACK для UDP upload — ключ к скорости sender'а."""
        now = time.time()
        for cid, session in list(self.file_manager.sessions.items()):
            if not session.is_upload:
                continue
            if ":" not in cid or cid.isdigit():
                continue
            if now - session.udp_last_ack_time < 0.002:
                continue

            parts = cid.rsplit(":", 1)
            if len(parts) != 2:
                continue
            try:
                addr = (parts[0], int(parts[1]))
            except ValueError:
                continue

            ack = _HDR.pack(session.expected_seq, PacketType.ACK.value)
            try:
                self.server_socket_udp.sendto(ack, addr)
            except OSError:
                pass
            session.udp_last_ack_time = now

    def _start_udp_download(self, cid: str, addr: Tuple[str, int]) -> None:
        """UDP download через выделенный сокет в отдельном потоке."""
        session = self.file_manager.get_session(cid)
        if not session or session.is_upload:
            return

        session.udp_download_active = True
        session.udp_client_addr = addr
        file_handle = session.file_handle
        total_size = session.total_size
        filename = session.filename

        def download_worker():
            dl_sock = create_udp_socket()
            dl_sock.setblocking(False)
            dl_sock.bind(('', 0))
            dl_port = dl_sock.getsockname()[1]

            print(f"[{_ts()}] UDP Download: {filename} → "
                  f"{addr[0]}:{addr[1]} via port {dl_port}")

            # Отправляем клиенту порт (надёжно, несколько раз)
            port_pkt = (_HDR.pack(0, PacketType.CMD.value) +
                        f"DOWNLOAD_PORT {dl_port}".encode())
            for i in range(10):
                try:
                    self.server_socket_udp.sendto(port_pkt, addr)
                except OSError:
                    pass
                time.sleep(0.03)

            # Ждём hello от клиента
            client_dl_addr = None
            t0 = time.monotonic()
            while time.monotonic() - t0 < 10.0:
                r, _, _ = select.select([dl_sock], [], [], 0.1)
                if r:
                    try:
                        _, ca = dl_sock.recvfrom(65536)
                        client_dl_addr = ca
                        break
                    except OSError:
                        continue

            if client_dl_addr is None:
                print(f"[{_ts()}] UDP Download: no hello from client")
                self.file_manager.close_session(cid)
                dl_sock.close()
                return

            print(f"[{_ts()}] UDP Download: client at "
                  f"{client_dl_addr[0]}:{client_dl_addr[1]}, starting stream")

            try:
                rudp = RUDPSocket(dl_sock, dest_addr=client_dl_addr)
                rudp.send_stream(
                    file_handle,
                    total_size=total_size,
                    progress_callback=lambda s: self._update_dl(cid, s, session),
                )
                session.transferred = total_size
                bitrate = self.file_manager.calculate_bitrate(session)
                br_str = self.file_manager.format_bitrate(bitrate)
                print(f"[{_ts()}] UDP Download done: {filename} ({br_str})")
            except ConnectionLostError as e:
                print(f"[{_ts()}] UDP Download failed: {e}")
            except Exception as e:
                print(f"[{_ts()}] UDP Download error: {e}")
            finally:
                self.file_manager.complete_session(cid)
                dl_sock.close()

        t = threading.Thread(target=download_worker, daemon=True)
        t.start()

    def _update_dl(self, cid: str, transferred: int,
                   session: TransferSession) -> None:
        session.transferred = transferred
        session.last_activity = time.time()
        self._log(cid, session, "UDP Download")

    # ── utils ─────────────────────────────────────────────

    def _log(self, cid: str, session: TransferSession, op: str) -> None:
        if session.total_size <= 0:
            return
        pct = int(session.transferred / session.total_size * 100)
        last = session._last_pct
        if pct >= last + 10 or pct == 100:
            session._last_pct = pct
            print(f"[{_ts()}] {op}: {session.filename} [{cid}] — {pct}%")

    def _remove(self, sock: socket.socket) -> None:
        try:
            addr = sock.getpeername()
            print(f"[{_ts()}] Disconnected: {addr[0]}:{addr[1]}")
        except OSError:
            pass

        fd = sock.fileno()
        if sock in self.inputs:
            self.inputs.remove(sock)
        if sock in self.outputs:
            self.outputs.remove(sock)
        self.tcp_buffers.pop(fd, None)
        self.file_manager.close_session(str(fd))
        try:
            sock.close()
        except OSError:
            pass