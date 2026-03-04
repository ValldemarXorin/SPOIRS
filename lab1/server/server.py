"""TCP/UDP сервер — обработка upload через non-blocking select loop,
download через блокирующий RUDP send_stream в том же потоке.

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
    COMMAND_TERMINATOR, UDP_PAYLOAD_SIZE, UDP_WINDOW_SIZE, UDP_TIMEOUT,
    Response,
)
from common.socket_utils import create_server_socket, send_all, create_udp_socket
from common.rudp import RUDPSocket, ConnectionLostError
from server.command_handler import CommandHandler
from server.file_manager import FileManager, TransferSession

TCP_CHUNK = 64 * 1024
UDP_READ_BATCH = 2048
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

        # Track active UDP download threads
        self._udp_download_threads: Dict[str, threading.Thread] = {}

    # ── lifecycle ──────────────────────────────────────────

    def start(self) -> None:
        self._setup_signals()

        # TCP
        self.server_socket_tcp = create_server_socket(self.host, self.port)
        self.server_socket_tcp.setblocking(False)
        self.inputs.append(self.server_socket_tcp)

        # UDP
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

    # ── main loop ─────────────────────────────────────────

    def _loop(self) -> None:
        while self.running:
            try:
                self.outputs = [
                    s.sock
                    for s in self.file_manager.sessions.values()
                    if (not s.is_upload and s.sock is not None
                        and s.sock in self.inputs)
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

                # Process pending UDP upload ACKs
                self._udp_process_uploads()

            except Exception as e:
                if self.running:
                    print(f"[{_ts()}] Loop error: {e}")
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

            try:
                session.file_handle.write(chunk)
            except (IOError, OSError) as e:
                print(f"[{_ts()}] Write error: {e}")
                self._remove(sock)
                return

            session.transferred += len(chunk)
            session.last_activity = time.time()
            self._log(cid, session, "TCP Upload")

            if session.transferred >= session.total_size:
                bitrate = self.file_manager.calculate_bitrate(session)
                br_str = self.file_manager.format_bitrate(bitrate)
                self.file_manager.complete_session(cid)
                msg = f"Received {session.transferred} bytes. {br_str}"
                send_all(sock, format_response(Response(True, msg)))
                print(f"[{_ts()}] TCP Upload done ({br_str})")
            return

        # Command channel
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
            transfer_cmds = {
                CommandType.UPLOAD, CommandType.DOWNLOAD,
                CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD,
            }
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
                    # Seek back for unsent data
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

    # ── UDP handlers ──────────────────────────────────────

    def _udp_read(self) -> None:
        """Read all available UDP packets and dispatch."""
        if not self.server_socket_udp:
            return

        for _ in range(UDP_READ_BATCH):
            try:
                pkt, addr = self.server_socket_udp.recvfrom(65536)
            except (BlockingIOError, OSError):
                break

            if len(pkt) < 5:  # UDP_HEADER_SIZE
                continue

            seq, ptype = _HDR.unpack_from(pkt)
            data = pkt[5:]
            cid = f"{addr[0]}:{addr[1]}"

            # ── Command packet ──
            if ptype == PacketType.CMD.value:
                self._udp_handle_cmd(cid, seq, data, addr)
                continue

            # ── DATA packet (upload from client) ──
            if ptype == PacketType.DATA.value:
                self._udp_handle_data(cid, seq, data, addr)
                continue

            # ── FIN packet (upload from client) ──
            if ptype == PacketType.FIN.value:
                self._udp_handle_fin(cid, seq, addr)
                continue

            # ── ACK packet (response to our download send) ──
            if ptype == PacketType.ACK.value:
                # Handled by download thread's recv_stream ACK processing
                pass

    def _udp_handle_cmd(self, cid: str, seq: int, data: bytes,
                        addr: Tuple[str, int]) -> None:
        """Handle a UDP command packet."""
        # Send command ACK immediately
        ack_pkt = _HDR.pack(seq, PacketType.CMD.value) + b"ACK_CMD"
        try:
            self.server_socket_udp.sendto(ack_pkt, addr)
        except OSError:
            pass

        msg = data.decode(errors="ignore")

        # Skip if it's a response (not a command)
        if msg.startswith("OK") or msg.startswith("ERROR"):
            return

        cmd = parse_command(msg, "UDP")
        print(f"[{_ts()}] UDP CMD [{cid}]: {msg.strip()}")

        resp = self.command_handler.execute(
            cmd, None, self.server_socket_udp, addr
        )

        transfer_cmds = {
            CommandType.UPLOAD, CommandType.DOWNLOAD,
            CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD,
        }

        if cmd.type in transfer_cmds:
            if not resp.success:
                err_pkt = (_HDR.pack(0, PacketType.CMD.value) +
                           format_response(resp))
                try:
                    self.server_socket_udp.sendto(err_pkt, addr)
                except OSError:
                    pass
            elif cmd.type in (CommandType.DOWNLOAD,
                              CommandType.RESUME_DOWNLOAD):
                # Start download in a separate thread using dedicated socket
                self._start_udp_download(cid, addr)
        else:
            # Non-transfer command — send response
            resp_pkt = (_HDR.pack(0, PacketType.CMD.value) +
                        format_response(resp))
            try:
                self.server_socket_udp.sendto(resp_pkt, addr)
            except OSError:
                pass

    def _udp_handle_data(self, cid: str, seq: int, data: bytes,
                         addr: Tuple[str, int]) -> None:
        """Handle incoming DATA packet for UDP upload."""
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

            # Drain out-of-order buffer
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
            max_buf = UDP_WINDOW_SIZE * 4
            if seq < session.expected_seq + max_buf:
                session.udp_recv_buffer.setdefault(seq, data)

            # Send NACK for missing packet
            nack = _HDR.pack(session.expected_seq, PacketType.NACK.value)
            try:
                self.server_socket_udp.sendto(nack, addr)
            except OSError:
                pass

    def _udp_handle_fin(self, cid: str, seq: int,
                        addr: Tuple[str, int]) -> None:
        """Handle FIN packet — finalize upload."""
        session = self.file_manager.get_session(cid)

        # Send FIN-ACK
        fin_ack = _HDR.pack(seq + 1, PacketType.ACK.value)
        for _ in range(5):
            try:
                self.server_socket_udp.sendto(fin_ack, addr)
            except OSError:
                pass

        if session and session.is_upload:
            bitrate = self.file_manager.calculate_bitrate(session)
            br_str = self.file_manager.format_bitrate(bitrate)
            self.file_manager.complete_session(cid)
            print(f"[{_ts()}] UDP Upload done: {session.filename} ({br_str})")

    def _udp_process_uploads(self) -> None:
        """Send periodic ACKs for active UDP upload sessions."""
        now = time.time()
        for cid, session in list(self.file_manager.sessions.items()):
            if not session.is_upload:
                continue
            # Identify UDP sessions by cid format "ip:port"
            if ":" not in cid or cid.isdigit():
                continue

            # Send ACK periodically
            if now - session.udp_last_ack_time > 0.005:
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
        """Start UDP download in a separate thread with dedicated socket."""
        session = self.file_manager.get_session(cid)
        if not session or session.is_upload:
            return

        def download_worker():
            # Create dedicated UDP socket for this download
            dl_sock = create_udp_socket()
            dl_sock.setblocking(False)
            # Bind to ephemeral port
            dl_sock.bind(('', 0))
            local_port = dl_sock.getsockname()[1]

            # Notify client of the download port via command
            notify = (_HDR.pack(0, PacketType.CMD.value) +
                      f"DOWNLOAD_PORT {local_port}\n".encode())
            try:
                self.server_socket_udp.sendto(notify, addr)
            except OSError:
                dl_sock.close()
                return

            # Wait briefly for client to be ready
            time.sleep(0.1)

            try:
                rudp = RUDPSocket(dl_sock, dest_addr=addr)
                rudp.send_stream(
                    session.file_handle,
                    total_size=session.total_size,
                    progress_callback=lambda s: self._update_session(cid, s),
                )
                bitrate = self.file_manager.calculate_bitrate(session)
                br_str = self.file_manager.format_bitrate(bitrate)
                print(f"[{_ts()}] UDP Download done: {session.filename} ({br_str})")
            except ConnectionLostError as e:
                print(f"[{_ts()}] UDP Download failed: {e}")
            except Exception as e:
                print(f"[{_ts()}] UDP Download error: {e}")
            finally:
                self.file_manager.complete_session(cid)
                dl_sock.close()

        t = threading.Thread(target=download_worker, daemon=True)
        t.start()
        self._udp_download_threads[cid] = t

    def _update_session(self, cid: str, transferred: int) -> None:
        session = self.file_manager.get_session(cid)
        if session:
            session.transferred = transferred
            session.last_activity = time.time()

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