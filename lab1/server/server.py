"""TCP/UDP сервер с поддержкой пула процессов (ЛР4, вариант 12).

Вариант 12:
- протокол TCP,
- пул процессов (os.fork),
- механизм защиты: параллельный вызов — несколько процессов
  параллельно делают accept на одном слушающем сокете.
"""

import os
import socket
import select
import signal
import struct
import sys
import time
import threading
from datetime import datetime
from typing import Optional, Dict, List

from common.protocol import (
    CommandType,
    PacketType,
    parse_command,
    format_response,
    COMMAND_TERMINATOR,
    UDP_WINDOW_SIZE,
    Response,
    UDP_ACK_INTERVAL,
)
from common.socket_utils import create_server_socket, send_all, create_udp_socket
from common.rudp import RUDPSocket, ConnectionLostError
from server.command_handler import CommandHandler
from server.file_manager import FileManager, TransferSession

TCP_CHUNK = 64 * 1024
_HDR = struct.Struct("!IB")

WORKER_MIN = 3
WORKER_MAX = 5


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


class TCPServer:
    """Логика сервера — запускается внутри каждого рабочего процесса."""

    def __init__(
        self,
        tcp_sock: socket.socket,
        udp_sock: socket.socket,
        host: str = "0.0.0.0",
        port: int = 9000,
    ) -> None:
        self.host = host
        self.port = port
        self.running = False

        self.server_socket_tcp: socket.socket = tcp_sock
        self.server_socket_udp: socket.socket = udp_sock

        self.file_manager = FileManager()
        self.command_handler = CommandHandler(self.file_manager)

        self.inputs: List[socket.socket] = [self.server_socket_tcp, self.server_socket_udp]
        self.outputs: List[socket.socket] = []
        self.tcp_buffers: Dict[int, bytes] = {}

    # ── lifecycle ──────────────────────────────────────────

    def start(self) -> None:
        self._setup_signals()
        self.running = True

        try:
            ip = socket.gethostbyname(socket.gethostname())
        except OSError:
            ip = "127.0.0.1"
        bind_ip = ip if self.host in ("0.0.0.0", "") else self.host
        print(f"[{_ts()}] [PID {os.getpid()}] Worker started on {bind_ip}:{self.port}")
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
                    if not s.is_upload and s.sock is not None and s.sock in self.inputs
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
                    print(f"[{_ts()}] [PID {os.getpid()}] Loop error: {e}")

    # ── TCP ───────────────────────────────────────────────

    def _tcp_accept(self) -> None:
        try:
            cs, addr = self.server_socket_tcp.accept()
        except OSError:
            return
        print(f"[{_ts()}] [PID {os.getpid()}] TCP Connect: {addr[0]}:{addr[1]}")
        cs.setblocking(False)
        self.inputs.append(cs)
        self.tcp_buffers[cs.fileno()] = b""
        send_all(cs, b"220 Welcome\n")

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
                br = self.file_manager.calculate_bitrate(session)
                bs = self.file_manager.format_bitrate(br)
                self.file_manager.complete_session(cid)
                send_all(
                    sock,
                    format_response(
                        Response(True, f"Received {session.transferred} bytes. {bs}")
                    ),
                )
                print(f"[{_ts()}] TCP Upload done ({bs})")
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
            transfer_cmds = {
                CommandType.UPLOAD,
                CommandType.DOWNLOAD,
                CommandType.RESUME_UPLOAD,
                CommandType.RESUME_DOWNLOAD,
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
                    session.file_handle.seek(-(len(chunk) - sent), 1)
                session.transferred += sent
            except OSError:
                self._remove(sock)
                return
            session.last_activity = time.time()
            self._log(cid, session, "TCP Download")
        else:
            br = self.file_manager.calculate_bitrate(session)
            bs = self.file_manager.format_bitrate(br)
            self.file_manager.complete_session(cid)
            print(f"[{_ts()}] TCP Download done ({bs})")

    # ── UDP ───────────────────────────────────────────────

    def _udp_read(self) -> None:
        if not self.server_socket_udp:
            return
        for _ in range(8192):
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
                self._udp_cmd(cid, seq, data, addr)
            elif ptype == PacketType.DATA.value:
                self._udp_data(cid, seq, data, addr)
            elif ptype == PacketType.FIN.value:
                self._udp_fin(cid, seq, addr)

    def _udp_cmd(self, cid, seq, data, addr) -> None:
        ack = _HDR.pack(seq, PacketType.CMD.value) + b"ACK_CMD"
        try:
            self.server_socket_udp.sendto(ack, addr)
        except OSError:
            pass

        msg = data.decode(errors="ignore")
        if msg.startswith("OK") or msg.startswith("ERROR") or msg == "ACK_CMD":
            return

        cmd = parse_command(msg, "UDP")
        print(f"[{_ts()}] UDP CMD [{cid}]: {msg.strip()}")
        resp = self.command_handler.execute(cmd, None, self.server_socket_udp, addr)

        transfer_cmds = {
            CommandType.UPLOAD,
            CommandType.DOWNLOAD,
            CommandType.RESUME_UPLOAD,
            CommandType.RESUME_DOWNLOAD,
        }

        if cmd.type in transfer_cmds:
            if not resp.success:
                ep = _HDR.pack(0, PacketType.CMD.value) + format_response(resp)
                try:
                    self.server_socket_udp.sendto(ep, addr)
                except OSError:
                    pass
            elif cmd.type in (CommandType.DOWNLOAD, CommandType.RESUME_DOWNLOAD):
                self._start_udp_download(cid, addr)
        else:
            rp = _HDR.pack(0, PacketType.CMD.value) + format_response(resp)
            try:
                self.server_socket_udp.sendto(rp, addr)
            except OSError:
                pass

    def _udp_data(self, cid, seq, data, addr) -> None:
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
            if seq < session.expected_seq + UDP_WINDOW_SIZE * 2:
                session.udp_recv_buffer.setdefault(seq, data)
            nack = _HDR.pack(session.expected_seq, PacketType.NACK.value)
            try:
                self.server_socket_udp.sendto(nack, addr)
            except OSError:
                pass

    def _udp_fin(self, cid, seq, addr) -> None:
        fin_ack = _HDR.pack(seq + 1, PacketType.ACK.value)
        for _ in range(5):
            try:
                self.server_socket_udp.sendto(fin_ack, addr)
            except OSError:
                pass
        session = self.file_manager.get_session(cid)
        if session and session.is_upload:
            br = self.file_manager.calculate_bitrate(session)
            bs = self.file_manager.format_bitrate(br)
            self.file_manager.complete_session(cid)
            print(f"[{_ts()}] UDP Upload done: {session.filename} ({bs})")

    def _udp_send_acks(self) -> None:
        now = time.time()
        for cid, sess in list(self.file_manager.sessions.items()):
            if not sess.is_upload or ":" not in cid or cid.isdigit():
                continue
            if now - sess.udp_last_ack_time < UDP_ACK_INTERVAL:
                continue
            parts = cid.rsplit(":", 1)
            if len(parts) != 2:
                continue
            try:
                addr = (parts[0], int(parts[1]))
            except ValueError:
                continue
            ack = _HDR.pack(sess.expected_seq, PacketType.ACK.value)
            try:
                self.server_socket_udp.sendto(ack, addr)
            except OSError:
                pass
            sess.udp_last_ack_time = now

    def _start_udp_download(self, cid, addr) -> None:
        session = self.file_manager.get_session(cid)
        if not session or session.is_upload:
            return

        session.udp_download_active = True
        session.udp_client_addr = addr
        file_handle = session.file_handle
        total_size = session.total_size
        filename = session.filename

        def worker():
            dl_sock = create_udp_socket()
            dl_sock.setblocking(False)
            dl_sock.bind(("", 0))
            dl_port = dl_sock.getsockname()[1]

            print(f"[{_ts()}] UDP Download: {filename} → {addr[0]}:{addr[1]} port {dl_port}")

            port_pkt = (
                _HDR.pack(0, PacketType.CMD.value) + f"DOWNLOAD_PORT {dl_port}".encode()
            )
            for _ in range(15):
                try:
                    self.server_socket_udp.sendto(port_pkt, addr)
                except OSError:
                    pass
                time.sleep(0.02)

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

            print(f"[{_ts()}] UDP Download: streaming to {client_dl_addr}")

            try:
                rudp = RUDPSocket(dl_sock, dest_addr=client_dl_addr)
                rudp.send_stream(
                    file_handle,
                    total_size=total_size,
                    progress_callback=lambda s: self._update_dl(cid, s, session),
                )
                session.transferred = total_size
                br = self.file_manager.calculate_bitrate(session)
                bs = self.file_manager.format_bitrate(br)
                print(f"[{_ts()}] UDP Download done: {filename} ({bs})")
            except ConnectionLostError as e:
                print(f"[{_ts()}] UDP Download failed: {e}")
            except Exception as e:
                print(f"[{_ts()}] UDP Download error: {e}")
            finally:
                self.file_manager.complete_session(cid)
                dl_sock.close()

        threading.Thread(target=worker, daemon=True).start()

    def _update_dl(self, cid, transferred, session) -> None:
        session.transferred = transferred
        session.last_activity = time.time()
        self._log(cid, session, "UDP Download")

    def _log(self, cid, session, op) -> None:
        if session.total_size <= 0:
            return
        pct = int(session.transferred / session.total_size * 100)
        if pct >= getattr(session, "_last_pct", -10) + 10 or pct == 100:
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


# ── process-pool manager (os.fork, вариант 12) ────────────


def main() -> None:
    host = "0.0.0.0"
    port = 9000

    # master создаёт сокеты до fork — дети наследуют их автоматически
    tcp_sock = create_server_socket(host, port)
    tcp_sock.setblocking(False)

    udp_sock = create_udp_socket()
    udp_sock.bind((host, port))
    udp_sock.setblocking(False)

    try:
        ip = socket.gethostbyname(socket.gethostname())
    except OSError:
        ip = "127.0.0.1"
    bind_ip = ip if host in ("0.0.0.0", "") else host
    print(f"[{_ts()}] ===== Master PID={os.getpid()} on {bind_ip}:{port} =====")
    print(f"[{_ts()}] Pool: WORKER_MIN={WORKER_MIN}, WORKER_MAX={WORKER_MAX}")

    worker_pids: List[int] = []

    def spawn_worker() -> None:
        pid = os.fork()
        if pid == 0:
            # дочерний процесс — запускаем воркер
            srv = TCPServer(tcp_sock, udp_sock, host=host, port=port)
            srv.start()
            sys.exit(0)
        # мастер
        worker_pids.append(pid)
        print(f"[{_ts()}] Spawned worker PID={pid}")

    def handle_term(signum, frame) -> None:
        print(f"[{_ts()}] Master received signal {signum}, stopping...")
        for pid in list(worker_pids):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for pid in list(worker_pids):
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
        try:
            tcp_sock.close()
            udp_sock.close()
        except OSError:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_term)
    signal.signal(signal.SIGTERM, handle_term)
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)

    for _ in range(WORKER_MIN):
        spawn_worker()

    # менеджер пула
    try:
        while True:
            # собираем завершившихся воркеров
            while True:
                try:
                    pid, status = os.waitpid(-1, os.WNOHANG)
                    if pid == 0:
                        break
                    exit_code = os.waitstatus_to_exitcode(status)
                    print(f"[{_ts()}] Worker PID={pid} exited with {exit_code}")
                    if pid in worker_pids:
                        worker_pids.remove(pid)
                except ChildProcessError:
                    break

            # доливаем до WORKER_MIN
            while len(worker_pids) < WORKER_MIN:
                spawn_worker()

            # обрезаем до WORKER_MAX
            while len(worker_pids) > WORKER_MAX:
                extra_pid = worker_pids.pop()
                try:
                    os.kill(extra_pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass

            time.sleep(1.0)
    finally:
        handle_term(signal.SIGTERM, None)


if __name__ == "__main__":
    main()
