"""
Сервер на МНОГОПОТОЧНОСТИ — по потоку на каждого TCP-клиента.
UDP обрабатывается в отдельном потоке.
Никакого select для клиентских сокетов.
"""

import os
import socket
import struct
import signal
import sys
import time
import threading
from datetime import datetime
from typing import Optional, Dict

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
from common.socket_utils import create_server_socket, send_all, create_udp_socket, recv_until
from server.command_handler import CommandHandler
from server.file_manager import FileManager, TransferSession

TCP_CHUNK = 64 * 1024
_HDR = struct.Struct("!IB")


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


class ThreadedServer:
    """Многопоточный сервер — по потоку на клиента."""

    def __init__(self, host: str = "0.0.0.0", port: int = 9001) -> None:
        self.host = host
        self.port = port
        self.running = False

        self.server_socket_tcp = create_server_socket(host, port)
        # TCP accept — блокирующий, в своём потоке
        self.server_socket_tcp.settimeout(1.0)

        self.server_socket_udp = create_udp_socket()
        self.server_socket_udp.bind((host, port))
        self.server_socket_udp.setblocking(False)

        self.file_manager = FileManager()
        self.command_handler = CommandHandler(self.file_manager, self.server_socket_udp)

        self.lock = threading.Lock()
        self.client_threads: list = []

    def start(self) -> None:
        self._setup_signals()
        self.running = True

        try:
            ip = socket.gethostbyname(socket.gethostname())
        except OSError:
            ip = "127.0.0.1"
        bind_ip = ip if self.host in ("0.0.0.0", "") else self.host
        print(f"[{_ts()}] THREADED Server started on {bind_ip}:{self.port}")
        print(f"[{_ts()}] Mode: one thread per client")

        # Поток для UDP
        udp_thread = threading.Thread(target=self._udp_loop, daemon=True)
        udp_thread.start()

        # Поток для периодической отправки ACK
        ack_thread = threading.Thread(target=self._ack_loop, daemon=True)
        ack_thread.start()

        # Основной поток — accept TCP
        self._accept_loop()

    def _setup_signals(self) -> None:
        signal.signal(signal.SIGINT, lambda *_: self._shutdown())
        signal.signal(signal.SIGTERM, lambda *_: self._shutdown())

    def _shutdown(self) -> None:
        print(f"\n[{_ts()}] Shutting down...")
        self.running = False
        try:
            self.server_socket_tcp.close()
        except OSError:
            pass
        try:
            self.server_socket_udp.close()
        except OSError:
            pass
        sys.exit(0)

    # ── TCP accept loop (main thread) ─────────────────────

    def _accept_loop(self) -> None:
        while self.running:
            try:
                cs, addr = self.server_socket_tcp.accept()
            except socket.timeout:
                continue
            except OSError:
                if self.running:
                    continue
                break
            print(f"[{_ts()}] TCP Connect: {addr[0]}:{addr[1]}")
            t = threading.Thread(
                target=self._handle_client,
                args=(cs, addr),
                daemon=True,
            )
            t.start()
            self.client_threads.append(t)
            # Чистим завершённые потоки
            self.client_threads = [t for t in self.client_threads if t.is_alive()]

    # ── per-client thread ─────────────────────────────────

    def _handle_client(self, sock: socket.socket, addr) -> None:
        cid = str(sock.fileno())
        sock.settimeout(300)  # 5 минут таймаут
        send_all(sock, b"220 Welcome\n")

        try:
            buf = b""
            while self.running:
                session = self.file_manager.get_session(cid)

                if session and session.is_upload:
                    # Режим приёма файла
                    self._receive_upload(sock, cid, session)
                    continue

                # Читаем команды
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    continue
                except OSError:
                    break
                if not data:
                    break

                buf += data
                while COMMAND_TERMINATOR in buf:
                    line, buf = buf.split(COMMAND_TERMINATOR, 1)
                    raw = line.decode("utf-8", errors="ignore")
                    cmd = parse_command(raw, "TCP")
                    print(f"[{_ts()}] TCP CMD [{cid}]: {raw.strip()}")

                    if cmd.type == CommandType.QUIT:
                        send_all(sock, format_response(Response(True, "Goodbye")))
                        return

                    resp = self.command_handler.execute(cmd, sock, None, None)

                    transfer_cmds = {
                        CommandType.UPLOAD,
                        CommandType.DOWNLOAD,
                        CommandType.RESUME_UPLOAD,
                        CommandType.RESUME_DOWNLOAD,
                    }

                    if cmd.type in transfer_cmds and resp.success:
                        # Для download — отправляем файл прямо в этом потоке
                        session = self.file_manager.get_session(cid)
                        if session and not session.is_upload:
                            self._send_download(sock, cid, session)
                        # Для upload — следующая итерация while попадёт в _receive_upload
                    else:
                        send_all(sock, format_response(resp))

        except Exception as e:
            print(f"[{_ts()}] Client {addr} error: {e}")
        finally:
            print(f"[{_ts()}] Disconnected: {addr[0]}:{addr[1]}")
            self.file_manager.close_session(cid)
            try:
                sock.close()
            except OSError:
                pass

    def _receive_upload(self, sock, cid, session):
        """Приём файла — блокирующий, в потоке клиента."""
        try:
            while session.transferred < session.total_size:
                remaining = session.total_size - session.transferred
                chunk = sock.recv(min(TCP_CHUNK, remaining))
                if not chunk:
                    break
                session.file_handle.write(chunk)
                session.transferred += len(chunk)
                session.last_activity = time.time()
                self._log(cid, session, "TCP Upload")
        except (OSError, IOError) as e:
            print(f"[{_ts()}] Upload error [{cid}]: {e}")
            self.file_manager.close_session(cid)
            return

        if session.transferred >= session.total_size:
            br = self.file_manager.calculate_bitrate(session)
            bs = self.file_manager.format_bitrate(br)
            self.file_manager.complete_session(cid)
            send_all(sock, format_response(
                Response(True, f"Received {session.transferred} bytes. {bs}")
            ))
            print(f"[{_ts()}] TCP Upload done ({bs})")
        else:
            print(f"[{_ts()}] TCP Upload incomplete [{cid}]")
            self.file_manager.close_session(cid)

    def _send_download(self, sock, cid, session):
        """Отправка файла — блокирующий, в потоке клиента."""
        try:
            while session.transferred < session.total_size:
                chunk = session.file_handle.read(TCP_CHUNK)
                if not chunk:
                    break
                sock.sendall(chunk)
                session.transferred += len(chunk)
                session.last_activity = time.time()
                self._log(cid, session, "TCP Download")
        except (OSError, IOError, BrokenPipeError) as e:
            print(f"[{_ts()}] Download error [{cid}]: {e}")
            self.file_manager.close_session(cid)
            return

        br = self.file_manager.calculate_bitrate(session)
        bs = self.file_manager.format_bitrate(br)
        self.file_manager.complete_session(cid)
        print(f"[{_ts()}] TCP Download done ({bs})")

    # ── UDP loop (separate thread) ────────────────────────

    def _udp_loop(self) -> None:
        import select as sel
        while self.running:
            try:
                r, _, _ = sel.select([self.server_socket_udp], [], [], 0.1)
                if not r:
                    continue
                for _ in range(16384):
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
            except Exception as e:
                if self.running:
                    print(f"[{_ts()}] UDP loop error: {e}")

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

        with self.lock:
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
            else:
                if cmd.type in (CommandType.DOWNLOAD, CommandType.RESUME_DOWNLOAD):
                    # Запускаем download в отдельном потоке
                    t = threading.Thread(
                        target=self._udp_download_thread,
                        args=(cid, addr),
                        daemon=True,
                    )
                    t.start()
                elif cmd.type in (CommandType.UPLOAD, CommandType.RESUME_UPLOAD):
                    if cmd.type == CommandType.UPLOAD:
                        filename = cmd.args[0]
                        size = int(cmd.args[1])
                        offset = 0
                    else:
                        filename = cmd.args[0]
                        offset = int(cmd.args[1])
                        size = int(cmd.args[2])
                    t = threading.Thread(
                        target=self._udp_upload_thread,
                        args=(cid, addr, filename, size, offset),
                        daemon=True,
                    )
                    t.start()
        else:
            rp = _HDR.pack(0, PacketType.CMD.value) + format_response(resp)
            try:
                self.server_socket_udp.sendto(rp, addr)
            except OSError:
                pass

    def _udp_upload_thread(self, cid, addr, filename, total_size, offset=0):
        """Upload через UDP → TCP в отдельном потоке."""
        session = self.file_manager.get_session(cid)
        if not session or not session.is_upload:
            return

        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_sock.bind(("", 0))
        tcp_port = tcp_sock.getsockname()[1]
        tcp_sock.listen(1)
        tcp_sock.settimeout(30.0)

        print(f"[{_ts()}] UDP Upload (thread): {filename} ← {addr[0]}:{addr[1]} port {tcp_port}")

        # Отправляем порт клиенту
        port_pkt = _HDR.pack(0, PacketType.CMD.value) + f"UPLOAD_PORT {tcp_port}".encode()
        for _ in range(30):
            try:
                self.server_socket_udp.sendto(port_pkt, addr)
            except OSError:
                pass
            time.sleep(0.1)

        try:
            client_conn, client_addr = tcp_sock.accept()
            print(f"[{_ts()}] Upload thread: got connection from {client_addr}")
        except (socket.timeout, OSError):
            print(f"[{_ts()}] Upload thread: no connection")
            self.file_manager.close_session(cid)
            tcp_sock.close()
            return

        try:
            received = 0
            while received < total_size:
                chunk = client_conn.recv(65536)
                if not chunk:
                    break
                session.file_handle.write(chunk)
                received += len(chunk)
                session.transferred = offset + received
                self._log(cid, session, "UDP Upload (thread)")

            if received >= total_size:
                br = self.file_manager.calculate_bitrate(session)
                bs = self.file_manager.format_bitrate(br)
                self.file_manager.complete_session(cid)
                try:
                    client_conn.send(b"OK\n")
                except Exception:
                    pass
                print(f"[{_ts()}] UDP Upload done (thread): {filename} ({bs})")
            else:
                print(f"[{_ts()}] UDP Upload incomplete: {received}/{total_size}")
                self.file_manager.close_session(cid)
        except Exception as e:
            print(f"[{_ts()}] Upload thread error: {e}")
            self.file_manager.close_session(cid)
        finally:
            client_conn.close()
            tcp_sock.close()

    def _udp_download_thread(self, cid, addr):
        """Download через UDP → TCP в отдельном потоке."""
        session = self.file_manager.get_session(cid)
        if not session or session.is_upload:
            return

        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_sock.bind(("", 0))
        tcp_port = tcp_sock.getsockname()[1]
        tcp_sock.listen(1)
        tcp_sock.settimeout(30.0)

        print(f"[{_ts()}] UDP Download (thread): {session.filename} → {addr[0]}:{addr[1]} port {tcp_port}")

        port_pkt = _HDR.pack(0, PacketType.CMD.value) + f"DOWNLOAD_PORT {tcp_port}".encode()
        for _ in range(30):
            try:
                self.server_socket_udp.sendto(port_pkt, addr)
            except OSError:
                pass
            time.sleep(0.1)

        try:
            client_conn, client_addr = tcp_sock.accept()
            print(f"[{_ts()}] Download thread: got connection from {client_addr}")
        except (socket.timeout, OSError):
            print(f"[{_ts()}] Download thread: no connection")
            self.file_manager.close_session(cid)
            tcp_sock.close()
            return

        try:
            sent = 0
            total = session.total_size
            while sent < total:
                chunk = session.file_handle.read(65536)
                if not chunk:
                    break
                client_conn.sendall(chunk)
                sent += len(chunk)
                session.transferred = sent
                self._log(cid, session, "UDP Download (thread)")

            br = self.file_manager.calculate_bitrate(session)
            bs = self.file_manager.format_bitrate(br)
            print(f"[{_ts()}] UDP Download done (thread): {session.filename} ({bs})")
        except Exception as e:
            print(f"[{_ts()}] Download thread error: {e}")
        finally:
            self.file_manager.complete_session(cid)
            client_conn.close()
            tcp_sock.close()

    def _udp_data(self, cid, seq, data, addr) -> None:
        with self.lock:
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
                session.udp_recv_buffer[seq] = data
            nack = _HDR.pack(session.expected_seq, PacketType.NACK.value)
            try:
                self.server_socket_udp.sendto(nack, addr)
            except OSError:
                pass

    def _udp_fin(self, cid, seq, addr) -> None:
        fin_ack = _HDR.pack(seq + 1, PacketType.ACK.value)
        for _ in range(10):
            try:
                self.server_socket_udp.sendto(fin_ack, addr)
            except OSError:
                pass
            time.sleep(0.01)
        session = self.file_manager.get_session(cid)
        if session and session.is_upload:
            br = self.file_manager.calculate_bitrate(session)
            bs = self.file_manager.format_bitrate(br)
            self.file_manager.complete_session(cid)
            print(f"[{_ts()}] UDP Upload done: {session.filename} ({bs})")

    def _ack_loop(self) -> None:
        while self.running:
            time.sleep(0.5)
            now = time.time()
            with self.lock:
                sessions = list(self.file_manager.sessions.items())
            for cid, sess in sessions:
                if not sess.is_upload or ":" not in cid or cid.isdigit():
                    continue
                if now - sess.udp_last_ack_time < UDP_ACK_INTERVAL * 0.1:
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

    def _log(self, cid, session, op) -> None:
        if session.total_size <= 0:
            return
        pct = int(session.transferred / session.total_size * 100)
        if pct >= getattr(session, "_last_pct", -10) + 10 or pct == 100:
            session._last_pct = pct
            print(f"[{_ts()}] {op}: {session.filename} [{cid}] — {pct}%")


def main(host: str = "0.0.0.0", port: int = 9001) -> None:
    server = ThreadedServer(host, port)
    server.start()