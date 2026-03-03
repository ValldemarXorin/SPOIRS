"""
TCP/UDP сервер.

Архитектура:
  • Главный поток: select-loop для TCP + приём UDP-пакетов (upload/CMD).
  • Для каждого UDP download (сервер → клиент) запускается отдельный
    daemon-поток — он пишет напрямую в сокет без select-overhead.
    Это полностью убирает задержку в 1–5 мс на итерацию event loop.
"""

import socket
import select
import signal
import sys
import time
import threading
from datetime import datetime
from typing import Optional, Dict

from common.protocol import (
    Command, CommandType, parse_command,
    format_response, COMMAND_TERMINATOR, Response,
    PacketType, UDP_PAYLOAD_SIZE, UDP_WINDOW_SIZE, UDP_TIMEOUT,
)
from common.socket_utils import create_server_socket, send_all
from common.rudp import RUDPSocket
from server.command_handler import CommandHandler
from server.file_manager import FileManager

TCP_CHUNK        = 64 * 1024
UDP_READ_BATCH   = 1024
UDP_UPLOAD_WIN   = 4096
UDP_ACK_EVERY    = 4
UDP_FIN_TRIES    = 30
UDP_FIN_INTERVAL = 0.1
_SEND_BURST      = 512


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _would_block(e: BaseException) -> bool:
    return isinstance(e, BlockingIOError) or getattr(e, "errno", None) in (11, 10035)


# ── UDP download worker (поток) ────────────────────────────

def _udp_download_worker(
    udp_sock: socket.socket,
    addr: tuple,
    session,
    file_manager: "FileManager",
    client_id: str,
) -> None:
    """
    Отдельный поток для UDP download.
    Использует тот же алгоритм что и RUDPSocket.send_stream,
    но читает данные из уже открытого file_handle сессии.
    """
    from common.rudp import RUDPSocket, _SEND_BURST
    from common.protocol import PacketType, UDP_WINDOW_SIZE, UDP_TIMEOUT, UDP_PAYLOAD_SIZE
    import struct, select, time

    def _pack(seq, pt, data):
        return struct.pack("!IB", seq, pt) + data

    def _unpack(pkt):
        if len(pkt) < 5:
            return -1, -1, b""
        s, t = struct.unpack("!IB", pkt[:5])
        return s, t, pkt[5:]

    def _sendto(data):
        for _ in range(20):
            try:
                udp_sock.sendto(data, addr)
                return True
            except BlockingIOError:
                time.sleep(0.0001)
            except OSError as e:
                if getattr(e, "errno", None) in (11, 10035):
                    time.sleep(0.0001)
                    continue
                return False
        return False

    def _drain_acks(base, packets, next_seq):
        last_t = -1.0
        while True:
            r, _, _ = select.select([udp_sock], [], [], 0)
            if not r:
                break
            try:
                pkt, a = udp_sock.recvfrom(256)
            except (BlockingIOError, OSError):
                break
            if a != addr:
                # положим обратно нельзя — просто пропускаем
                continue
            s, pt, _ = _unpack(pkt)
            if pt == PacketType.ACK.value and s > base:
                ack = min(s, next_seq)
                for i in range(base, ack):
                    packets.pop(i, None)
                base   = ack
                last_t = time.monotonic()
        return base, last_t

    total_size = session.total_size
    fh         = session.file_handle
    base       = 0
    next_seq   = 0
    packets: Dict[int, bytes] = {}
    cursor     = 0
    last_ack_t = time.monotonic()
    eof        = False

    try:
        while cursor < total_size or base < next_seq:
            # 1. отправляем burst
            sent_now = 0
            while (not eof
                   and next_seq < base + UDP_WINDOW_SIZE
                   and sent_now < _SEND_BURST):
                to_read = min(UDP_PAYLOAD_SIZE, total_size - cursor)
                if to_read <= 0:
                    eof = True
                    break
                try:
                    chunk = fh.read(to_read)
                except Exception:
                    chunk = b""
                if not chunk:
                    eof = True
                    break
                pkt = _pack(next_seq, PacketType.DATA.value, chunk)
                packets[next_seq] = pkt
                _sendto(pkt)
                next_seq += 1
                cursor   += len(chunk)
                sent_now += 1

            session.transferred = cursor

            # 2. drain ACK
            new_base, t = _drain_acks(base, packets, next_seq)
            if new_base > base:
                base      = new_base
                last_ack_t = t if t > 0 else time.monotonic()

            # 3. окно заполнено → ждём ACK
            if next_seq >= base + UDP_WINDOW_SIZE and base < next_seq:
                r, _, _ = select.select([udp_sock], [], [], 0.001)
                if r:
                    new_base, t = _drain_acks(base, packets, next_seq)
                    if new_base > base:
                        base      = new_base
                        last_ack_t = t if t > 0 else time.monotonic()

            # 4. retransmit
            now = time.monotonic()
            if now - last_ack_t > UDP_TIMEOUT and packets:
                resent = 0
                for s in range(base, next_seq):
                    p = packets.get(s)
                    if p:
                        _sendto(p)
                        resent += 1
                        if resent >= _SEND_BURST:
                            break
                last_ack_t = now

        # FIN
        fin_seq = next_seq
        fin_pkt = _pack(fin_seq, PacketType.FIN.value, b"")
        for _ in range(UDP_FIN_TRIES):
            _sendto(fin_pkt)
            r, _, _ = select.select([udp_sock], [], [], UDP_FIN_INTERVAL)
            if not r:
                continue
            try:
                ap, a = udp_sock.recvfrom(256)
            except (BlockingIOError, OSError):
                continue
            if a != addr:
                continue
            aseq, apt, _ = _unpack(ap)
            if apt == PacketType.ACK.value and aseq == fin_seq + 1:
                break

        bitrate = file_manager.calculate_bitrate(session)
        print(f"[{_ts()}] UDP Download finished: {session.filename} "
              f"[{client_id}] ({file_manager.format_bitrate(bitrate)})")

    except Exception as exc:
        print(f"[{_ts()}] UDP Download error [{client_id}]: {exc}")
    finally:
        file_manager.complete_session(client_id)


# ── Server ─────────────────────────────────────────────────

class TCPServer:

    def __init__(self, host: str = "0.0.0.0", port: int = 9000):
        self.host    = host
        self.port    = port
        self.running = False

        self.server_socket_tcp: Optional[socket.socket] = None
        self.server_socket_udp: Optional[socket.socket] = None

        self.file_manager    = FileManager()
        self.command_handler = CommandHandler(self.file_manager)

        self.inputs:      list             = []
        self.outputs:     list             = []
        self.tcp_buffers: Dict[int, bytes] = {}
        self._rudp:       Optional[RUDPSocket] = None

        # download-сессии, для которых уже запущен поток
        self._udp_dl_threads: Dict[str, threading.Thread] = {}

    def start(self) -> None:
        self._setup_signals()

        self.server_socket_tcp = create_server_socket(self.host, self.port)
        self.server_socket_tcp.setblocking(False)
        self.inputs.append(self.server_socket_tcp)

        self.server_socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try:
                self.server_socket_udp.setsockopt(socket.SOL_SOCKET, opt,
                                                   64 * 1024 * 1024)
            except Exception:
                pass
        self.server_socket_udp.bind((self.host, self.port))
        self.server_socket_udp.setblocking(False)
        self.inputs.append(self.server_socket_udp)

        self._rudp   = RUDPSocket(self.server_socket_udp)
        self.running = True

        try:
            ip = socket.gethostbyname(socket.gethostname())
        except socket.error:
            ip = "127.0.0.1"
        host_str = ip if self.host in ("0.0.0.0", "") else self.host
        print(f"[{_ts()}] ===== Server on {host_str}:{self.port} =====")
        self._loop()

    def _setup_signals(self):
        signal.signal(signal.SIGINT,  lambda s, f: (self.stop(), sys.exit(0)))
        signal.signal(signal.SIGTERM, lambda s, f: (self.stop(), sys.exit(0)))

    def stop(self):
        self.running = False
        for s in self.inputs:
            try:
                s.close()
            except Exception:
                pass

    # ── main loop ──────────────────────────────────────────

    def _loop(self) -> None:
        while self.running:
            try:
                self.outputs = [
                    sess.sock
                    for sess in self.file_manager.sessions.values()
                    if not sess.is_upload and sess.sock and sess.sock in self.inputs
                ]

                readable, writable, exceptional = select.select(
                    self.inputs, self.outputs, self.inputs, 0.001
                )

                for s in readable:
                    if s is self.server_socket_tcp:
                        self._tcp_accept()
                    elif s is self.server_socket_udp:
                        self._udp_read_batch()
                    else:
                        self._tcp_read(s)

                for s in writable:
                    self._tcp_write(s)

                for s in exceptional:
                    self._remove(s)

                # запускаем download-потоки для новых UDP сессий
                self._spawn_udp_dl_threads()

            except Exception:
                pass

    def _spawn_udp_dl_threads(self):
        """Запускает поток для каждой UDP download-сессии без активного потока."""
        for cid, sess in list(self.file_manager.sessions.items()):
            if sess.is_upload or sess.sock is not None:
                continue
            if cid in self._udp_dl_threads:
                # чистим завершённые потоки
                if not self._udp_dl_threads[cid].is_alive():
                    del self._udp_dl_threads[cid]
                continue
            # новая сессия — запускаем поток
            try:
                host, port_s = cid.split(":", 1)
                addr = (host, int(port_s))
            except Exception:
                self.file_manager.close_session(cid)
                continue

            t = threading.Thread(
                target=_udp_download_worker,
                args=(self.server_socket_udp, addr, sess,
                      self.file_manager, cid),
                daemon=True,
            )
            self._udp_dl_threads[cid] = t
            t.start()
            print(f"[{_ts()}] UDP Download started: {sess.filename} [{cid}]")

    # ── TCP accept ─────────────────────────────────────────

    def _tcp_accept(self):
        try:
            cs, ca = self.server_socket_tcp.accept()
            print(f"[{_ts()}] TCP Connect: {ca[0]}:{ca[1]}")
            cs.setblocking(False)
            self.inputs.append(cs)
            self.tcp_buffers[cs.fileno()] = b""
            send_all(cs, b"220 Welcome\n")
        except Exception:
            pass

    # ── TCP read ───────────────────────────────────────────

    def _tcp_read(self, sock: socket.socket):
        cid     = str(sock.fileno())
        session = self.file_manager.get_session(cid)

        if session and session.is_upload:
            try:
                chunk = sock.recv(TCP_CHUNK)
                if not chunk:
                    self._remove(sock)
                    return
                session.file_handle.write(chunk)
                session.transferred += len(chunk)
                self._log(cid, session, "TCP Upload")
                if session.transferred >= session.total_size:
                    br  = self.file_manager.calculate_bitrate(session)
                    msg = (f"Received {session.transferred} bytes. "
                           f"{self.file_manager.format_bitrate(br)}")
                    self.file_manager.complete_session(cid)
                    send_all(sock, format_response(Response(True, msg)))
                    print(f"[{_ts()}] TCP Upload finished: {session.filename} "
                          f"({self.file_manager.format_bitrate(br)})")
            except socket.error:
                self._remove(sock)
            return

        try:
            data = sock.recv(4096)
            if not data:
                self._remove(sock)
                return
            buf = self.tcp_buffers.get(sock.fileno(), b"") + data
            if COMMAND_TERMINATOR in buf:
                line, rest = buf.split(COMMAND_TERMINATOR, 1)
                self.tcp_buffers[sock.fileno()] = rest
                cmd_str  = line.decode("utf-8", errors="ignore")
                command  = parse_command(cmd_str, "TCP")
                print(f"[{_ts()}] TCP CMD [{cid}]: {cmd_str.strip()}")
                response = self.command_handler.execute(command, sock, None, None)
                transfer = (CommandType.UPLOAD, CommandType.DOWNLOAD,
                            CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD)
                if command.type not in transfer or not response.success:
                    send_all(sock, format_response(response))
            else:
                self.tcp_buffers[sock.fileno()] = buf
        except socket.error:
            self._remove(sock)

    # ── TCP write (download) ───────────────────────────────

    def _tcp_write(self, sock: socket.socket):
        cid     = str(sock.fileno())
        session = self.file_manager.get_session(cid)
        if session and not session.is_upload and session.file_handle:
            try:
                chunk = session.file_handle.read(TCP_CHUNK)
                if chunk:
                    sock.send(chunk)
                    session.transferred += len(chunk)
                    self._log(cid, session, "TCP Download")
                else:
                    br = self.file_manager.calculate_bitrate(session)
                    self.file_manager.complete_session(cid)
                    print(f"[{_ts()}] TCP Download finished: {session.filename} "
                          f"({self.file_manager.format_bitrate(br)})")
            except socket.error:
                self._remove(sock)

    # ── UDP read batch (upload + ACK от download-потоков) ──

    def _udp_read_batch(self):
        if not self.server_socket_udp or not self._rudp:
            return
        rudp = self._rudp

        for _ in range(UDP_READ_BATCH):
            try:
                pkt, addr = self.server_socket_udp.recvfrom(65536)
            except (BlockingIOError, socket.error):
                break

            cid              = f"{addr[0]}:{addr[1]}"
            seq, p_type, data = rudp._unpack(pkt)

            # CMD
            if p_type == PacketType.CMD.value:
                ack = rudp._pack(seq, PacketType.CMD.value, b"ACK_CMD")
                self._udp_send(ack, addr)
                msg = data.decode(errors="ignore")
                if msg.startswith("OK") or msg.startswith("ERROR"):
                    continue
                command  = parse_command(msg, "UDP")
                print(f"[{_ts()}] UDP CMD [{cid}]: {msg.strip()}")
                response = self.command_handler.execute(
                    command, None, self.server_socket_udp, addr
                )
                transfer = (CommandType.UPLOAD, CommandType.DOWNLOAD,
                            CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD)
                if command.type in transfer and not response.success:
                    self._udp_send(
                        rudp._pack(0, PacketType.CMD.value, format_response(response)),
                        addr,
                    )

            # DATA (upload)
            elif p_type == PacketType.DATA.value:
                sess = self.file_manager.get_session(cid)
                if not sess or not sess.is_upload:
                    continue

                if seq == sess.expected_seq:
                    sess.file_handle.write(data)
                    sess.transferred += len(data)
                    sess.expected_seq += 1
                    while sess.expected_seq in sess.udp_recv_buffer:
                        d = sess.udp_recv_buffer.pop(sess.expected_seq)
                        sess.file_handle.write(d)
                        sess.transferred += len(d)
                        sess.expected_seq += 1
                    self._log(cid, sess, "UDP Upload")
                elif seq > sess.expected_seq:
                    if seq < sess.expected_seq + UDP_UPLOAD_WIN:
                        sess.udp_recv_buffer.setdefault(seq, data)

                # ACK каждые UDP_ACK_EVERY пакетов
                if sess.expected_seq % UDP_ACK_EVERY == 0:
                    self._udp_send(
                        rudp._pack(sess.expected_seq, PacketType.ACK.value, b""),
                        addr,
                    )

            # FIN (конец upload)
            elif p_type == PacketType.FIN.value:
                sess = self.file_manager.get_session(cid)
                if sess and sess.is_upload:
                    ack = rudp._pack(seq + 1, PacketType.ACK.value, b"")
                    for _ in range(3):
                        self._udp_send(ack, addr)
                    br = self.file_manager.calculate_bitrate(sess)
                    print(f"[{_ts()}] UDP Upload finished: {sess.filename} "
                          f"[{cid}] ({self.file_manager.format_bitrate(br)})")
                    self.file_manager.complete_session(cid)

            # ACK от download-потока обрабатывается внутри потока,
            # но select() на одном сокете может вытащить пакет сюда.
            # Просто игнорируем — поток сам drain-ит ACK.

    def _udp_send(self, data: bytes, addr: tuple) -> bool:
        if not self.server_socket_udp:
            return False
        try:
            self.server_socket_udp.sendto(data, addr)
            return True
        except (BlockingIOError, OSError) as e:
            return _would_block(e)

    # ── logging ────────────────────────────────────────────

    def _log(self, cid, sess, op: str):
        if sess.total_size <= 0:
            return
        pct  = int(sess.transferred / sess.total_size * 100)
        last = getattr(sess, "_lpct", -10)
        if pct >= last + 10:
            sess._lpct = (pct // 10) * 10
            print(f"[{_ts()}] {op}: {sess.filename} [{cid}] "
                  f"— {pct}% ({sess.transferred}/{sess.total_size})")

    # ── remove client ──────────────────────────────────────

    def _remove(self, sock: socket.socket):
        try:
            a = sock.getpeername()
            print(f"[{_ts()}] Disconnected: {a[0]}:{a[1]}")
        except Exception:
            print(f"[{_ts()}] Disconnected fd={sock.fileno()}")
        if sock in self.inputs:
            self.inputs.remove(sock)
        if sock in self.outputs:
            self.outputs.remove(sock)
        self.tcp_buffers.pop(sock.fileno(), None)
        self.file_manager.close_session(str(sock.fileno()))
        try:
            sock.close()
        except Exception:
            pass
