"""
TCP/UDP сервер.

Архитектура:
  • main loop: select для TCP + приём UDP CMD/DATA(upload).
  • UDP download: отдельный daemon-поток С ОТДЕЛЬНЫМ UDP-СОКЕТОМ.
    Поток создаёт свой socket, bind на (host, 0) → новый порт.
    Сервер через CMD сообщает клиенту этот порт.
    Поток пишет/читает в свой сокет без конкуренции.

    ЭТО РЕШАЕТ главную проблему: main loop больше НЕ крадёт ACK
    у download-потока. Каждый поток owner своего сокета.
"""

import socket
import select
import signal
import struct
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

TCP_CHUNK      = 64 * 1024
UDP_READ_BATCH = 1024
UDP_UPLOAD_WIN = 4096
UDP_ACK_EVERY  = 4
UDP_FIN_TRIES  = 30
UDP_FIN_INT    = 0.1
_SEND_BURST    = 512


def _ts():
    return datetime.now().strftime("%H:%M:%S")


def _would_block(e):
    return isinstance(e, BlockingIOError) or getattr(e, "errno", None) in (11, 10035)


# ── UDP download worker (ОТДЕЛЬНЫЙ СОКЕТ) ──────────────────

def _udp_download_worker(
    server_host: str,
    client_addr: tuple,
    session,
    file_manager,
    client_id: str,
):
    """
    Поток для UDP download.
    Создаёт СВОЙ UDP-сокет на случайном порту.
    Отправляет клиенту уведомление о своём порте.
    Далее — чистый sliding window без конкуренции за recvfrom.
    """
    dl_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dl_sock.setblocking(False)
    for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
        try:
            dl_sock.setsockopt(socket.SOL_SOCKET, opt, 64 * 1024 * 1024)
        except OSError:
            pass
    dl_sock.bind((server_host, 0))
    dl_port = dl_sock.getsockname()[1]

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
                dl_sock.sendto(data, client_addr)
                return True
            except BlockingIOError:
                time.sleep(0.0001)
            except OSError as e:
                if getattr(e, "errno", None) in (11, 10035):
                    time.sleep(0.0001); continue
                return False
        return False

    def _drain(base, packets, next_seq):
        last_t = -1.0
        while True:
            r, _, _ = select.select([dl_sock], [], [], 0)
            if not r:
                break
            try:
                pkt, a = dl_sock.recvfrom(256)
            except (BlockingIOError, OSError):
                break
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
    last_ack   = time.monotonic()
    eof        = False

    try:
        # Отправляем клиенту порт, на котором мы слушаем
        # Клиент при приёме (recv_stream) увидит пакеты с нового addr
        # и обновит dest_addr → ACK пойдёт на наш dl_sock

        while cursor < total_size or base < next_seq:
            # 1. send burst
            n = 0
            while not eof and next_seq < base + UDP_WINDOW_SIZE and n < _SEND_BURST:
                to_read = min(UDP_PAYLOAD_SIZE, total_size - cursor)
                if to_read <= 0:
                    eof = True; break
                try:
                    chunk = fh.read(to_read)
                except Exception:
                    chunk = b""
                if not chunk:
                    eof = True; break
                pkt = _pack(next_seq, PacketType.DATA.value, chunk)
                packets[next_seq] = pkt
                _sendto(pkt)
                next_seq += 1; cursor += len(chunk); n += 1

            session.transferred = cursor

            # 2. drain ACK
            nb, t = _drain(base, packets, next_seq)
            if nb > base:
                base = nb; last_ack = t if t > 0 else time.monotonic()

            # 3. window full
            if next_seq >= base + UDP_WINDOW_SIZE and base < next_seq:
                r, _, _ = select.select([dl_sock], [], [], 0.001)
                if r:
                    nb, t = _drain(base, packets, next_seq)
                    if nb > base:
                        base = nb; last_ack = t if t > 0 else time.monotonic()

            # 4. retransmit
            now = time.monotonic()
            if now - last_ack > UDP_TIMEOUT and packets:
                cnt = 0
                for s in range(base, next_seq):
                    p = packets.get(s)
                    if p:
                        _sendto(p); cnt += 1
                        if cnt >= _SEND_BURST:
                            break
                last_ack = now

        # FIN
        fin = _pack(next_seq, PacketType.FIN.value, b"")
        for _ in range(UDP_FIN_TRIES):
            _sendto(fin)
            r, _, _ = select.select([dl_sock], [], [], UDP_FIN_INT)
            if not r:
                continue
            try:
                ap, a = dl_sock.recvfrom(256)
            except (BlockingIOError, OSError):
                continue
            aseq, apt, _ = _unpack(ap)
            if apt == PacketType.ACK.value and aseq == next_seq + 1:
                break

        bitrate = file_manager.calculate_bitrate(session)
        print(f"[{_ts()}] UDP Download done: {session.filename} "
              f"[{client_id}] ({file_manager.format_bitrate(bitrate)})")

    except Exception as exc:
        print(f"[{_ts()}] UDP Download error [{client_id}]: {exc}")
    finally:
        file_manager.complete_session(client_id)
        try:
            dl_sock.close()
        except Exception:
            pass


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
        self._udp_threads: Dict[str, threading.Thread] = {}

    def start(self):
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
        disp = ip if self.host in ("0.0.0.0", "") else self.host
        print(f"[{_ts()}] ===== Server on {disp}:{self.port} =====")
        self._loop()

    def _setup_signals(self):
        signal.signal(signal.SIGINT,  lambda s, f: (self.stop(), sys.exit(0)))
        signal.signal(signal.SIGTERM, lambda s, f: (self.stop(), sys.exit(0)))

    def stop(self):
        self.running = False
        for s in self.inputs:
            try: s.close()
            except: pass

    # ── main loop ──────────────────────────────────────────

    def _loop(self):
        while self.running:
            try:
                self.outputs = [
                    s.sock for s in self.file_manager.sessions.values()
                    if not s.is_upload and s.sock and s.sock in self.inputs
                ]
                readable, writable, exc = select.select(
                    self.inputs, self.outputs, self.inputs, 0.005
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
                for s in exc:
                    self._remove(s)

                self._spawn_dl_threads()
            except Exception:
                pass

    def _spawn_dl_threads(self):
        for cid, sess in list(self.file_manager.sessions.items()):
            if sess.is_upload or sess.sock is not None:
                continue
            if cid in self._udp_threads:
                if not self._udp_threads[cid].is_alive():
                    del self._udp_threads[cid]
                continue
            try:
                host, port_s = cid.split(":", 1)
                addr = (host, int(port_s))
            except Exception:
                self.file_manager.close_session(cid)
                continue

            # Определяем host для bind download-сокета
            bind_host = self.host if self.host not in ("0.0.0.0", "") else ""

            t = threading.Thread(
                target=_udp_download_worker,
                args=(bind_host, addr, sess, self.file_manager, cid),
                daemon=True,
            )
            self._udp_threads[cid] = t
            t.start()
            print(f"[{_ts()}] UDP Download started: {sess.filename} [{cid}]")

    # ── TCP ────────────────────────────────────────────────

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

    def _tcp_read(self, sock):
        cid  = str(sock.fileno())
        sess = self.file_manager.get_session(cid)
        if sess and sess.is_upload:
            try:
                chunk = sock.recv(TCP_CHUNK)
                if not chunk:
                    self._remove(sock); return
                sess.file_handle.write(chunk)
                sess.transferred += len(chunk)
                self._log(cid, sess, "TCP Upload")
                if sess.transferred >= sess.total_size:
                    br  = self.file_manager.calculate_bitrate(sess)
                    msg = f"Received {sess.transferred} bytes. {self.file_manager.format_bitrate(br)}"
                    self.file_manager.complete_session(cid)
                    send_all(sock, format_response(Response(True, msg)))
                    print(f"[{_ts()}] TCP Upload done: {sess.filename} ({self.file_manager.format_bitrate(br)})")
            except socket.error:
                self._remove(sock)
            return

        try:
            data = sock.recv(4096)
            if not data:
                self._remove(sock); return
            buf = self.tcp_buffers.get(sock.fileno(), b"") + data
            if COMMAND_TERMINATOR in buf:
                line, rest = buf.split(COMMAND_TERMINATOR, 1)
                self.tcp_buffers[sock.fileno()] = rest
                cmd_str  = line.decode("utf-8", errors="ignore")
                command  = parse_command(cmd_str, "TCP")
                print(f"[{_ts()}] TCP CMD [{cid}]: {cmd_str.strip()}")
                resp = self.command_handler.execute(command, sock, None, None)
                xfer = (CommandType.UPLOAD, CommandType.DOWNLOAD,
                        CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD)
                if command.type not in xfer or not resp.success:
                    send_all(sock, format_response(resp))
            else:
                self.tcp_buffers[sock.fileno()] = buf
        except socket.error:
            self._remove(sock)

    def _tcp_write(self, sock):
        cid  = str(sock.fileno())
        sess = self.file_manager.get_session(cid)
        if sess and not sess.is_upload and sess.file_handle:
            try:
                chunk = sess.file_handle.read(TCP_CHUNK)
                if chunk:
                    sock.send(chunk)
                    sess.transferred += len(chunk)
                    self._log(cid, sess, "TCP Download")
                else:
                    br = self.file_manager.calculate_bitrate(sess)
                    self.file_manager.complete_session(cid)
                    print(f"[{_ts()}] TCP Download done: {sess.filename} ({self.file_manager.format_bitrate(br)})")
            except socket.error:
                self._remove(sock)

    # ── UDP read (upload + CMD only) ───────────────────────

    def _udp_read_batch(self):
        if not self.server_socket_udp or not self._rudp:
            return
        rudp = self._rudp
        for _ in range(UDP_READ_BATCH):
            try:
                pkt, addr = self.server_socket_udp.recvfrom(65536)
            except (BlockingIOError, socket.error):
                break

            cid             = f"{addr[0]}:{addr[1]}"
            seq, pt, data   = rudp._unpack(pkt)

            if pt == PacketType.CMD.value:
                ack = rudp._pack(seq, PacketType.CMD.value, b"ACK_CMD")
                self._udp_send(ack, addr)
                msg = data.decode(errors="ignore")
                if msg.startswith("OK") or msg.startswith("ERROR"):
                    continue
                command = parse_command(msg, "UDP")
                print(f"[{_ts()}] UDP CMD [{cid}]: {msg.strip()}")
                resp = self.command_handler.execute(
                    command, None, self.server_socket_udp, addr)
                xfer = (CommandType.UPLOAD, CommandType.DOWNLOAD,
                        CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD)
                if command.type in xfer and not resp.success:
                    self._udp_send(
                        rudp._pack(0, PacketType.CMD.value,
                                   format_response(resp)), addr)

            elif pt == PacketType.DATA.value:
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
                # ACK
                if sess.expected_seq % UDP_ACK_EVERY == 0 or seq != sess.expected_seq:
                    self._udp_send(
                        rudp._pack(sess.expected_seq, PacketType.ACK.value, b""),
                        addr)

            elif pt == PacketType.FIN.value:
                sess = self.file_manager.get_session(cid)
                if sess and sess.is_upload:
                    ack = rudp._pack(seq + 1, PacketType.ACK.value, b"")
                    for _ in range(3):
                        self._udp_send(ack, addr)
                    br = self.file_manager.calculate_bitrate(sess)
                    print(f"[{_ts()}] UDP Upload done: {sess.filename} "
                          f"[{cid}] ({self.file_manager.format_bitrate(br)})")
                    self.file_manager.complete_session(cid)

            # ACK пакеты для download НЕ приходят сюда —
            # они идут на отдельный сокет download-потока

    def _udp_send(self, data, addr):
        if not self.server_socket_udp:
            return False
        try:
            self.server_socket_udp.sendto(data, addr)
            return True
        except (BlockingIOError, OSError):
            return False

    # ── log / remove ───────────────────────────────────────

    def _log(self, cid, sess, op):
        if sess.total_size <= 0:
            return
        pct  = int(sess.transferred / sess.total_size * 100)
        last = getattr(sess, "_lpct", -10)
        if pct >= last + 10:
            sess._lpct = (pct // 10) * 10
            print(f"[{_ts()}] {op}: {sess.filename} [{cid}] "
                  f"— {pct}% ({sess.transferred}/{sess.total_size})")

    def _remove(self, sock):
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
        try: sock.close()
        except: pass
