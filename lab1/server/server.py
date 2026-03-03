"""TCP/UDP сервер — UDP upload/download в отдельных потоках с выделенными сокетами."""

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
    Command, CommandType, parse_command, format_response,
    COMMAND_TERMINATOR, Response, PacketType,
    UDP_PAYLOAD_SIZE, UDP_WINDOW_SIZE, UDP_TIMEOUT,
)
from common.socket_utils import create_server_socket, send_all
from common.rudp import RUDPSocket
from server.command_handler import CommandHandler
from server.file_manager import FileManager

TCP_CHUNK      = 64 * 1024
UDP_READ_BATCH = 512
_HDR           = struct.Struct("!IB")
_READ_CHUNK    = 4 * 1024 * 1024

def _ts():
    return datetime.now().strftime("%H:%M:%S")


def _udp_download_worker(server_host, client_addr, session, fm, cid):
    """UDP download: dedicated socket, windowed send."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
        try: sock.setsockopt(socket.SOL_SOCKET, opt, 8*1024*1024)
        except: pass
    sock.bind((server_host, 0))

    try:
        rudp = RUDPSocket(sock, client_addr)
        with session.file_handle as fh:
            rudp.send_stream(fh, session.total_size)
        session.transferred = session.total_size
        br = fm.calculate_bitrate(session)
        print(f"[{_ts()}] UDP Download done: {session.filename} "
              f"[{cid}] ({fm.format_bitrate(br)})")
    except Exception as exc:
        print(f"[{_ts()}] UDP Download error [{cid}]: {exc}")
    finally:
        fm.complete_session(cid)
        try: sock.close()
        except: pass


def _udp_upload_worker(server_host, client_addr, session, fm, cid, main_sock):
    """UDP upload: dedicated socket, tight recv loop.

    Binds a new socket, tells client the port via main_sock,
    then receives all data on the dedicated socket.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
        try: sock.setsockopt(socket.SOL_SOCKET, opt, 8*1024*1024)
        except: pass
    sock.bind((server_host, 0))

    _, upload_port = sock.getsockname()

    # Tell client the dedicated upload port via CMD response on main socket
    port_msg = _HDR.pack(0, PacketType.CMD.value) + f"UPLOAD_PORT {upload_port}".encode()
    for _ in range(10):
        try: main_sock.sendto(port_msg, client_addr)
        except: pass
        time.sleep(0.02)

    try:
        rudp = RUDPSocket(sock, client_addr)
        with session.file_handle as fh:
            received = rudp.recv_stream(fh, session.total_size)
        session.transferred = received
        br = fm.calculate_bitrate(session)
        print(f"[{_ts()}] UDP Upload done: {session.filename} "
              f"[{cid}] ({fm.format_bitrate(br)})")
    except Exception as exc:
        print(f"[{_ts()}] UDP Upload error [{cid}]: {exc}")
    finally:
        fm.complete_session(cid)
        try: sock.close()
        except: pass


class TCPServer:

    def __init__(self, host="0.0.0.0", port=9000):
        self.host = host; self.port = port; self.running = False
        self.server_socket_tcp: Optional[socket.socket] = None
        self.server_socket_udp: Optional[socket.socket] = None
        self.file_manager    = FileManager()
        self.command_handler = CommandHandler(self.file_manager)
        self.inputs  = []; self.outputs = []
        self.tcp_buffers: Dict[int, bytes] = {}
        self._rudp: Optional[RUDPSocket] = None
        self._threads: Dict[str, threading.Thread] = {}

    def start(self):
        self._setup_signals()
        self.server_socket_tcp = create_server_socket(self.host, self.port)
        self.server_socket_tcp.setblocking(False)
        self.inputs.append(self.server_socket_tcp)
        self.server_socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try: self.server_socket_udp.setsockopt(socket.SOL_SOCKET, opt, 8*1024*1024)
            except: pass
        self.server_socket_udp.bind((self.host, self.port))
        self.server_socket_udp.setblocking(False)
        self.inputs.append(self.server_socket_udp)
        self._rudp = RUDPSocket(self.server_socket_udp)
        self.running = True
        try: ip = socket.gethostbyname(socket.gethostname())
        except: ip = "127.0.0.1"
        d = ip if self.host in ("0.0.0.0","") else self.host
        print(f"[{_ts()}] ===== Server on {d}:{self.port} =====")
        self._loop()

    def _setup_signals(self):
        signal.signal(signal.SIGINT,  lambda s,f: (self.stop(), sys.exit(0)))
        signal.signal(signal.SIGTERM, lambda s,f: (self.stop(), sys.exit(0)))

    def stop(self):
        self.running = False
        for s in self.inputs:
            try: s.close()
            except: pass

    def _loop(self):
        while self.running:
            try:
                self.outputs = [
                    s.sock for s in self.file_manager.sessions.values()
                    if not s.is_upload and s.sock and s.sock in self.inputs]
                rd, wr, ex = select.select(
                    self.inputs, self.outputs, self.inputs, 0.01)
                for s in rd:
                    if s is self.server_socket_tcp: self._tcp_accept()
                    elif s is self.server_socket_udp: self._udp_read()
                    else: self._tcp_read(s)
                for s in wr: self._tcp_write(s)
                for s in ex: self._remove(s)
                self._spawn_threads()
            except: pass

    def _spawn_threads(self):
        """Spawn dedicated threads for UDP download sessions."""
        for cid, sess in list(self.file_manager.sessions.items()):
            if sess.sock is not None: continue  # TCP session
            if cid in self._threads:
                if not self._threads[cid].is_alive(): del self._threads[cid]
                continue
            if sess.is_upload: continue  # upload spawned immediately in _udp_read
            # Download: spawn worker
            try:
                h,p = cid.split(":",1); addr = (h, int(p))
            except:
                self.file_manager.close_session(cid); continue
            bh = self.host if self.host not in ("0.0.0.0","") else ""
            t = threading.Thread(target=_udp_download_worker,
                                 args=(bh, addr, sess, self.file_manager, cid),
                                 daemon=True)
            self._threads[cid] = t; t.start()
            print(f"[{_ts()}] UDP Download started: {sess.filename} [{cid}]")

    def _tcp_accept(self):
        try:
            cs, ca = self.server_socket_tcp.accept()
            print(f"[{_ts()}] TCP Connect: {ca[0]}:{ca[1]}")
            cs.setblocking(False); self.inputs.append(cs)
            self.tcp_buffers[cs.fileno()] = b""
            send_all(cs, b"220 Welcome\n")
        except: pass

    def _tcp_read(self, sock):
        cid = str(sock.fileno())
        sess = self.file_manager.get_session(cid)
        if sess and sess.is_upload:
            try:
                chunk = sock.recv(TCP_CHUNK)
                if not chunk: self._remove(sock); return
                sess.file_handle.write(chunk)
                sess.transferred += len(chunk)
                self._log(cid, sess, "TCP Upload")
                if sess.transferred >= sess.total_size:
                    br = self.file_manager.calculate_bitrate(sess)
                    self.file_manager.complete_session(cid)
                    send_all(sock, format_response(Response(True,
                        f"Received {sess.transferred} bytes. "
                        f"{self.file_manager.format_bitrate(br)}")))
                    print(f"[{_ts()}] TCP Upload done ({self.file_manager.format_bitrate(br)})")
            except socket.error: self._remove(sock)
            return
        try:
            data = sock.recv(4096)
            if not data: self._remove(sock); return
            buf = self.tcp_buffers.get(sock.fileno(), b"") + data
            if COMMAND_TERMINATOR in buf:
                line, rest = buf.split(COMMAND_TERMINATOR, 1)
                self.tcp_buffers[sock.fileno()] = rest
                cs = line.decode("utf-8", errors="ignore")
                cmd = parse_command(cs, "TCP")
                print(f"[{_ts()}] TCP CMD [{cid}]: {cs.strip()}")
                resp = self.command_handler.execute(cmd, sock, None, None)
                xf = (CommandType.UPLOAD, CommandType.DOWNLOAD,
                      CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD)
                if cmd.type not in xf or not resp.success:
                    send_all(sock, format_response(resp))
            else:
                self.tcp_buffers[sock.fileno()] = buf
        except socket.error: self._remove(sock)

    def _tcp_write(self, sock):
        cid = str(sock.fileno())
        sess = self.file_manager.get_session(cid)
        if sess and not sess.is_upload and sess.file_handle:
            try:
                chunk = sess.file_handle.read(TCP_CHUNK)
                if chunk:
                    sock.send(chunk); sess.transferred += len(chunk)
                    self._log(cid, sess, "TCP Download")
                else:
                    br = self.file_manager.calculate_bitrate(sess)
                    self.file_manager.complete_session(cid)
                    print(f"[{_ts()}] TCP Download done ({self.file_manager.format_bitrate(br)})")
            except socket.error: self._remove(sock)

    def _udp_read(self):
        """Handle UDP commands only. Data transfer happens in worker threads."""
        if not self.server_socket_udp or not self._rudp: return
        rudp = self._rudp
        for _ in range(UDP_READ_BATCH):
            try: pkt, addr = self.server_socket_udp.recvfrom(65536)
            except (BlockingIOError, socket.error): break
            if len(pkt) < _HDR.size: continue
            seq, pt = _HDR.unpack_from(pkt)

            if pt == PacketType.CMD.value:
                data = pkt[_HDR.size:]
                rudp._send(rudp._pack(seq, PacketType.CMD.value, b"ACK_CMD"), addr)
                msg = data.decode(errors="ignore")
                if msg.startswith("OK") or msg.startswith("ERROR"): continue
                cid = f"{addr[0]}:{addr[1]}"
                cmd = parse_command(msg, "UDP")
                print(f"[{_ts()}] UDP CMD [{cid}]: {msg.strip()}")
                resp = self.command_handler.execute(cmd, None, self.server_socket_udp, addr)
                xf = (CommandType.UPLOAD, CommandType.DOWNLOAD,
                      CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD)
                if cmd.type in xf and resp.success:
                    sess = self.file_manager.get_session(cid)
                    if sess and sess.is_upload:
                        # Spawn upload worker with dedicated socket
                        bh = self.host if self.host not in ("0.0.0.0","") else ""
                        t = threading.Thread(
                            target=_udp_upload_worker,
                            args=(bh, addr, sess, self.file_manager, cid,
                                  self.server_socket_udp),
                            daemon=True)
                        self._threads[cid] = t; t.start()
                        print(f"[{_ts()}] UDP Upload started: {sess.filename} [{cid}]")
                elif cmd.type in xf:
                    rudp._send(rudp._pack(0, PacketType.CMD.value,
                               format_response(resp)), addr)
            # Ignore DATA/FIN/ACK on main socket — handled by worker threads

    def _log(self, cid, sess, op):
        if sess.total_size <= 0: return
        pct = int(sess.transferred / sess.total_size * 100)
        last = getattr(sess, "_lpct", -10)
        if pct >= last + 10:
            sess._lpct = (pct // 10) * 10
            print(f"[{_ts()}] {op}: {sess.filename} [{cid}] — {pct}%")

    def _remove(self, sock):
        try: a = sock.getpeername(); print(f"[{_ts()}] Disconnected: {a[0]}:{a[1]}")
        except: pass
        if sock in self.inputs: self.inputs.remove(sock)
        if sock in self.outputs: self.outputs.remove(sock)
        self.tcp_buffers.pop(sock.fileno(), None)
        self.file_manager.close_session(str(sock.fileno()))
        try: sock.close()
        except: pass
