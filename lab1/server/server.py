"""TCP/UDP сервер — upload drain в tight loop, download в отдельном потоке."""

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
_HDR           = struct.Struct("!IB")
_ACK_EVERY     = 256
_UPLOAD_WIN    = 4096

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
        fh = session.file_handle
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
        # Active UDP upload session (only one at a time)
        self._udp_upload_cid: Optional[str] = None
        self._udp_upload_addr: Optional[tuple] = None

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
                # If active UDP upload → tight drain mode
                if self._udp_upload_cid:
                    self._udp_upload_drain()
                    continue

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
                self._spawn_download()
            except Exception as exc:
                pass

    def _udp_upload_drain(self):
        """Tight loop: drain UDP packets for active upload. No TCP, no select timeout."""
        cid = self._udp_upload_cid
        addr = self._udp_upload_addr
        sess = self.file_manager.get_session(cid)
        if not sess or not sess.is_upload:
            self._udp_upload_cid = None
            self._udp_upload_addr = None
            return

        sock = self.server_socket_udp
        rudp = self._rudp
        ACK = PacketType.ACK.value
        DATA = PacketType.DATA.value
        FIN = PacketType.FIN.value
        CMD = PacketType.CMD.value
        cnt_ack = 0
        last_ack = time.monotonic()
        write_buf = bytearray()
        _FLUSH = 1024 * 1024  # 1 MB

        while self.running:
            r, _, _ = select.select([sock], [], [], 0.05)
            if not r:
                now = time.monotonic()
                if now - last_ack > 0.05:
                    rudp._send(_HDR.pack(sess.expected_seq, ACK), addr)
                    last_ack = now
                if now - sess.last_activity > 30.0:
                    print(f"[{_ts()}] UDP Upload timeout [{cid}]")
                    break
                continue

            while True:
                try: pkt, paddr = sock.recvfrom(65536)
                except: break

                if len(pkt) < _HDR.size: continue
                seq, pt = _HDR.unpack_from(pkt)

                if pt == CMD:
                    # Handle commands even during upload
                    data = pkt[_HDR.size:]
                    rudp._send(rudp._pack(seq, CMD, b"ACK_CMD"), paddr)
                    continue

                if paddr != addr: continue
                sess.last_activity = time.monotonic()

                if pt == DATA:
                    data = pkt[_HDR.size:]
                    if seq == sess.expected_seq:
                        write_buf.extend(data)
                        sess.transferred += len(data)
                        sess.expected_seq += 1
                        cnt_ack += 1
                        # Drain OOO buffer
                        while sess.expected_seq in sess.udp_recv_buffer:
                            d = sess.udp_recv_buffer.pop(sess.expected_seq)
                            write_buf.extend(d)
                            sess.transferred += len(d)
                            sess.expected_seq += 1
                            cnt_ack += 1
                        # Flush write buffer
                        if len(write_buf) >= _FLUSH:
                            sess.file_handle.write(bytes(write_buf))
                            write_buf.clear()
                        self._log(cid, sess, "UDP Upload")
                    elif seq > sess.expected_seq:
                        if seq < sess.expected_seq + _UPLOAD_WIN:
                            sess.udp_recv_buffer.setdefault(seq, pkt[_HDR.size:])
                        cnt_ack = _ACK_EVERY  # force ACK on OOO

                    # ACK
                    if cnt_ack >= _ACK_EVERY or time.monotonic() - last_ack > 0.01:
                        rudp._send(_HDR.pack(sess.expected_seq, ACK), addr)
                        cnt_ack = 0; last_ack = time.monotonic()

                elif pt == FIN:
                    # Flush remaining
                    if write_buf:
                        sess.file_handle.write(bytes(write_buf))
                        write_buf.clear()
                    # Send ACK for FIN
                    ack = _HDR.pack(seq + 1, ACK)
                    for _ in range(5): rudp._send(ack, addr)
                    br = self.file_manager.calculate_bitrate(sess)
                    print(f"[{_ts()}] UDP Upload done: {sess.filename} "
                          f"[{cid}] ({self.file_manager.format_bitrate(br)})")
                    self.file_manager.complete_session(cid)
                    self._udp_upload_cid = None
                    self._udp_upload_addr = None
                    return

                r2, _, _ = select.select([sock], [], [], 0)
                if not r2: break

            # Periodic ACK outside inner loop
            if cnt_ack > 0 and time.monotonic() - last_ack > 0.005:
                rudp._send(_HDR.pack(sess.expected_seq, ACK), addr)
                cnt_ack = 0; last_ack = time.monotonic()

        # Timeout / stopped
        if write_buf:
            sess.file_handle.write(bytes(write_buf))
        self.file_manager.complete_session(cid)
        self._udp_upload_cid = None
        self._udp_upload_addr = None

    def _spawn_download(self):
        for cid, sess in list(self.file_manager.sessions.items()):
            if sess.is_upload or sess.sock is not None: continue
            if cid in self._threads:
                if not self._threads[cid].is_alive(): del self._threads[cid]
                continue
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
        """Handle UDP commands. When upload starts → activate tight drain mode."""
        if not self.server_socket_udp or not self._rudp: return
        rudp = self._rudp
        for _ in range(512):
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
                        # Activate tight drain mode for this upload
                        self._udp_upload_cid = cid
                        self._udp_upload_addr = addr
                        print(f"[{_ts()}] UDP Upload started: {sess.filename} [{cid}]")
                        return  # exit _udp_read, main loop will enter drain mode
                elif cmd.type in xf:
                    rudp._send(rudp._pack(0, PacketType.CMD.value,
                               format_response(resp)), addr)

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
