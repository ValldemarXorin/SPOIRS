"""TCP/UDP сервер — max throughput UDP."""

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
UDP_READ_BATCH = 1024
UDP_UPLOAD_WIN = 4096
UDP_ACK_EVERY  = 128     # match client: ACK every 128 packets
_BURST         = 512
_PACK_HDR      = struct.Struct("!IB")
_HDR           = 5
_READ_CHUNK    = 4 * 1024 * 1024

def _ts():
    return datetime.now().strftime("%H:%M:%S")


def _udp_download_worker(server_host, client_addr, session, fm, cid):
    """UDP download — max throughput, fixed window."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
        try: sock.setsockopt(socket.SOL_SOCKET, opt, 8*1024*1024)
        except: pass
    sock.bind((server_host, 0))

    sendto = sock.sendto
    DATA = PacketType.DATA.value
    ACK  = PacketType.ACK.value
    FIN  = PacketType.FIN.value
    payload = UDP_PAYLOAD_SIZE
    win  = UDP_WINDOW_SIZE

    ts   = session.total_size
    fh   = session.file_handle
    base = 0; nxt = 0; cur = 0; eof = False
    lat  = time.monotonic()
    pkt_buf = []
    pkt_off = 0
    file_buf = b""
    fb_pos   = 0

    try:
        while cur < ts or base < nxt:
            # SEND
            room = base + win - nxt
            if room > 0 and not eof:
                n = 0
                while n < room:
                    if fb_pos >= len(file_buf):
                        remain = ts - cur
                        if remain <= 0: eof = True; break
                        file_buf = fh.read(min(_READ_CHUNK, remain))
                        fb_pos = 0
                        if not file_buf: eof = True; break
                    end = min(fb_pos + payload, len(file_buf))
                    chunk = file_buf[fb_pos:end]
                    fb_pos = end
                    pkt = _PACK_HDR.pack(nxt, DATA) + chunk
                    pkt_buf.append(pkt)
                    try: sendto(pkt, client_addr)
                    except BlockingIOError:
                        time.sleep(0.00002)
                        try: sendto(pkt, client_addr)
                        except: pass
                    except: pass
                    nxt += 1; cur += len(chunk); n += 1
                session.transferred = cur

            # DRAIN ACK
            gn = False
            while True:
                r,_,_ = select.select([sock],[],[],0)
                if not r: break
                try: ap,_ = sock.recvfrom(64)
                except: break
                s,t = _PACK_HDR.unpack_from(ap)
                if t != ACK: continue
                if s > base:
                    freed = s - pkt_off
                    if freed > 0 and freed <= len(pkt_buf):
                        del pkt_buf[:freed]; pkt_off = s
                    elif freed > len(pkt_buf):
                        pkt_buf.clear(); pkt_off = s
                    base = s; gn = True; lat = time.monotonic()

            if gn: continue

            now = time.monotonic()
            if now - lat > UDP_TIMEOUT and pkt_buf:
                c = 0
                for p in pkt_buf:
                    try: sendto(p, client_addr)
                    except: pass
                    c += 1
                    if c >= 64: break
                lat = now
            elif base < nxt:
                select.select([sock],[],[],0.0002)

        # FIN
        fin = _PACK_HDR.pack(nxt, FIN)
        for _ in range(30):
            try: sendto(fin, client_addr)
            except: pass
            r,_,_ = select.select([sock],[],[],0.1)
            if not r: continue
            try: ap,_ = sock.recvfrom(64)
            except: continue
            s,t = _PACK_HDR.unpack_from(ap)
            if t == ACK and s == nxt+1: break

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
                    self.inputs, self.outputs, self.inputs, 0.001)
                for s in rd:
                    if s is self.server_socket_tcp: self._tcp_accept()
                    elif s is self.server_socket_udp: self._udp_read()
                    else: self._tcp_read(s)
                for s in wr: self._tcp_write(s)
                for s in ex: self._remove(s)
                self._spawn()
            except: pass

    def _spawn(self):
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
        if not self.server_socket_udp or not self._rudp: return
        rudp = self._rudp
        ACK  = PacketType.ACK.value
        for _ in range(UDP_READ_BATCH):
            try: pkt, addr = self.server_socket_udp.recvfrom(65536)
            except (BlockingIOError, socket.error): break
            cid = f"{addr[0]}:{addr[1]}"
            if len(pkt) < _HDR: continue
            seq, pt = _PACK_HDR.unpack_from(pkt)
            data = pkt[_HDR:]

            if pt == PacketType.CMD.value:
                rudp._send(rudp._pack(seq, PacketType.CMD.value, b"ACK_CMD"), addr)
                msg = data.decode(errors="ignore")
                if msg.startswith("OK") or msg.startswith("ERROR"): continue
                cmd = parse_command(msg, "UDP")
                print(f"[{_ts()}] UDP CMD [{cid}]: {msg.strip()}")
                resp = self.command_handler.execute(cmd, None, self.server_socket_udp, addr)
                xf = (CommandType.UPLOAD, CommandType.DOWNLOAD,
                      CommandType.RESUME_UPLOAD, CommandType.RESUME_DOWNLOAD)
                if cmd.type in xf and not resp.success:
                    rudp._send(rudp._pack(0, PacketType.CMD.value,
                               format_response(resp)), addr)

            elif pt == PacketType.DATA.value:
                sess = self.file_manager.get_session(cid)
                if not sess or not sess.is_upload: continue
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
                # ACK every 128 packets — KEY for throughput
                if sess.expected_seq % UDP_ACK_EVERY == 0 or seq != sess.expected_seq:
                    rudp._send(_PACK_HDR.pack(sess.expected_seq, ACK), addr)

            elif pt == PacketType.FIN.value:
                sess = self.file_manager.get_session(cid)
                if sess and sess.is_upload:
                    ack = _PACK_HDR.pack(seq+1, ACK)
                    for _ in range(3): rudp._send(ack, addr)
                    br = self.file_manager.calculate_bitrate(sess)
                    print(f"[{_ts()}] UDP Upload done: {sess.filename} "
                          f"[{cid}] ({self.file_manager.format_bitrate(br)})")
                    self.file_manager.complete_session(cid)

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
