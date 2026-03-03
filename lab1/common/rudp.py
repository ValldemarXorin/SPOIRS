"""
Reliable UDP — windowed, optimized for dedicated receiver thread.

Upload: client sends windowed data to server's dedicated upload socket.
Download: server sends windowed data from dedicated download socket.
Both sides: dedicated socket = tight recv loop = no packet loss.

Window = 1024 × 4091 = ~4 MB in flight.
ACK every 256 packets from receiver.
"""

import socket
import struct
import time
import select
from typing import Optional, Tuple, Dict, Callable

from .protocol import (
    UDP_PAYLOAD_SIZE, UDP_HEADER_SIZE, UDP_WINDOW_SIZE,
    UDP_TIMEOUT, PacketType, UDP_RETRY_LIMIT,
)

_HDR = struct.Struct("!IB")
_READ_CHUNK = 4 * 1024 * 1024
_ACK_EVERY = 256
_BURST = 512


class RUDPSocket:

    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock      = sock
        self.dest_addr = dest_addr
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try: self.sock.setsockopt(socket.SOL_SOCKET, opt, 8 * 1024 * 1024)
            except OSError: pass

    def _pack(self, seq: int, ptype: int, data: bytes = b"") -> bytes:
        return _HDR.pack(seq, ptype) + data

    def _send(self, data: bytes, addr: tuple) -> bool:
        try:
            self.sock.sendto(data, addr); return True
        except BlockingIOError:
            time.sleep(0.0001)
            try: self.sock.sendto(data, addr); return True
            except: return False
        except OSError:
            return False

    # ── CMD (on main socket) ──────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
        pkt = self._pack(0, PacketType.CMD.value, text.encode())
        ack = False
        for _ in range(UDP_RETRY_LIMIT):
            if not ack and self.dest_addr: self._send(pkt, self.dest_addr)
            wait = 1.0 if ack else 0.3; t0 = time.monotonic()
            while time.monotonic() - t0 < wait:
                r, _, _ = select.select([self.sock], [], [], 0.05)
                if not r: continue
                try: rp, ra = self.sock.recvfrom(65536)
                except: break
                if self.dest_addr and ra != self.dest_addr: continue
                if len(rp) < _HDR.size: continue
                _, rt = _HDR.unpack_from(rp)
                if rt != PacketType.CMD.value: continue
                d = rp[_HDR.size:].decode(errors="ignore")
                if d == "ACK_CMD": ack = True; continue
                return d
        return None

    def recv_command(self) -> Tuple[str, Tuple[str, int]]:
        try: pkt, addr = self.sock.recvfrom(65536)
        except: return "", ("", 0)
        if len(pkt) < _HDR.size: return "", addr
        s, t = _HDR.unpack_from(pkt)
        if t != PacketType.CMD.value: return "", addr
        msg = pkt[_HDR.size:].decode(errors="ignore")
        if msg.startswith("OK ") or msg.startswith("ERROR "): return "", addr
        self._send(self._pack(s, PacketType.CMD.value, b"ACK_CMD"), addr)
        return msg, addr

    # ── send_stream (windowed, fast) ──────────────────────

    def send_stream(self, reader, total_size: int,
                    progress_callback: Callable[[int], None] = None) -> None:
        """Send file data with fixed window. Used by client upload & server download."""
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")
        addr   = self.dest_addr
        sock   = self.sock
        sendto = sock.sendto
        DATA   = PacketType.DATA.value
        ACK    = PacketType.ACK.value
        FIN    = PacketType.FIN.value
        psize  = UDP_PAYLOAD_SIZE
        win    = UDP_WINDOW_SIZE

        base     = 0
        next_seq = 0
        pkt_buf  = []
        pkt_off  = 0
        cursor   = 0
        eof      = False
        last_ack = time.monotonic()
        file_buf = b""
        fb_pos   = 0

        while cursor < total_size or base < next_seq:
            # 1. SEND — fill window
            room = min(_BURST, base + win - next_seq)
            if room > 0 and not eof:
                sent_n = 0
                while sent_n < room:
                    if fb_pos >= len(file_buf):
                        file_buf = reader.read(_READ_CHUNK)
                        fb_pos = 0
                        if not file_buf:
                            eof = True; cursor = total_size; break
                    end = min(fb_pos + psize, len(file_buf))
                    chunk = file_buf[fb_pos:end]
                    fb_pos = end

                    pkt = _HDR.pack(next_seq, DATA) + chunk
                    pkt_buf.append(pkt)
                    try: sendto(pkt, addr)
                    except BlockingIOError:
                        time.sleep(0.00002)
                        try: sendto(pkt, addr)
                        except: pass
                    except: pass
                    next_seq += 1
                    cursor += len(chunk)
                    sent_n += 1

                if progress_callback:
                    progress_callback(min(cursor, total_size))

            # 2. DRAIN ACKs
            got_new = False
            while True:
                r, _, _ = select.select([sock], [], [], 0)
                if not r: break
                try: ap, _ = sock.recvfrom(64)
                except: break
                if len(ap) < _HDR.size: continue
                s, t = _HDR.unpack_from(ap)
                if t != ACK: continue
                if s > base:
                    freed = s - pkt_off
                    if 0 < freed <= len(pkt_buf):
                        del pkt_buf[:freed]; pkt_off = s
                    elif freed > len(pkt_buf):
                        pkt_buf.clear(); pkt_off = s
                    base = s; got_new = True; last_ack = time.monotonic()

            if got_new: continue

            # 3. No progress — timeout retransmit or wait
            now = time.monotonic()
            if now - last_ack > UDP_TIMEOUT and pkt_buf:
                cnt = 0
                for p in pkt_buf:
                    try: sendto(p, addr)
                    except: pass
                    cnt += 1
                    if cnt >= 64: break
                last_ack = now
            elif base < next_seq:
                select.select([sock], [], [], 0.0002)

        # FIN
        fin = self._pack(next_seq, FIN)
        for _ in range(25):
            self._send(fin, addr)
            r, _, _ = select.select([sock], [], [], 0.2)
            if not r: continue
            try: ap, aa = sock.recvfrom(64)
            except: continue
            if aa != addr: continue
            if len(ap) < _HDR.size: continue
            s, t = _HDR.unpack_from(ap)
            if t == ACK and s == next_seq + 1: break

    # ── recv_stream (tight loop, for dedicated socket) ────

    def recv_stream(self, writer, total_size: int = 0,
                    progress_callback: Callable[[int], None] = None) -> int:
        """Receive file data. Sends ACK every _ACK_EVERY packets."""
        sock     = self.sock
        DATA     = PacketType.DATA.value
        ACK      = PacketType.ACK.value
        FIN      = PacketType.FIN.value
        CMD      = PacketType.CMD.value
        win      = UDP_WINDOW_SIZE

        expected = 0
        ooo: Dict[int, bytes] = {}
        total    = 0
        last_pkt = time.monotonic()
        cnt_ack  = 0
        last_ack = time.monotonic()
        write_buf = bytearray()
        _FLUSH   = 1024 * 1024

        while True:
            now = time.monotonic()
            if now - last_pkt > 30.0: break

            r, _, _ = select.select([sock], [], [], 0.05)
            if not r:
                # Periodic ACK to keep sender alive
                if now - last_ack > 0.05 and self.dest_addr:
                    self._send(_HDR.pack(expected, ACK), self.dest_addr)
                    last_ack = now
                continue

            # Drain all available
            while True:
                try: pkt, addr = sock.recvfrom(65536)
                except: break

                if self.dest_addr is None:
                    self.dest_addr = addr
                elif addr != self.dest_addr: continue

                if len(pkt) < _HDR.size: continue
                seq, pt = _HDR.unpack_from(pkt)
                last_pkt = time.monotonic()

                if pt == CMD: continue
                if pt == FIN:
                    if write_buf:
                        writer.write(bytes(write_buf)); write_buf.clear()
                    ack = _HDR.pack(seq + 1, ACK)
                    for _ in range(3): self._send(ack, addr)
                    return total
                if pt != DATA: continue

                data = pkt[_HDR.size:]
                if seq == expected:
                    write_buf.extend(data); total += len(data)
                    expected += 1; cnt_ack += 1
                    while expected in ooo:
                        d = ooo.pop(expected)
                        write_buf.extend(d); total += len(d)
                        expected += 1; cnt_ack += 1
                    if len(write_buf) >= _FLUSH:
                        writer.write(bytes(write_buf)); write_buf.clear()
                    if progress_callback:
                        progress_callback(total)
                elif seq > expected and seq < expected + win * 4:
                    ooo.setdefault(seq, data)
                    cnt_ack = _ACK_EVERY  # force ACK on OOO

                if cnt_ack >= _ACK_EVERY or time.monotonic() - last_ack > 0.01:
                    self._send(_HDR.pack(expected, ACK), addr)
                    cnt_ack = 0; last_ack = time.monotonic()

                r2, _, _ = select.select([sock], [], [], 0)
                if not r2: break

            # Periodic ACK
            if cnt_ack > 0 and time.monotonic() - last_ack > 0.005:
                self._send(_HDR.pack(expected, ACK),
                           self.dest_addr if self.dest_addr else addr)
                cnt_ack = 0; last_ack = time.monotonic()

        if write_buf:
            writer.write(bytes(write_buf))
        return total
