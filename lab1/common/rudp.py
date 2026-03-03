"""
Reliable UDP — max throughput edition.

Key: minimize syscalls per byte transferred.
- ACK every 128 packets (not 4) → 32x less ACK overhead
- Batch file read (4 MB at a time) → less read() syscalls  
- Pre-build all packets in memory → one tight sendto loop
- Fixed window 512 × 8KB = 4 MB
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

_ACK_EVERY  = 128   # receiver ACKs every N packets
_READ_CHUNK = 4 * 1024 * 1024  # 4 MB file read batch

# Pre-compute header bytes for speed
_DATA_TYPE = PacketType.DATA.value
_ACK_TYPE  = PacketType.ACK.value
_FIN_TYPE  = PacketType.FIN.value
_CMD_TYPE  = PacketType.CMD.value
_HDR_SIZE  = UDP_HEADER_SIZE
_PACK_HDR  = struct.Struct("!IB")


class RUDPSocket:

    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock      = sock
        self.dest_addr = dest_addr
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try: self.sock.setsockopt(socket.SOL_SOCKET, opt, 8 * 1024 * 1024)
            except OSError: pass

    def _pack(self, seq: int, ptype: int, data: bytes = b"") -> bytes:
        return _PACK_HDR.pack(seq, ptype) + data

    def _unpack_hdr(self, pkt: bytes) -> Tuple[int, int]:
        if len(pkt) < _HDR_SIZE: return -1, -1
        return _PACK_HDR.unpack_from(pkt)

    def _send(self, data: bytes, addr: tuple) -> bool:
        try:
            self.sock.sendto(data, addr); return True
        except BlockingIOError:
            # One retry after tiny sleep
            time.sleep(0.00005)
            try: self.sock.sendto(data, addr); return True
            except: return False
        except OSError:
            return False

    # ── CMD ────────────────────────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
        pkt = self._pack(0, _CMD_TYPE, text.encode())
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
                s, t = self._unpack_hdr(rp)
                if t != _CMD_TYPE: continue
                d = rp[_HDR_SIZE:].decode(errors="ignore")
                if d == "ACK_CMD": ack = True; continue
                return d
        return None

    def recv_command(self) -> Tuple[str, Tuple[str, int]]:
        try: pkt, addr = self.sock.recvfrom(65536)
        except: return "", ("", 0)
        s, t = self._unpack_hdr(pkt)
        if t != _CMD_TYPE: return "", addr
        msg = pkt[_HDR_SIZE:].decode(errors="ignore")
        if msg.startswith("OK ") or msg.startswith("ERROR "): return "", addr
        self._send(self._pack(s, _CMD_TYPE, b"ACK_CMD"), addr)
        return msg, addr

    # ── send_stream ────────────────────────────────────────

    def send_stream(self, reader, total_size: int,
                    progress_callback: Callable[[int], None] = None) -> None:
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")
        addr     = self.dest_addr
        sock     = self.sock
        sendto   = sock.sendto
        payload  = UDP_PAYLOAD_SIZE
        win      = UDP_WINDOW_SIZE

        base     = 0
        next_seq = 0
        # Use list for O(1) indexed access (packets[seq - pkt_offset])
        pkt_buf: list = []   # list of (seq, bytes)
        pkt_off  = 0         # offset: pkt_buf[0] corresponds to seq=pkt_off
        cursor   = 0
        eof      = False
        last_ack = time.monotonic()
        file_buf = b""
        fb_pos   = 0

        while cursor < total_size or base < next_seq:

            # 1. SEND — fill window
            room = base + win - next_seq
            if room > 0 and not eof:
                sent_this = 0
                while sent_this < room:
                    # refill file buffer if needed
                    if fb_pos >= len(file_buf):
                        file_buf = reader.read(_READ_CHUNK)
                        fb_pos = 0
                        if not file_buf:
                            eof = True; cursor = total_size; break

                    end = min(fb_pos + payload, len(file_buf))
                    chunk = file_buf[fb_pos:end]
                    fb_pos = end

                    hdr = _PACK_HDR.pack(next_seq, _DATA_TYPE)
                    pkt = hdr + chunk
                    pkt_buf.append(pkt)

                    try: sendto(pkt, addr)
                    except BlockingIOError:
                        time.sleep(0.00002)
                        try: sendto(pkt, addr)
                        except: pass
                    except: pass

                    next_seq += 1
                    cursor += len(chunk)
                    sent_this += 1

                if progress_callback:
                    progress_callback(min(cursor, total_size))

            # 2. DRAIN ACKs
            got_new = False
            while True:
                r, _, _ = select.select([sock], [], [], 0)
                if not r: break
                try: ap, _ = sock.recvfrom(64)
                except: break
                s, t = self._unpack_hdr(ap)
                if t != _ACK_TYPE: continue
                if s > base:
                    # Free acked packets
                    freed = s - pkt_off
                    if freed > 0 and freed <= len(pkt_buf):
                        del pkt_buf[:freed]
                        pkt_off = s
                    elif freed > len(pkt_buf):
                        pkt_buf.clear()
                        pkt_off = s
                    base = s
                    got_new = True
                    last_ack = time.monotonic()

            if got_new: continue

            # 3. No progress
            now = time.monotonic()
            if now - last_ack > UDP_TIMEOUT and pkt_buf:
                cnt = 0
                for pkt in pkt_buf:
                    try: sendto(pkt, addr)
                    except: pass
                    cnt += 1
                    if cnt >= 64: break
                last_ack = now
            elif base < next_seq:
                select.select([sock], [], [], 0.0002)

        # FIN
        fin = self._pack(next_seq, _FIN_TYPE)
        for _ in range(25):
            self._send(fin, addr)
            r, _, _ = select.select([sock], [], [], 0.2)
            if not r: continue
            try: ap, aa = sock.recvfrom(64)
            except: continue
            if aa != addr: continue
            s, t = self._unpack_hdr(ap)
            if t == _ACK_TYPE and s == next_seq + 1: break

    # ── recv_stream ────────────────────────────────────────

    def recv_stream(self, writer, total_size: int = 0,
                    progress_callback: Callable[[int], None] = None) -> int:
        expected = 0
        ooo: Dict[int, bytes] = {}
        total    = 0
        last_pkt = time.monotonic()
        cnt_ack  = 0
        last_ack = time.monotonic()
        sock     = self.sock
        win      = UDP_WINDOW_SIZE
        write_buf = bytearray()
        _FLUSH   = 1024 * 1024  # flush every 1 MB

        while True:
            now = time.monotonic()
            if now - last_pkt > 60.0: break

            r, _, _ = select.select([sock], [], [], 0.05)
            if not r:
                if now - last_ack > 0.05 and self.dest_addr:
                    self._send(self._pack(expected, _ACK_TYPE), self.dest_addr)
                    last_ack = now
                continue

            while True:
                try: pkt, addr = sock.recvfrom(65536)
                except: break

                if self.dest_addr is None: self.dest_addr = addr
                elif addr != self.dest_addr: continue

                s, t = self._unpack_hdr(pkt)
                last_pkt = time.monotonic()

                if t == _CMD_TYPE: continue
                if t == _FIN_TYPE:
                    if write_buf:
                        writer.write(bytes(write_buf)); write_buf.clear()
                    ack = self._pack(s + 1, _ACK_TYPE)
                    for _ in range(3): self._send(ack, addr)
                    return total
                if t != _DATA_TYPE: continue

                data = pkt[_HDR_SIZE:]

                if s == expected:
                    write_buf.extend(data); total += len(data)
                    expected += 1; cnt_ack += 1
                    while expected in ooo:
                        d = ooo.pop(expected)
                        write_buf.extend(d); total += len(d)
                        expected += 1; cnt_ack += 1
                    if len(write_buf) >= _FLUSH:
                        writer.write(bytes(write_buf)); write_buf.clear()
                    if progress_callback: progress_callback(total)
                elif s > expected and s < expected + win * 4:
                    ooo[s] = data
                    cnt_ack = _ACK_EVERY

                if cnt_ack >= _ACK_EVERY or time.monotonic() - last_ack > 0.01:
                    self._send(self._pack(expected, _ACK_TYPE), addr)
                    cnt_ack = 0; last_ack = time.monotonic()

                r2, _, _ = select.select([sock], [], [], 0)
                if not r2: break

            # Periodic ACK even if not at threshold
            if cnt_ack > 0 and time.monotonic() - last_ack > 0.005:
                self._send(self._pack(expected, _ACK_TYPE),
                           self.dest_addr if self.dest_addr else addr)
                cnt_ack = 0; last_ack = time.monotonic()

        if write_buf:
            writer.write(bytes(write_buf))
        return total
