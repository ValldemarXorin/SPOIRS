"""
Reliable UDP — fixed window, пакет 8KB.

Стратегия: фиксированное окно, без congestion control.
Окно = 256 пакетов × 8KB = 2 MB max in flight.
Burst = 64 пакетов за итерацию (чтобы не перегрузить буфер).
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

_BURST     = 64
_ACK_EVERY = 8


class RUDPSocket:

    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock      = sock
        self.dest_addr = dest_addr
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try: self.sock.setsockopt(socket.SOL_SOCKET, opt, 4 * 1024 * 1024)
            except OSError: pass

    def _pack(self, seq: int, ptype: int, data: bytes = b"") -> bytes:
        return struct.pack("!IB", seq, ptype) + data

    def _unpack(self, pkt: bytes) -> Tuple[int, int, bytes]:
        if len(pkt) < UDP_HEADER_SIZE: return -1, -1, b""
        s, t = struct.unpack("!IB", pkt[:UDP_HEADER_SIZE])
        return s, t, pkt[UDP_HEADER_SIZE:]

    def _send(self, data: bytes, addr: tuple) -> bool:
        for attempt in range(20):
            try:
                self.sock.sendto(data, addr); return True
            except BlockingIOError:
                time.sleep(0.0001 * (attempt + 1))
            except OSError as e:
                errno = getattr(e, "errno", None) or getattr(e, "winerror", None)
                if errno in (11, 10035, 35):  # EAGAIN / EWOULDBLOCK
                    time.sleep(0.0001 * (attempt + 1)); continue
                return False
        return False

    # ── CMD ────────────────────────────────────────────────

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
                except (BlockingIOError, OSError): break
                if self.dest_addr and ra != self.dest_addr: continue
                _, rt, rd = self._unpack(rp)
                if rt != PacketType.CMD.value: continue
                d = rd.decode(errors="ignore")
                if d == "ACK_CMD": ack = True; continue
                return d
        return None

    def recv_command(self) -> Tuple[str, Tuple[str, int]]:
        try: pkt, addr = self.sock.recvfrom(65536)
        except (BlockingIOError, socket.timeout, OSError): return "", ("", 0)
        seq, pt, data = self._unpack(pkt)
        if pt != PacketType.CMD.value: return "", addr
        msg = data.decode(errors="ignore")
        if msg.startswith("OK ") or msg.startswith("ERROR "): return "", addr
        self._send(self._pack(seq, PacketType.CMD.value, b"ACK_CMD"), addr)
        return msg, addr

    # ── send_stream (fixed window) ─────────────────────────

    def send_stream(self, reader, total_size: int,
                    progress_callback: Callable[[int], None] = None) -> None:
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")
        addr = self.dest_addr

        base     = 0
        next_seq = 0
        packets: Dict[int, bytes] = {}
        cursor   = 0
        eof      = False
        last_ack = time.monotonic()
        win      = UDP_WINDOW_SIZE

        while cursor < total_size or base < next_seq:

            # 1. Send new packets (burst-limited)
            n = 0
            while not eof and next_seq < base + win and n < _BURST:
                chunk = reader.read(UDP_PAYLOAD_SIZE)
                if not chunk:
                    eof = True; cursor = total_size; break
                pkt = self._pack(next_seq, PacketType.DATA.value, chunk)
                packets[next_seq] = pkt
                self._send(pkt, addr)
                next_seq += 1; cursor += len(chunk); n += 1

            if progress_callback and n > 0:
                progress_callback(cursor)

            # 2. Drain ACKs (non-blocking poll)
            got_new = False
            for _ in range(256):
                r, _, _ = select.select([self.sock], [], [], 0)
                if not r: break
                try: ap, aa = self.sock.recvfrom(512)
                except (BlockingIOError, OSError): break
                if self.dest_addr and aa != self.dest_addr: continue
                aseq, apt, _ = self._unpack(ap)
                if apt != PacketType.ACK.value: continue
                if aseq > base:
                    for i in range(base, min(aseq, next_seq)):
                        packets.pop(i, None)
                    base = aseq; got_new = True
                    last_ack = time.monotonic()

            # 3. If we sent new packets → loop immediately (no wait)
            if n > 0 or got_new:
                continue

            # 4. Window full, waiting for ACK — short wait
            if base < next_seq:
                r, _, _ = select.select([self.sock], [], [], 0.001)
                if r: continue  # go drain

                # Timeout check
                now = time.monotonic()
                if now - last_ack > UDP_TIMEOUT and packets:
                    cnt = 0
                    for s in range(base, next_seq):
                        p = packets.get(s)
                        if p:
                            self._send(p, addr); cnt += 1
                            if cnt >= _BURST: break
                    last_ack = now

        # FIN
        fin = self._pack(next_seq, PacketType.FIN.value)
        for _ in range(25):
            self._send(fin, addr)
            r, _, _ = select.select([self.sock], [], [], 0.2)
            if not r: continue
            try: ap, aa = self.sock.recvfrom(512)
            except: continue
            if aa != addr: continue
            aseq, apt, _ = self._unpack(ap)
            if apt == PacketType.ACK.value and aseq == next_seq + 1:
                break

    # ── recv_stream ────────────────────────────────────────

    def recv_stream(self, writer, total_size: int = 0,
                    progress_callback: Callable[[int], None] = None) -> int:
        expected = 0
        ooo: Dict[int, bytes] = {}
        total    = 0
        last_pkt = time.monotonic()
        cnt_ack  = 0
        last_ack = time.monotonic()

        while True:
            now = time.monotonic()
            if now - last_pkt > 60.0: break

            r, _, _ = select.select([self.sock], [], [], 0.05)
            if not r:
                if now - last_ack > 0.05 and self.dest_addr:
                    self._send(self._pack(expected, PacketType.ACK.value),
                               self.dest_addr)
                    last_ack = now
                continue

            while True:
                try: pkt, addr = self.sock.recvfrom(65536)
                except (BlockingIOError, OSError): break

                if self.dest_addr is None: self.dest_addr = addr
                elif addr != self.dest_addr: continue

                seq, pt, data = self._unpack(pkt)
                last_pkt = time.monotonic()

                if pt == PacketType.CMD.value: continue
                if pt == PacketType.FIN.value:
                    ack = self._pack(seq + 1, PacketType.ACK.value)
                    for _ in range(3): self._send(ack, addr)
                    return total
                if pt != PacketType.DATA.value: continue

                if seq == expected:
                    writer.write(data); total += len(data)
                    expected += 1; cnt_ack += 1
                    while expected in ooo:
                        d = ooo.pop(expected)
                        writer.write(d); total += len(d)
                        expected += 1; cnt_ack += 1
                    if progress_callback: progress_callback(total)
                elif seq > expected and seq < expected + win * 4:
                    ooo.setdefault(seq, data)
                    cnt_ack = _ACK_EVERY  # force ACK

                t2 = time.monotonic()
                if cnt_ack >= _ACK_EVERY or t2 - last_ack > 0.01:
                    self._send(self._pack(expected, PacketType.ACK.value), addr)
                    cnt_ack = 0; last_ack = t2

                r2, _, _ = select.select([self.sock], [], [], 0)
                if not r2: break

        return total
