"""
Reliable UDP — скользящее окно.

• Никаких setblocking / settimeout — только select + nonblocking.
• send_stream: непрерывный pipeline, drain ACK после каждого burst.
• recv_stream: читаем всё за один проход, ACK каждые 4 пакета / 10 мс.
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

_SEND_BURST = 256
_ACK_EVERY  = 4


class RUDPSocket:

    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock      = sock
        self.dest_addr = dest_addr
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try:
                self.sock.setsockopt(socket.SOL_SOCKET, opt, 64 * 1024 * 1024)
            except OSError:
                pass

    # ── helpers ────────────────────────────────────────────

    def _pack(self, seq: int, ptype: int, data: bytes) -> bytes:
        return struct.pack("!IB", seq, ptype) + data

    def _unpack(self, pkt: bytes) -> Tuple[int, int, bytes]:
        if len(pkt) < UDP_HEADER_SIZE:
            return -1, -1, b""
        seq, pt = struct.unpack("!IB", pkt[:UDP_HEADER_SIZE])
        return seq, pt, pkt[UDP_HEADER_SIZE:]

    def _send(self, data: bytes, addr: tuple) -> bool:
        for _ in range(20):
            try:
                self.sock.sendto(data, addr)
                return True
            except BlockingIOError:
                time.sleep(0.0001)
            except OSError as e:
                if getattr(e, "errno", None) in (11, 10035):
                    time.sleep(0.0001)
                    continue
                return False
        return False

    def _drain_acks(self, base: int, packets: Dict[int, bytes],
                    next_seq: int) -> Tuple[int, float]:
        last_t = -1.0
        while True:
            r, _, _ = select.select([self.sock], [], [], 0)
            if not r:
                break
            try:
                pkt, addr = self.sock.recvfrom(256)
            except (BlockingIOError, OSError):
                break
            if self.dest_addr and addr != self.dest_addr:
                continue
            seq, pt, _ = self._unpack(pkt)
            if pt == PacketType.ACK.value and seq > base:
                ack = min(seq, next_seq)
                for i in range(base, ack):
                    packets.pop(i, None)
                base   = ack
                last_t = time.monotonic()
        return base, last_t

    # ── CMD ────────────────────────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
        pkt = self._pack(0, PacketType.CMD.value, text.encode())
        ack_received = False
        for _ in range(UDP_RETRY_LIMIT):
            if not ack_received and self.dest_addr:
                self._send(pkt, self.dest_addr)
            wait = 1.0 if ack_received else 0.3
            t0   = time.monotonic()
            while time.monotonic() - t0 < wait:
                r, _, _ = select.select([self.sock], [], [], 0.05)
                if not r:
                    continue
                try:
                    rp, addr = self.sock.recvfrom(65536)
                except (BlockingIOError, OSError):
                    break
                if self.dest_addr and addr != self.dest_addr:
                    continue
                _, rt, rd = self._unpack(rp)
                if rt != PacketType.CMD.value:
                    continue
                decoded = rd.decode(errors="ignore")
                if decoded == "ACK_CMD":
                    ack_received = True
                    continue
                return decoded
        return None

    def recv_command(self) -> Tuple[str, Tuple[str, int]]:
        try:
            pkt, addr = self.sock.recvfrom(65536)
        except (BlockingIOError, socket.timeout, OSError):
            return "", ("", 0)
        seq, pt, data = self._unpack(pkt)
        if pt != PacketType.CMD.value:
            return "", addr
        msg = data.decode(errors="ignore")
        if msg.startswith("OK ") or msg.startswith("ERROR "):
            return "", addr
        self._send(self._pack(seq, PacketType.CMD.value, b"ACK_CMD"), addr)
        return msg, addr

    # ── send_stream ────────────────────────────────────────

    def send_stream(
        self, reader, total_size: int,
        progress_callback: Callable[[int], None] = None,
    ) -> None:
        base     = 0
        next_seq = 0
        packets: Dict[int, bytes] = {}
        cursor   = 0
        last_ack = time.monotonic()
        eof      = False

        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")
        addr = self.dest_addr

        while cursor < total_size or base < next_seq:
            # 1. send burst
            n = 0
            while not eof and next_seq < base + UDP_WINDOW_SIZE and n < _SEND_BURST:
                chunk = reader.read(UDP_PAYLOAD_SIZE)
                if not chunk:
                    eof = True; cursor = total_size; break
                pkt = self._pack(next_seq, PacketType.DATA.value, chunk)
                packets[next_seq] = pkt
                self._send(pkt, addr)
                next_seq += 1; cursor += len(chunk); n += 1
            if progress_callback:
                progress_callback(cursor)

            # 2. drain ACK
            nb, t = self._drain_acks(base, packets, next_seq)
            if nb > base:
                base = nb; last_ack = t if t > 0 else time.monotonic()

            # 3. window full → short wait
            if next_seq >= base + UDP_WINDOW_SIZE and base < next_seq:
                r, _, _ = select.select([self.sock], [], [], 0.001)
                if r:
                    nb, t = self._drain_acks(base, packets, next_seq)
                    if nb > base:
                        base = nb; last_ack = t if t > 0 else time.monotonic()

            # 4. retransmit
            now = time.monotonic()
            if now - last_ack > UDP_TIMEOUT and packets:
                cnt = 0
                for s in range(base, next_seq):
                    p = packets.get(s)
                    if p:
                        self._send(p, addr); cnt += 1
                        if cnt >= _SEND_BURST:
                            break
                last_ack = now

        # FIN
        fin = self._pack(next_seq, PacketType.FIN.value, b"")
        for _ in range(25):
            self._send(fin, addr)
            r, _, _ = select.select([self.sock], [], [], 0.2)
            if not r:
                continue
            try:
                ap, adr = self.sock.recvfrom(256)
            except (BlockingIOError, OSError):
                continue
            if adr != addr:
                continue
            aseq, apt, _ = self._unpack(ap)
            if apt == PacketType.ACK.value and aseq == next_seq + 1:
                break

    # ── recv_stream ────────────────────────────────────────

    def recv_stream(
        self, writer, total_size: int = 0,
        progress_callback: Callable[[int], None] = None,
    ) -> int:
        expected  = 0
        buf: Dict[int, bytes] = {}
        total     = 0
        last_pkt  = time.monotonic()
        cnt_ack   = 0
        last_ack  = time.monotonic()
        DEAD      = 60.0

        while True:
            now = time.monotonic()
            if now - last_pkt > DEAD:
                break
            r, _, _ = select.select([self.sock], [], [], 0.05)
            if not r:
                if now - last_ack > 0.01 and self.dest_addr:
                    self._send(self._pack(expected, PacketType.ACK.value, b""),
                               self.dest_addr)
                    last_ack = now
                continue

            while True:
                try:
                    pkt, addr = self.sock.recvfrom(65536)
                except (BlockingIOError, OSError):
                    break
                if self.dest_addr is None:
                    self.dest_addr = addr
                elif addr != self.dest_addr:
                    continue
                seq, pt, data = self._unpack(pkt)
                last_pkt = time.monotonic()

                if pt == PacketType.CMD.value:
                    continue
                if pt == PacketType.FIN.value:
                    ack = self._pack(seq + 1, PacketType.ACK.value, b"")
                    for _ in range(3):
                        self._send(ack, addr)
                    return total
                if pt != PacketType.DATA.value:
                    continue

                if seq == expected:
                    writer.write(data); total += len(data)
                    expected += 1; cnt_ack += 1
                    while expected in buf:
                        d = buf.pop(expected)
                        writer.write(d); total += len(d)
                        expected += 1; cnt_ack += 1
                    if progress_callback:
                        progress_callback(total)
                elif seq > expected and seq < expected + UDP_WINDOW_SIZE:
                    buf[seq] = data
                    cnt_ack = _ACK_EVERY

                t2 = time.monotonic()
                if cnt_ack >= _ACK_EVERY or t2 - last_ack > 0.01:
                    self._send(self._pack(expected, PacketType.ACK.value, b""), addr)
                    cnt_ack = 0; last_ack = t2

                r2, _, _ = select.select([self.sock], [], [], 0)
                if not r2:
                    break
        return total
