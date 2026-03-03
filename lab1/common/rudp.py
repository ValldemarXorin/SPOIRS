"""
Reliable UDP — sliding window с congestion control.

Ключевое: адаптивное окно (slow start + AIMD).
  • cwnd начинается с 4, растёт экспоненциально (slow start)
    до ssthresh, потом линейно (congestion avoidance).
  • При потере (timeout): ssthresh = cwnd/2, cwnd = 4.
  • При 3 duplicate ACK: fast retransmit без ожидания timeout.
  • Пакеты 1400 байт — без IP-фрагментации.
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

_MAX_BURST  = 64     # максимум пакетов за 1 итерацию отправки
_ACK_EVERY  = 4      # клиент шлёт ACK каждые N пакетов


class RUDPSocket:

    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock      = sock
        self.dest_addr = dest_addr
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try:
                self.sock.setsockopt(socket.SOL_SOCKET, opt, 8 * 1024 * 1024)
            except OSError:
                pass

    # ── helpers ────────────────────────────────────────────

    def _pack(self, seq: int, ptype: int, data: bytes = b"") -> bytes:
        return struct.pack("!IB", seq, ptype) + data

    def _unpack(self, pkt: bytes) -> Tuple[int, int, bytes]:
        if len(pkt) < UDP_HEADER_SIZE:
            return -1, -1, b""
        s, t = struct.unpack("!IB", pkt[:UDP_HEADER_SIZE])
        return s, t, pkt[UDP_HEADER_SIZE:]

    def _send(self, data: bytes, addr: tuple) -> bool:
        for _ in range(10):
            try:
                self.sock.sendto(data, addr)
                return True
            except BlockingIOError:
                time.sleep(0.0001)
            except OSError as e:
                if getattr(e, "errno", None) in (11, 10035):
                    time.sleep(0.0001); continue
                return False
        return False

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
                if not r: continue
                try:
                    rp, addr = self.sock.recvfrom(65536)
                except (BlockingIOError, OSError):
                    break
                if self.dest_addr and addr != self.dest_addr:
                    continue
                _, rt, rd = self._unpack(rp)
                if rt != PacketType.CMD.value:
                    continue
                dec = rd.decode(errors="ignore")
                if dec == "ACK_CMD":
                    ack_received = True; continue
                return dec
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

    # ── send_stream (с congestion control) ─────────────────

    def send_stream(
        self, reader, total_size: int,
        progress_callback: Callable[[int], None] = None,
    ) -> None:
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")
        addr = self.dest_addr

        base     = 0
        next_seq = 0
        packets: Dict[int, bytes] = {}
        cursor   = 0
        eof      = False

        # Congestion control
        cwnd     = 4.0        # начальное окно (slow start)
        ssthresh = 256.0      # порог перехода из slow start в CA
        dup_ack_count = 0
        last_ack_seq  = 0

        last_ack_time = time.monotonic()
        rto           = 0.3   # начальный retransmit timeout

        while cursor < total_size or base < next_seq:
            # Эффективное окно: min(cwnd, UDP_WINDOW_SIZE)
            effective_win = int(min(cwnd, UDP_WINDOW_SIZE))

            # 1. send new packets
            burst = min(_MAX_BURST, max(1, effective_win - (next_seq - base)))
            n = 0
            while not eof and next_seq < base + effective_win and n < burst:
                chunk = reader.read(UDP_PAYLOAD_SIZE)
                if not chunk:
                    eof = True; cursor = total_size; break
                pkt = self._pack(next_seq, PacketType.DATA.value, chunk)
                packets[next_seq] = pkt
                self._send(pkt, addr)
                next_seq += 1; cursor += len(chunk); n += 1

            if progress_callback:
                progress_callback(cursor)

            # 2. read ACKs (nonblocking drain)
            got_new_ack = False
            while True:
                r, _, _ = select.select([self.sock], [], [], 0)
                if not r: break
                try:
                    ap, aa = self.sock.recvfrom(256)
                except (BlockingIOError, OSError):
                    break
                if self.dest_addr and aa != self.dest_addr:
                    continue
                aseq, apt, _ = self._unpack(ap)
                if apt != PacketType.ACK.value:
                    continue

                if aseq > base:
                    # New ACK — advance window
                    acked = aseq - base
                    for i in range(base, min(aseq, next_seq)):
                        packets.pop(i, None)
                    base          = aseq
                    got_new_ack   = True
                    last_ack_time = time.monotonic()
                    dup_ack_count = 0
                    last_ack_seq  = aseq

                    # Congestion control: grow window
                    if cwnd < ssthresh:
                        # Slow start: +1 за каждый ACK
                        cwnd += acked
                    else:
                        # Congestion avoidance: +1/cwnd за каждый ACK
                        cwnd += acked / cwnd

                elif aseq == base and aseq == last_ack_seq:
                    # Duplicate ACK
                    dup_ack_count += 1
                    if dup_ack_count >= 3:
                        # Fast retransmit
                        pkt = packets.get(base)
                        if pkt:
                            self._send(pkt, addr)
                        # Fast recovery
                        ssthresh = max(cwnd / 2, 4)
                        cwnd     = ssthresh + 3
                        dup_ack_count = 0
                        last_ack_time = time.monotonic()

            # 3. window full or no new data — short wait for ACK
            if not got_new_ack and (next_seq >= base + effective_win or eof) and base < next_seq:
                r, _, _ = select.select([self.sock], [], [], 0.002)
                if r:
                    continue  # go back to drain loop

            # 4. timeout retransmit
            now = time.monotonic()
            if now - last_ack_time > rto and packets:
                # Timeout → congestion event
                ssthresh = max(cwnd / 2, 4)
                cwnd     = 4  # reset to slow start
                cnt = 0
                for s in range(base, next_seq):
                    p = packets.get(s)
                    if p:
                        self._send(p, addr); cnt += 1
                        if cnt >= int(cwnd):
                            break
                last_ack_time = now
                # Increase RTO (exponential backoff, capped)
                rto = min(rto * 1.5, 3.0)
            elif got_new_ack:
                # Successful ACK → decrease RTO
                rto = max(0.1, rto * 0.9)

        # FIN
        fin = self._pack(next_seq, PacketType.FIN.value)
        for _ in range(25):
            self._send(fin, addr)
            r, _, _ = select.select([self.sock], [], [], 0.2)
            if not r: continue
            try:
                ap, aa = self.sock.recvfrom(256)
            except (BlockingIOError, OSError):
                continue
            if aa != addr: continue
            aseq, apt, _ = self._unpack(ap)
            if apt == PacketType.ACK.value and aseq == next_seq + 1:
                break

    # ── recv_stream ────────────────────────────────────────

    def recv_stream(
        self, writer, total_size: int = 0,
        progress_callback: Callable[[int], None] = None,
    ) -> int:
        expected  = 0
        ooo: Dict[int, bytes] = {}   # out-of-order buffer
        total     = 0
        last_pkt  = time.monotonic()
        cnt_ack   = 0
        last_ack  = time.monotonic()

        while True:
            now = time.monotonic()
            if now - last_pkt > 60.0:
                break

            r, _, _ = select.select([self.sock], [], [], 0.05)
            if not r:
                # Periodic ACK so sender doesn't stall
                if now - last_ack > 0.02 and self.dest_addr:
                    self._send(self._pack(expected, PacketType.ACK.value), self.dest_addr)
                    last_ack = now
                continue

            # Drain all available packets
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
                    ack = self._pack(seq + 1, PacketType.ACK.value)
                    for _ in range(3):
                        self._send(ack, addr)
                    return total
                if pt != PacketType.DATA.value:
                    continue

                if seq == expected:
                    writer.write(data); total += len(data)
                    expected += 1; cnt_ack += 1
                    while expected in ooo:
                        d = ooo.pop(expected)
                        writer.write(d); total += len(d)
                        expected += 1; cnt_ack += 1
                    if progress_callback:
                        progress_callback(total)
                elif seq > expected:
                    if seq < expected + UDP_WINDOW_SIZE * 2:
                        ooo.setdefault(seq, data)
                    cnt_ack = _ACK_EVERY  # force immediate ACK

                # ACK frequently
                t2 = time.monotonic()
                if cnt_ack >= _ACK_EVERY or t2 - last_ack > 0.01:
                    self._send(self._pack(expected, PacketType.ACK.value), addr)
                    cnt_ack = 0; last_ack = t2

                r2, _, _ = select.select([self.sock], [], [], 0)
                if not r2:
                    break

        return total
