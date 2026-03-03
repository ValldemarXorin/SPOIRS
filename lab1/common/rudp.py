"""
Reliable UDP — скользящее окно (Go-Back-N / selective-ACK).

Ключевые решения:
  • Никаких setblocking(True) / settimeout() — только select() + nonblocking.
  • send_stream: непрерывный пайплайн; ACK вычитываются после каждого
    burst без каких-либо sleep-ов.
  • recv_stream: читаем ВСЁ доступное за один проход select; ACK каждые
    ACK_EVERY пакетов (по умолчанию 4) и каждые 10 мс.
  • Состояние сокета (blocking/timeout) НИКОГДА не меняется.
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

# Сколько пакетов отправляем за одну итерацию send_stream
_SEND_BURST = 256
# ACK каждые N корректных пакетов (recv_stream / server upload)
_ACK_EVERY  = 4


class RUDPSocket:
    """Обёртка над UDP-сокетом: надёжная передача потока."""

    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock      = sock
        self.dest_addr = dest_addr
        # Расширяем OS-буферы
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try:
                self.sock.setsockopt(socket.SOL_SOCKET, opt, 64 * 1024 * 1024)
            except OSError:
                pass

    # ── низкоуровневые helpers ─────────────────────────────

    def _pack(self, seq: int, ptype: int, data: bytes) -> bytes:
        return struct.pack("!IB", seq, ptype) + data

    def _unpack(self, pkt: bytes) -> Tuple[int, int, bytes]:
        if len(pkt) < UDP_HEADER_SIZE:
            return -1, -1, b""
        seq, pt = struct.unpack("!IB", pkt[:UDP_HEADER_SIZE])
        return seq, pt, pkt[UDP_HEADER_SIZE:]

    def _send(self, data: bytes, addr: tuple) -> bool:
        """Неблокирующая отправка с несколькими попытками при EAGAIN."""
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
        """
        Вычитывает ВСЕ накопившиеся ACK из сокета (неблокирующий).
        Возвращает (новый base, время последнего ACK или -1 если ничего).
        """
        last_ack_t = -1.0
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
                base      = ack
                last_ack_t = time.monotonic()
        return base, last_ack_t

    # ── CMD (handshake) ────────────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
        pkt          = self._pack(0, PacketType.CMD.value, text.encode())
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

    # ── send_stream (клиент → сервер) ─────────────────────

    def send_stream(
        self,
        reader,
        total_size: int,
        progress_callback: Callable[[int], None] = None,
    ) -> None:
        """
        Непрерывный pipeline:
          1. Отправляем burst новых пакетов (пока окно не заполнено)
          2. Вычитываем ВСЕ ACK (неблокирующий drain)
          3. Если окно заполнено — короткий poll 1 мс и повтор
          4. Retransmit при timeout
        """
        base         = 0
        next_seq     = 0
        packets: Dict[int, bytes] = {}
        cursor       = 0
        last_ack_t   = time.monotonic()
        eof          = False

        if self.dest_addr is None:
            raise RuntimeError("dest_addr not set")

        addr = self.dest_addr

        while cursor < total_size or base < next_seq:

            # ── 1. отправляем новые пакеты ──────────────
            sent_now = 0
            while (not eof
                   and next_seq < base + UDP_WINDOW_SIZE
                   and sent_now < _SEND_BURST):
                chunk = reader.read(UDP_PAYLOAD_SIZE)
                if not chunk:
                    eof    = True
                    cursor = total_size
                    break
                pkt = self._pack(next_seq, PacketType.DATA.value, chunk)
                packets[next_seq] = pkt
                self._send(pkt, addr)
                next_seq += 1
                cursor   += len(chunk)
                sent_now += 1

            if progress_callback:
                progress_callback(cursor)

            # ── 2. drain всех ACK ────────────────────────
            new_base, t = self._drain_acks(base, packets, next_seq)
            if new_base > base:
                base      = new_base
                last_ack_t = t if t > 0 else time.monotonic()

            # ── 3. окно заполнено → ждём ACK ────────────
            if next_seq >= base + UDP_WINDOW_SIZE and base < next_seq:
                r, _, _ = select.select([self.sock], [], [], 0.001)
                if r:
                    new_base, t = self._drain_acks(base, packets, next_seq)
                    if new_base > base:
                        base      = new_base
                        last_ack_t = t if t > 0 else time.monotonic()

            # ── 4. retransmit при timeout ────────────────
            now = time.monotonic()
            if now - last_ack_t > UDP_TIMEOUT and packets:
                resent = 0
                for s in range(base, next_seq):
                    p = packets.get(s)
                    if p:
                        self._send(p, addr)
                        resent += 1
                        if resent >= _SEND_BURST:
                            break
                last_ack_t = now

        # ── FIN handshake ────────────────────────────────
        fin_seq = next_seq
        fin_pkt = self._pack(fin_seq, PacketType.FIN.value, b"")
        for _ in range(25):
            self._send(fin_pkt, addr)
            r, _, _ = select.select([self.sock], [], [], 0.2)
            if not r:
                continue
            try:
                ap, adr = self.sock.recvfrom(256)
            except (BlockingIOError, OSError):
                continue
            if self.dest_addr and adr != addr:
                continue
            aseq, apt, _ = self._unpack(ap)
            if apt == PacketType.ACK.value and aseq == fin_seq + 1:
                break

    # ── recv_stream (клиент ← сервер) ─────────────────────

    def recv_stream(
        self,
        writer,
        total_size: int = 0,
        progress_callback: Callable[[int], None] = None,
    ) -> int:
        """
        Принимает поток данных.
        • Читаем ВСЁ доступное за один проход.
        • ACK каждые _ACK_EVERY пакетов ИЛИ каждые 10 мс.
        • select timeout = 50 мс (не 500 мс как раньше).
        """
        expected     = 0
        buf: Dict[int, bytes] = {}
        total        = 0
        last_pkt_t   = time.monotonic()
        DEAD         = 60.0

        cnt_since_ack = 0
        last_ack_t    = time.monotonic()

        while True:
            now = time.monotonic()
            if now - last_pkt_t > DEAD:
                print("RUDP recv_stream: timeout")
                break

            r, _, _ = select.select([self.sock], [], [], 0.05)

            # периодически подтверждаем, чтобы сервер не застрял
            if not r:
                if now - last_ack_t > 0.01 and self.dest_addr:
                    self._send(
                        self._pack(expected, PacketType.ACK.value, b""),
                        self.dest_addr,
                    )
                    last_ack_t = now
                continue

            # вычитываем ВСЕ доступные пакеты
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
                last_pkt_t = time.monotonic()

                if pt == PacketType.CMD.value:
                    continue  # остаток handshake

                if pt == PacketType.FIN.value:
                    ack = self._pack(seq + 1, PacketType.ACK.value, b"")
                    for _ in range(3):
                        self._send(ack, addr)
                    return total

                if pt != PacketType.DATA.value:
                    continue

                if seq == expected:
                    writer.write(data)
                    total    += len(data)
                    expected += 1
                    cnt_since_ack += 1

                    while expected in buf:
                        d = buf.pop(expected)
                        writer.write(d)
                        total    += len(d)
                        expected += 1
                        cnt_since_ack += 1

                    if progress_callback:
                        progress_callback(total)

                elif seq > expected:
                    if seq < expected + UDP_WINDOW_SIZE:
                        buf[seq] = data
                    cnt_since_ack = _ACK_EVERY  # форсируем ACK

                # отправляем ACK часто
                t2 = time.monotonic()
                if cnt_since_ack >= _ACK_EVERY or t2 - last_ack_t > 0.01:
                    self._send(
                        self._pack(expected, PacketType.ACK.value, b""),
                        addr,
                    )
                    cnt_since_ack = 0
                    last_ack_t    = t2

                # ещё пакеты в буфере ОС?
                r2, _, _ = select.select([self.sock], [], [], 0)
                if not r2:
                    break

        return total
