"""Reliable UDP с подтверждениями, повторной передачей и скользящим окном.

Оптимизирован для максимальной пропускной способности в LAN.
Пакет 8KB, окно 4096, кумулятивные ACK + selective NACK.

Ключевые механизмы:
1. Подтверждение передачи: кумулятивные ACK (подтверждают все пакеты до номера)
2. Повторная передача: по таймауту и по NACK (selective retransmit)
3. Скользящее окно: sender не ждёт ACK на каждый пакет, шлёт до window_size пакетов

Кроссплатформенный: Windows + Linux.
"""

import socket
import struct
import time
import select
import sys
from typing import Optional, Tuple, Dict, Callable, List

from .protocol import (
    UDP_PAYLOAD_SIZE,
    UDP_HEADER_SIZE,
    UDP_WINDOW_SIZE,
    UDP_TIMEOUT,
    PacketType,
    UDP_RETRY_LIMIT,
    UDP_ACK_INTERVAL,
    UDP_BURST_SIZE,
)

# Header: 4 bytes sequence number + 1 byte packet type = 5 bytes
_HDR = struct.Struct("!IB")
_FLUSH_SIZE = 512 * 1024  # flush write buffer every 512KB

# Connection timeout — if no data for this long, consider connection dead
_CONNECTION_TIMEOUT = 30.0
_FIN_RETRIES = 30
_FIN_TIMEOUT = 0.3


class ConnectionLostError(Exception):
    """Raised when connection is lost (timeout, firewall DROP/REJECT, etc.)."""
    pass


class RUDPSocket:
    """Reliable UDP socket with sliding window, ACK, retransmission."""

    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock = sock
        self.dest_addr = dest_addr

        # Enlarge OS socket buffers for throughput
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try:
                self.sock.setsockopt(socket.SOL_SOCKET, opt, 16 * 1024 * 1024)
            except OSError:
                pass

    # ── low-level helpers ─────────────────────────────────

    def _pack(self, seq: int, ptype: int, data: bytes = b"") -> bytes:
        return _HDR.pack(seq, ptype) + data

    def _unpack(self, pkt: bytes):
        if len(pkt) < UDP_HEADER_SIZE:
            return -1, -1, b""
        s, t = _HDR.unpack_from(pkt)
        return s, t, pkt[UDP_HEADER_SIZE:]

    def _send(self, data: bytes, addr: Tuple[str, int]) -> bool:
        """Send a single UDP datagram with retry on transient errors."""
        for attempt in range(4):
            try:
                self.sock.sendto(data, addr)
                return True
            except BlockingIOError:
                # Socket send buffer full — brief pause
                time.sleep(0.0001)
            except InterruptedError:
                continue
            except ConnectionRefusedError:
                # REJECT rule — remote port unreachable
                raise ConnectionLostError("Connection refused (REJECT)")
            except OSError as e:
                # Check for common "connection reset" errors
                err_str = str(e).lower()
                if "forcibly closed" in err_str or "reset" in err_str:
                    raise ConnectionLostError(f"Connection reset: {e}")
                if attempt == 3:
                    return False
        return False

    def _drain_recv(self) -> List[Tuple[bytes, Tuple[str, int]]]:
        """Read all immediately available datagrams."""
        packets = []
        while True:
            r, _, _ = select.select([self.sock], [], [], 0)
            if not r:
                break
            try:
                pkt, addr = self.sock.recvfrom(65536)
                packets.append((pkt, addr))
            except (BlockingIOError, OSError):
                break
        return packets

    # ── командный канал (reliable command exchange) ────────

    def send_command(self, text: str) -> Optional[str]:
        """Send a command and wait for response. Reliable with retries."""
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")

        addr = self.dest_addr
        pkt = self._pack(0, PacketType.CMD.value, text.encode())

        for attempt in range(UDP_RETRY_LIMIT):
            try:
                self._send(pkt, addr)
            except ConnectionLostError as e:
                print(f"Connection lost sending command: {e}")
                return None

            t0 = time.monotonic()
            while time.monotonic() - t0 < 0.5:
                r, _, _ = select.select([self.sock], [], [], 0.05)
                if not r:
                    continue
                try:
                    rp, ra = self.sock.recvfrom(65536)
                except OSError:
                    break

                if ra != addr or len(rp) < UDP_HEADER_SIZE:
                    continue

                s, t = _HDR.unpack_from(rp)
                if t != PacketType.CMD.value:
                    continue

                msg = rp[UDP_HEADER_SIZE:].decode(errors="ignore")
                if msg == "ACK_CMD":
                    continue  # intermediate ACK, wait for real response
                return msg

        print(f"Command timed out after {UDP_RETRY_LIMIT} retries")
        return None

    # ══════════════════════════════════════════════════════
    #  SEND STREAM — агрессивная отправка со скользящим окном
    # ══════════════════════════════════════════════════════

    def send_stream(self, reader, total_size: int,
                    progress_callback: Callable[[int], None] = None) -> None:
        """
        Отправка потока данных с reliable UDP.

        Механизмы:
        - Скользящее окно размером UDP_WINDOW_SIZE пакетов
        - Кумулятивные ACK: получатель подтверждает все пакеты до номера
        - Повторная передача по таймауту (UDP_TIMEOUT)
        - Selective retransmit по NACK
        - Burst sending: отправка до UDP_BURST_SIZE пакетов за итерацию
        """
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")

        addr = self.dest_addr
        sock = self.sock
        sendto = sock.sendto

        # Sliding window state
        base = 0          # oldest unacknowledged packet
        next_seq = 0      # next sequence number to assign
        packets: Dict[int, bytes] = {}  # seq -> full packet (for retransmit)
        cursor = 0        # bytes read from file so far
        eof = False
        last_ack_time = time.monotonic()
        last_prog = 0
        retransmit_count = 0
        max_retransmits = total_size // UDP_PAYLOAD_SIZE * 3 + 1000

        try:
            while cursor < total_size or base < next_seq:
                # ── 1. Fill window: read data & send new packets ──
                can_send = min(UDP_BURST_SIZE, base + UDP_WINDOW_SIZE - next_seq)
                sent_new = 0

                while not eof and sent_new < can_send:
                    chunk = reader.read(UDP_PAYLOAD_SIZE)
                    if not chunk:
                        eof = True
                        cursor = total_size
                        break

                    pkt = self._pack(next_seq, PacketType.DATA.value, chunk)
                    packets[next_seq] = pkt

                    try:
                        sendto(pkt, addr)
                    except BlockingIOError:
                        time.sleep(0.00005)
                        try:
                            sendto(pkt, addr)
                        except OSError:
                            pass
                    except ConnectionRefusedError:
                        raise ConnectionLostError("Connection refused")
                    except OSError:
                        pass

                    next_seq += 1
                    cursor += len(chunk)
                    sent_new += 1

                # Progress
                if progress_callback and cursor - last_prog > max(total_size // 200, 1):
                    progress_callback(min(cursor, total_size))
                    last_prog = cursor

                # ── 2. Process incoming ACKs ──
                moved = False
                for raw_pkt, _ in self._drain_recv():
                    if len(raw_pkt) < UDP_HEADER_SIZE:
                        continue
                    s, t = _HDR.unpack_from(raw_pkt)

                    if t == PacketType.ACK.value:
                        # Cumulative ACK: s = next expected by receiver
                        if s > base:
                            for k in range(base, s):
                                packets.pop(k, None)
                            base = s
                            last_ack_time = time.monotonic()
                            moved = True

                    elif t == PacketType.NACK.value:
                        # Selective NACK: retransmit specific packet
                        if s in packets:
                            try:
                                sendto(packets[s], addr)
                                retransmit_count += 1
                            except OSError:
                                pass

                if moved:
                    continue

                # ── 3. Timeout retransmission ──
                now = time.monotonic()
                if packets and now - last_ack_time > UDP_TIMEOUT:
                    # Check connection liveness
                    if now - last_ack_time > _CONNECTION_TIMEOUT:
                        raise ConnectionLostError(
                            f"No ACK for {_CONNECTION_TIMEOUT}s — connection lost"
                        )

                    # Retransmit unacked packets (oldest first, limited batch)
                    cnt = 0
                    for k in sorted(packets.keys()):
                        try:
                            sendto(packets[k], addr)
                            retransmit_count += 1
                        except OSError:
                            pass
                        cnt += 1
                        if cnt >= 512:
                            break
                    last_ack_time = now

                    if retransmit_count > max_retransmits:
                        raise ConnectionLostError("Too many retransmissions")
                else:
                    # Brief yield to avoid busy-wait
                    if not moved and not sent_new:
                        time.sleep(0.00005)

        except ConnectionLostError:
            raise
        except Exception as e:
            raise ConnectionLostError(f"Send error: {e}")

        # ── FIN handshake ──
        fin_seq = next_seq
        fin_pkt = self._pack(fin_seq, PacketType.FIN.value)
        for attempt in range(_FIN_RETRIES):
            try:
                self._send(fin_pkt, addr)
            except ConnectionLostError:
                break

            r, _, _ = select.select([sock], [], [], _FIN_TIMEOUT)
            if not r:
                continue
            try:
                ap, _ = sock.recvfrom(64)
            except OSError:
                continue
            if len(ap) < UDP_HEADER_SIZE:
                continue
            s, t = _HDR.unpack_from(ap)
            if t == PacketType.ACK.value and s == fin_seq + 1:
                break

    # ══════════════════════════════════════════════════════
    #  RECV STREAM — быстрый приём с out-of-order буфером
    # ══════════════════════════════════════════════════════

    def recv_stream(self, writer, total_size: int = 0,
                    progress_callback: Callable[[int], None] = None) -> int:
        """
        Приём потока данных.

        Механизмы:
        - Out-of-order буфер для пакетов, пришедших не по порядку
        - Кумулятивные ACK раз в UDP_ACK_INTERVAL пакетов
        - Periodic ACK при простое (не реже чем раз в 10ms)
        - NACK при обнаружении пропуска
        - Буферизация записи для снижения дисковых операций
        """
        sock = self.sock

        expected = 0            # next expected sequence number
        ooo: Dict[int, bytes] = {}  # out-of-order buffer
        total_received = 0
        last_pkt_time = time.monotonic()
        last_ack_time = time.monotonic()
        ack_counter = 0
        write_buf = bytearray()
        last_nack_seq = -1      # avoid spamming NACK for same seq

        while True:
            now = time.monotonic()

            # Connection timeout detection
            if now - last_pkt_time > _CONNECTION_TIMEOUT:
                print(f"\nConnection timeout ({_CONNECTION_TIMEOUT}s no data)")
                break

            # Wait for data
            r, _, _ = select.select([sock], [], [], 0.01)

            if not r:
                # No data — send periodic ACK so sender doesn't retransmit
                if now - last_ack_time > 0.01 and self.dest_addr is not None:
                    try:
                        self._send(
                            _HDR.pack(expected, PacketType.ACK.value),
                            self.dest_addr
                        )
                    except ConnectionLostError:
                        break
                    last_ack_time = now
                continue

            # Read all available packets
            try:
                pkt, addr = sock.recvfrom(65536)
            except ConnectionRefusedError:
                print("\nConnection refused (REJECT)")
                break
            except OSError:
                continue

            if self.dest_addr is None:
                self.dest_addr = addr
            elif addr != self.dest_addr:
                continue

            if len(pkt) < UDP_HEADER_SIZE:
                continue

            seq, ptype = _HDR.unpack_from(pkt)
            last_pkt_time = time.monotonic()

            # Skip commands during stream
            if ptype == PacketType.CMD.value:
                continue

            # ── FIN received ──
            if ptype == PacketType.FIN.value:
                # Flush remaining write buffer
                if write_buf:
                    writer.write(bytes(write_buf))
                    write_buf.clear()
                # Send multiple FIN-ACKs for reliability
                fin_ack = _HDR.pack(seq + 1, PacketType.ACK.value)
                for _ in range(5):
                    try:
                        self._send(fin_ack, addr)
                    except ConnectionLostError:
                        pass
                return total_received

            if ptype != PacketType.DATA.value:
                continue

            data = pkt[UDP_HEADER_SIZE:]

            # ── In-order packet ──
            if seq == expected:
                write_buf.extend(data)
                total_received += len(data)
                expected += 1
                ack_counter += 1

                # Drain out-of-order buffer
                while expected in ooo:
                    d = ooo.pop(expected)
                    write_buf.extend(d)
                    total_received += len(d)
                    expected += 1
                    ack_counter += 1

                # Flush write buffer periodically
                if len(write_buf) >= _FLUSH_SIZE:
                    writer.write(bytes(write_buf))
                    write_buf.clear()

                if progress_callback:
                    progress_callback(total_received)

            # ── Out-of-order packet (future) ──
            elif seq > expected:
                if seq < expected + UDP_WINDOW_SIZE * 4:
                    ooo.setdefault(seq, data)

                # Send NACK for the missing packet
                if expected != last_nack_seq:
                    try:
                        self._send(
                            _HDR.pack(expected, PacketType.NACK.value),
                            addr
                        )
                    except ConnectionLostError:
                        break
                    last_nack_seq = expected

                # Force immediate ACK
                ack_counter = UDP_ACK_INTERVAL

            # ── Duplicate (old) packet — just ACK ──
            # (seq < expected: already received, ignore data)

            # ── Send ACK ──
            if (ack_counter >= UDP_ACK_INTERVAL or
                    time.monotonic() - last_ack_time > 0.005):
                try:
                    self._send(
                        _HDR.pack(expected, PacketType.ACK.value),
                        addr
                    )
                except ConnectionLostError:
                    break
                ack_counter = 0
                last_ack_time = time.monotonic()

        # Flush on exit
        if write_buf:
            writer.write(bytes(write_buf))
        return total_received