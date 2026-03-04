"""Reliable UDP с адаптивным rate control.

Ключевые механизмы:
1. Подтверждение: кумулятивные ACK (всё до seq N получено)
2. Повторная передача: по таймауту + по NACK (selective retransmit)
3. Скользящее окно: до UDP_WINDOW_SIZE пакетов in flight
4. Адаптивный rate control: AIMD (additive increase, multiplicative decrease)
   - При получении ACK: увеличиваем cwnd
   - При потере (таймаут/NACK): уменьшаем cwnd вдвое
5. Pacing: контролируем межпакетный интервал для предотвращения burst-потерь

Кроссплатформенный: Windows + Linux.
"""

import socket
import struct
import time
import select
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

_HDR = struct.Struct("!IB")
_FLUSH_SIZE = 2 * 1024 * 1024   # flush write buffer every 2MB
_CONNECTION_TIMEOUT = 30.0
_FIN_RETRIES = 30
_FIN_TIMEOUT = 0.3

# Congestion control parameters
_INITIAL_CWND = 64              # начальное окно (пакетов)
_MIN_CWND = 8                   # минимальное окно
_SLOW_START_THRESH = 512        # порог перехода из slow start в congestion avoidance


class ConnectionLostError(Exception):
    pass


class RUDPSocket:
    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock = sock
        self.dest_addr = dest_addr
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try:
                self.sock.setsockopt(socket.SOL_SOCKET, opt, 32 * 1024 * 1024)
            except OSError:
                pass

    def _pack(self, seq: int, ptype: int, data: bytes = b"") -> bytes:
        return _HDR.pack(seq, ptype) + data

    def _unpack(self, pkt: bytes):
        if len(pkt) < UDP_HEADER_SIZE:
            return -1, -1, b""
        s, t = _HDR.unpack_from(pkt)
        return s, t, pkt[UDP_HEADER_SIZE:]

    def _send(self, data: bytes, addr: Tuple[str, int]) -> bool:
        for attempt in range(4):
            try:
                self.sock.sendto(data, addr)
                return True
            except BlockingIOError:
                time.sleep(0.0001)
            except InterruptedError:
                continue
            except ConnectionRefusedError:
                raise ConnectionLostError("Connection refused (REJECT)")
            except OSError as e:
                err_str = str(e).lower()
                if "forcibly closed" in err_str or "reset" in err_str:
                    raise ConnectionLostError(f"Connection reset: {e}")
                if attempt == 3:
                    return False
        return False

    def _drain_recv(self) -> List[Tuple[bytes, Tuple[str, int]]]:
        """Read all immediately available datagrams without blocking."""
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

    # ── командный канал ───────────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
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
                    continue
                return msg

        print(f"Command timed out after {UDP_RETRY_LIMIT} retries")
        return None

    # ══════════════════════════════════════════════════════
    #  SEND STREAM — скользящее окно с congestion control
    # ══════════════════════════════════════════════════════

    def send_stream(self, reader, total_size: int,
                    progress_callback: Callable[[int], None] = None) -> None:
        """Отправка с адаптивным congestion control.

        Реализует AIMD подобный TCP Reno:
        - Slow start: cwnd удваивается каждый RTT
        - Congestion avoidance: cwnd растёт линейно
        - При потере: cwnd /= 2, ssthresh = cwnd
        """
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")

        addr = self.dest_addr
        sock = self.sock
        sendto = sock.sendto

        # Sliding window state
        base = 0
        next_seq = 0
        packets: Dict[int, bytes] = {}
        cursor = 0
        eof = False
        last_ack_time = time.monotonic()
        last_prog = 0

        # Congestion control
        cwnd = _INITIAL_CWND        # congestion window (packets)
        ssthresh = _SLOW_START_THRESH
        dup_ack_count = 0
        last_ack_seq = 0
        rtt_estimate = 0.001        # start with 1ms estimate
        send_times: Dict[int, float] = {}  # seq -> send time for RTT

        # Pacing: inter-packet delay to avoid bursts
        pacing_interval = 0.0       # will be computed dynamically
        last_send_time = 0.0

        try:
            while cursor < total_size or base < next_seq:
                now = time.monotonic()

                # ── 1. Send new packets up to cwnd ──
                effective_window = min(int(cwnd), UDP_WINDOW_SIZE)
                in_flight = next_seq - base
                can_send = effective_window - in_flight

                if can_send > 0:
                    burst = min(can_send, UDP_BURST_SIZE)
                    sent_new = 0

                    while not eof and sent_new < burst:
                        # Pacing: wait between sends to avoid overwhelming receiver
                        if pacing_interval > 0:
                            elapsed = time.monotonic() - last_send_time
                            if elapsed < pacing_interval:
                                # Don't sleep, just break and process ACKs
                                break

                        chunk = reader.read(UDP_PAYLOAD_SIZE)
                        if not chunk:
                            eof = True
                            cursor = total_size
                            break

                        pkt = self._pack(next_seq, PacketType.DATA.value, chunk)
                        packets[next_seq] = pkt
                        send_times[next_seq] = time.monotonic()

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

                        last_send_time = time.monotonic()
                        next_seq += 1
                        cursor += len(chunk)
                        sent_new += 1

                # Progress
                if progress_callback and cursor - last_prog > max(total_size // 200, 1):
                    progress_callback(min(cursor, total_size))
                    last_prog = cursor

                # ── 2. Process incoming ACKs ──
                moved = False
                nack_received = False

                for raw_pkt, _ in self._drain_recv():
                    if len(raw_pkt) < UDP_HEADER_SIZE:
                        continue
                    s, t = _HDR.unpack_from(raw_pkt)

                    if t == PacketType.ACK.value:
                        if s > base:
                            acked_count = s - base

                            # RTT measurement
                            for seq_n in range(base, s):
                                if seq_n in send_times:
                                    sample_rtt = time.monotonic() - send_times[seq_n]
                                    rtt_estimate = 0.8 * rtt_estimate + 0.2 * sample_rtt
                                    del send_times[seq_n]

                            # Clean up
                            for k in range(base, s):
                                packets.pop(k, None)
                                send_times.pop(k, None)
                            base = s
                            last_ack_time = time.monotonic()
                            moved = True
                            dup_ack_count = 0
                            last_ack_seq = s

                            # Congestion control: increase window
                            if cwnd < ssthresh:
                                # Slow start: exponential growth
                                cwnd += acked_count
                            else:
                                # Congestion avoidance: linear growth
                                cwnd += acked_count / cwnd

                            cwnd = min(cwnd, UDP_WINDOW_SIZE)

                            # Update pacing based on RTT and cwnd
                            if rtt_estimate > 0 and cwnd > 0:
                                pacing_interval = rtt_estimate / cwnd * 0.5
                            else:
                                pacing_interval = 0.0

                        elif s == last_ack_seq:
                            # Duplicate ACK
                            dup_ack_count += 1
                            if dup_ack_count >= 3:
                                # Fast retransmit
                                if base in packets:
                                    try:
                                        sendto(packets[base], addr)
                                    except OSError:
                                        pass
                                # Multiplicative decrease
                                ssthresh = max(int(cwnd / 2), _MIN_CWND)
                                cwnd = ssthresh
                                dup_ack_count = 0

                    elif t == PacketType.NACK.value:
                        nack_received = True
                        if s in packets:
                            try:
                                sendto(packets[s], addr)
                            except OSError:
                                pass
                            # Mild decrease on NACK (not as aggressive as timeout)
                            cwnd = max(cwnd * 0.75, _MIN_CWND)
                            ssthresh = max(int(cwnd), _MIN_CWND)

                if moved:
                    continue

                # ── 3. Timeout retransmission ──
                now = time.monotonic()
                if packets and now - last_ack_time > max(UDP_TIMEOUT, rtt_estimate * 3):
                    if now - last_ack_time > _CONNECTION_TIMEOUT:
                        raise ConnectionLostError(
                            f"No ACK for {_CONNECTION_TIMEOUT}s"
                        )

                    # Timeout = severe congestion
                    ssthresh = max(int(cwnd / 2), _MIN_CWND)
                    cwnd = _MIN_CWND

                    # Retransmit from base
                    cnt = 0
                    for k in sorted(packets.keys()):
                        try:
                            sendto(packets[k], addr)
                            send_times[k] = time.monotonic()
                        except OSError:
                            pass
                        cnt += 1
                        if cnt >= int(cwnd):
                            break
                    last_ack_time = now
                else:
                    if not moved:
                        # Brief yield
                        time.sleep(0.00001)

        except ConnectionLostError:
            raise
        except Exception as e:
            raise ConnectionLostError(f"Send error: {e}")

        # ── FIN handshake ──
        fin_seq = next_seq
        fin_pkt = self._pack(fin_seq, PacketType.FIN.value)
        for _ in range(_FIN_RETRIES):
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
    #  RECV STREAM — быстрый приём
    # ══════════════════════════════════════════════════════

    def recv_stream(self, writer, total_size: int = 0,
                    progress_callback: Callable[[int], None] = None) -> int:
        """Приём с агрессивным ACK для максимальной скорости sender'а."""
        sock = self.sock

        expected = 0
        ooo: Dict[int, bytes] = {}
        total_received = 0
        last_pkt_time = time.monotonic()
        last_ack_time = 0.0
        ack_counter = 0
        write_buf = bytearray()
        last_nack_seq = -1
        pkts_since_ack = 0

        while True:
            now = time.monotonic()
            if now - last_pkt_time > _CONNECTION_TIMEOUT:
                print(f"\nConnection timeout ({_CONNECTION_TIMEOUT}s no data)")
                break

            # Read with short timeout for responsiveness
            r, _, _ = select.select([sock], [], [], 0.001)

            if not r:
                # Send periodic ACK even when idle
                if now - last_ack_time > 0.002 and self.dest_addr is not None:
                    try:
                        self._send(
                            _HDR.pack(expected, PacketType.ACK.value),
                            self.dest_addr
                        )
                    except ConnectionLostError:
                        break
                    last_ack_time = now
                continue

            # Read ALL available packets in tight loop
            batch_count = 0
            while batch_count < 4096:
                try:
                    pkt, addr = sock.recvfrom(65536)
                except (BlockingIOError, OSError):
                    break

                batch_count += 1

                if self.dest_addr is None:
                    self.dest_addr = addr
                elif addr != self.dest_addr:
                    continue

                if len(pkt) < UDP_HEADER_SIZE:
                    continue

                seq, ptype = _HDR.unpack_from(pkt)
                last_pkt_time = time.monotonic()

                if ptype == PacketType.CMD.value:
                    continue

                if ptype == PacketType.FIN.value:
                    if write_buf:
                        writer.write(bytes(write_buf))
                        write_buf.clear()
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

                if seq == expected:
                    write_buf.extend(data)
                    total_received += len(data)
                    expected += 1
                    pkts_since_ack += 1

                    # Drain out-of-order buffer
                    while expected in ooo:
                        d = ooo.pop(expected)
                        write_buf.extend(d)
                        total_received += len(d)
                        expected += 1
                        pkts_since_ack += 1

                    if progress_callback and pkts_since_ack % 32 == 0:
                        progress_callback(total_received)

                elif seq > expected:
                    if seq < expected + UDP_WINDOW_SIZE * 2:
                        ooo.setdefault(seq, data)

                    # Send NACK for the gap
                    if expected != last_nack_seq:
                        try:
                            self._send(
                                _HDR.pack(expected, PacketType.NACK.value),
                                addr
                            )
                        except ConnectionLostError:
                            break
                        last_nack_seq = expected
                    pkts_since_ack = UDP_ACK_INTERVAL  # force ACK

                # seq < expected: duplicate, ignore

            # Flush write buffer
            if len(write_buf) >= _FLUSH_SIZE:
                writer.write(bytes(write_buf))
                write_buf.clear()

            # Send ACK after processing batch
            if (pkts_since_ack >= UDP_ACK_INTERVAL or
                    time.monotonic() - last_ack_time > 0.002):
                try:
                    self._send(
                        _HDR.pack(expected, PacketType.ACK.value),
                        self.dest_addr
                    )
                except ConnectionLostError:
                    break
                pkts_since_ack = 0
                last_ack_time = time.monotonic()

        if write_buf:
            writer.write(bytes(write_buf))
        return total_received