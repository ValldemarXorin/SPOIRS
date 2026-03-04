"""Reliable UDP — простой и быстрый, с поддержкой Windows/Linux.

Без сложного congestion control. Для LAN/localhost где потерь мало.

Механизмы:
1. Кумулятивные ACK — подтверждают все до seq N
2. Скользящее окно — до UDP_WINDOW_SIZE пакетов без ожидания
3. Повторная передача — по таймауту и по NACK
4. Receiver шлёт ACK каждые 32 пакета и каждые 2ms

Ключ к скорости:
- Sender НЕ блокируется пока окно не заполнено
- Receiver дренит буфер в tight loop (без select на каждый пакет)
- Нет sleep() в hot path
- Нет congestion window — только фиксированное скользящее окно
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
)

_HDR = struct.Struct("!IB")
_HDR_SIZE = _HDR.size
_FLUSH_SIZE = 1024 * 1024
_CONNECTION_TIMEOUT = 30.0
_FIN_RETRIES = 25
_FIN_TIMEOUT = 0.3

# Сколько пакетов отправлять за один burst перед проверкой ACK
_SEND_BURST = 256  # Уменьшено для лучшей совместимости с Windows


class ConnectionLostError(Exception):
    pass


class RUDPSocket:
    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock = sock
        self.dest_addr = dest_addr
        # Увеличиваем буферы для Windows
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            for size in [8 * 1024 * 1024, 4 * 1024 * 1024, 2 * 1024 * 1024, 1024 * 1024]:
                try:
                    self.sock.setsockopt(socket.SOL_SOCKET, opt, size)
                    break
                except OSError:
                    continue

        # Для Windows устанавливаем неблокирующий режим
        if sys.platform == "win32":
            self.sock.setblocking(False)

    def _pack(self, seq: int, ptype: int, data: bytes = b"") -> bytes:
        return _HDR.pack(seq, ptype) + data

    def _send_raw(self, data: bytes, addr: Tuple[str, int]) -> bool:
        for attempt in range(5):  # Увеличено количество попыток для Windows
            try:
                self.sock.sendto(data, addr)
                return True
            except BlockingIOError:
                # Буфер отправки полный — короткая пауза
                time.sleep(0.001)  # Увеличена пауза для Windows
            except InterruptedError:
                continue
            except ConnectionRefusedError:
                raise ConnectionLostError("Connection refused (REJECT)")
            except OSError as e:
                err = str(e).lower()
                if "forcibly closed" in err or "reset" in err or "10054" in str(e):
                    raise ConnectionLostError(f"Connection reset: {e}")
                # Для Windows игнорируем некоторые ошибки
                if sys.platform == "win32" and "10035" in str(e):  # WSAEWOULDBLOCK
                    time.sleep(0.001)
                    continue
                return False
        return False

    # ── командный канал ───────────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")
        addr = self.dest_addr
        pkt = self._pack(0, PacketType.CMD.value, text.encode())

        for _ in range(UDP_RETRY_LIMIT):
            try:
                self._send_raw(pkt, addr)
            except ConnectionLostError:
                return None

            t0 = time.monotonic()
            while time.monotonic() - t0 < 1.0:  # Увеличен таймаут для Windows
                r, _, _ = select.select([self.sock], [], [], 0.1)
                if not r:
                    continue
                try:
                    rp, ra = self.sock.recvfrom(65536)
                except (BlockingIOError, OSError):
                    break
                if ra != addr or len(rp) < _HDR_SIZE:
                    continue
                _, t = _HDR.unpack_from(rp)
                if t != PacketType.CMD.value:
                    continue
                msg = rp[_HDR_SIZE:].decode(errors="ignore")
                if msg == "ACK_CMD":
                    continue
                return msg
        return None

    # ══════════════════════════════════════════════════════
    #  SEND STREAM
    # ══════════════════════════════════════════════════════

    def send_stream(self, reader, total_size: int,
                    progress_callback: Callable[[int], None] = None) -> None:
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")

        addr = self.dest_addr
        sock = self.sock
        sendto = sock.sendto

        base = 0           # oldest unacked seq
        next_seq = 0        # next seq to assign
        packets: Dict[int, bytes] = {}  # seq -> raw packet
        cursor = 0          # bytes read from file
        eof = False
        last_ack_time = time.monotonic()
        last_prog = 0
        retransmit_count = 0

        try:
            while cursor < total_size or base < next_seq:
                # ── 1. Заполняем окно: шлём пакеты пока можно ──
                in_flight = next_seq - base
                can_send = UDP_WINDOW_SIZE - in_flight
                sent_count = 0

                while not eof and can_send > 0 and sent_count < _SEND_BURST:
                    chunk = reader.read(UDP_PAYLOAD_SIZE)
                    if not chunk:
                        eof = True
                        cursor = total_size
                        break

                    pkt = _HDR.pack(next_seq, PacketType.DATA.value) + chunk
                    packets[next_seq] = pkt

                    try:
                        sendto(pkt, addr)
                    except (BlockingIOError, OSError) as e:
                        if sys.platform == "win32" and "10035" in str(e):
                            # WSAEWOULDBLOCK - буфер полный
                            time.sleep(0.001)
                            break
                        break

                    next_seq += 1
                    cursor += len(chunk)
                    can_send -= 1
                    sent_count += 1

                # Progress
                if progress_callback and cursor - last_prog > max(total_size // 200, 1):
                    progress_callback(min(cursor, total_size))
                    last_prog = cursor

                # ── 2. Читаем ACK (non-blocking) ──
                moved = False
                # Читаем все доступные ACK
                for _ in range(100):  # Ограничиваем количество за раз
                    r, _, _ = select.select([sock], [], [], 0)
                    if not r:
                        break
                    try:
                        ap, _ = sock.recvfrom(16)
                    except (BlockingIOError, OSError):
                        break
                    if len(ap) < _HDR_SIZE:
                        continue
                    s, t = _HDR.unpack_from(ap)

                    if t == PacketType.ACK.value and s > base:
                        # Кумулятивный ACK: всё до s подтверждено
                        for k in range(base, s):
                            packets.pop(k, None)
                        base = s
                        last_ack_time = time.monotonic()
                        moved = True
                        retransmit_count = 0

                    elif t == PacketType.NACK.value and s in packets:
                        # Selective retransmit
                        try:
                            sendto(packets[s], addr)
                        except OSError:
                            pass
                        retransmit_count = 0

                # Если окно двинулось — сразу шлём ещё
                if moved:
                    continue

                # ── 3. Окно заполнено, ACK нет — ждём немного ──
                now = time.monotonic()

                if now - last_ack_time > _CONNECTION_TIMEOUT:
                    raise ConnectionLostError(
                        f"No ACK for {_CONNECTION_TIMEOUT}s — connection lost"
                    )

                if packets and now - last_ack_time > UDP_TIMEOUT:
                    # Таймаут: переотправляем начало окна
                    cnt = 0
                    retransmit_count += 1
                    if retransmit_count > 10:  # Слишком много ретрансмиссий
                        raise ConnectionLostError("Too many retransmissions")

                    # Отправляем пакеты с начала окна
                    for k in sorted(packets.keys())[:128]:  # Ограничиваем количество
                        try:
                            sendto(packets[k], addr)
                        except OSError:
                            pass
                        cnt += 1
                        if cnt >= 128:
                            break
                    last_ack_time = now
                elif in_flight >= UDP_WINDOW_SIZE:
                    # Окно полное — ждём ACK с коротким таймаутом
                    if sys.platform == "win32":
                        time.sleep(0.001)  # Небольшая пауза для Windows
                    r, _, _ = select.select([sock], [], [], 0.001)
                    if r:
                        continue  # перечитаем ACK на следующей итерации

        except ConnectionLostError:
            raise
        except Exception as e:
            raise ConnectionLostError(f"Send error: {e}")

        # ── FIN ──
        fin_seq = next_seq
        fin_pkt = self._pack(fin_seq, PacketType.FIN.value)
        for attempt in range(_FIN_RETRIES):
            try:
                self._send_raw(fin_pkt, addr)
            except ConnectionLostError:
                break
            r, _, _ = select.select([sock], [], [], _FIN_TIMEOUT)
            if not r:
                continue
            try:
                ap, _ = sock.recvfrom(16)
            except (BlockingIOError, OSError):
                continue
            if len(ap) >= _HDR_SIZE:
                s, t = _HDR.unpack_from(ap)
                if t == PacketType.ACK.value and s == fin_seq + 1:
                    break

    # ══════════════════════════════════════════════════════
    #  RECV STREAM
    # ══════════════════════════════════════════════════════

    def recv_stream(self, writer, total_size: int = 0,
                    progress_callback: Callable[[int], None] = None) -> int:
        sock = self.sock

        expected = 0
        ooo: Dict[int, bytes] = {}
        total_received = 0
        last_pkt_time = time.monotonic()
        last_ack_time = 0.0
        pkts_since_ack = 0
        write_buf = bytearray()

        while True:
            now = time.monotonic()
            if now - last_pkt_time > _CONNECTION_TIMEOUT:
                print(f"\nConnection timeout ({_CONNECTION_TIMEOUT}s no data)")
                break

            # Читаем все доступные пакеты
            packets_read = 0
            while packets_read < 1024:  # Ограничиваем количество за раз
                try:
                    pkt, addr = sock.recvfrom(65536)
                except (BlockingIOError, OSError):
                    break

                packets_read += 1

                if self.dest_addr is None:
                    self.dest_addr = addr
                elif addr != self.dest_addr:
                    continue

                if len(pkt) < _HDR_SIZE:
                    continue

                seq, ptype = _HDR.unpack_from(pkt)
                last_pkt_time = time.monotonic()

                if ptype == PacketType.CMD.value:
                    continue

                # FIN
                if ptype == PacketType.FIN.value:
                    if write_buf:
                        writer.write(bytes(write_buf))
                        write_buf.clear()
                    fin_ack = _HDR.pack(seq + 1, PacketType.ACK.value)
                    for _ in range(10):  # Увеличено количество попыток
                        try:
                            self._send_raw(fin_ack, addr)
                        except ConnectionLostError:
                            pass
                        time.sleep(0.01)
                    return total_received

                if ptype != PacketType.DATA.value:
                    continue

                data = pkt[_HDR_SIZE:]

                if seq == expected:
                    # In-order: append to buffer
                    write_buf.extend(data)
                    total_received += len(data)
                    expected += 1
                    pkts_since_ack += 1

                    # Drain OOO buffer
                    while expected in ooo:
                        d = ooo.pop(expected)
                        write_buf.extend(d)
                        total_received += len(d)
                        expected += 1
                        pkts_since_ack += 1

                elif seq > expected and seq < expected + UDP_WINDOW_SIZE * 2:
                    # Out of order — буферизируем
                    ooo[seq] = data
                    # NACK для пропущенного пакета
                    try:
                        self._send_raw(
                            _HDR.pack(expected, PacketType.NACK.value),
                            addr
                        )
                    except ConnectionLostError:
                        break
                    pkts_since_ack = UDP_ACK_INTERVAL  # force ACK

                # Дубликат (seq < expected) — игнорируем

            # После drain — flush и ACK
            if len(write_buf) >= _FLUSH_SIZE:
                writer.write(bytes(write_buf))
                write_buf.clear()

            if progress_callback and pkts_since_ack > 0:
                progress_callback(total_received)

            # Отправляем ACK
            if pkts_since_ack >= UDP_ACK_INTERVAL or time.monotonic() - last_ack_time > 0.005:  # Увеличен интервал
                if self.dest_addr:
                    try:
                        self._send_raw(
                            _HDR.pack(expected, PacketType.ACK.value),
                            self.dest_addr
                        )
                    except ConnectionLostError:
                        break
                pkts_since_ack = 0
                last_ack_time = time.monotonic()

            # Небольшая пауза для Windows чтобы не перегружать CPU
            if sys.platform == "win32" and packets_read == 0:
                time.sleep(0.001)

        if write_buf:
            writer.write(bytes(write_buf))
        return total_received