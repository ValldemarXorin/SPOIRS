"""Reliable UDP — простой и быстрый, с поддержкой Windows/Linux.

Без сложного congestion control. Для LAN/localhost где потерь мало.

Механизмы:
1. Кумулятивные ACK — подтверждают все до seq N
2. Скользящее окно — до UDP_WINDOW_SIZE пакетов без ожидания
3. Повторная передача — по таймауту и по NACK
4. Receiver шлёт ACK каждые 32 пакета и каждые 2ms
"""

import socket
import struct
import time
import select
import sys
from typing import Optional, Tuple, Dict, Callable, List

from .protocol import (
    UDP_PAYLOAD_SIZE,
    UDP_WINDOW_SIZE,
    UDP_TIMEOUT,
    PacketType,
    UDP_RETRY_LIMIT,
    UDP_ACK_INTERVAL,
)

_HDR = struct.Struct("!IB")
_HDR_SIZE = _HDR.size
_FLUSH_SIZE = 512 * 1024  # Уменьшен для более частой записи
_CONNECTION_TIMEOUT = 60.0  # Увеличен таймаут соединения
_FIN_RETRIES = 100  # Увеличено количество попыток FIN
_FIN_TIMEOUT = 1.0  # Увеличен таймаут FIN

# Сколько пакетов отправлять за один burst перед проверкой ACK
_SEND_BURST = 64  # Уменьшено для надежности

# Максимальное количество ретрансмиссий перед ошибкой
_MAX_RETRANSMISSIONS = 200  # Сильно увеличено


class ConnectionLostError(Exception):
    pass


class RUDPSocket:
    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock = sock
        self.dest_addr = dest_addr
        # Увеличиваем буферы для Windows
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            for size in [32 * 1024 * 1024, 16 * 1024 * 1024, 8 * 1024 * 1024]:
                try:
                    self.sock.setsockopt(socket.SOL_SOCKET, opt, size)
                    break
                except OSError:
                    continue

        # Для Windows устанавливаем неблокирующий режим
        if sys.platform == "win32":
            self.sock.setblocking(False)

        self.last_ack_sent = 0
        self.packets_sent = 0

    def _pack(self, seq: int, ptype: int, data: bytes = b"") -> bytes:
        return _HDR.pack(seq, ptype) + data

    def _send_raw(self, data: bytes, addr: Tuple[str, int]) -> bool:
        for attempt in range(20):  # Увеличено количество попыток
            try:
                self.sock.sendto(data, addr)
                return True
            except BlockingIOError:
                # Буфер отправки полный — короткая пауза
                time.sleep(0.01)  # Увеличена пауза
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
                    time.sleep(0.01)
                    continue
                if sys.platform == "win32" and "10040" in str(e):  # MSGSIZE
                    return False
                return False
        return False

    # ── командный канал ───────────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")
        addr = self.dest_addr
        pkt = self._pack(0, PacketType.CMD.value, text.encode())

        for attempt in range(UDP_RETRY_LIMIT * 3):  # Утроено количество попыток
            try:
                self._send_raw(pkt, addr)
            except ConnectionLostError:
                return None

            t0 = time.monotonic()
            while time.monotonic() - t0 < 3.0:  # Увеличен таймаут
                r, _, _ = select.select([self.sock], [], [], 0.5)
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
    #  SEND STREAM (для отправки файлов)
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
        last_retransmit_time = 0
        no_progress_time = time.monotonic()
        last_base = 0
        last_send_time = time.monotonic()

        try:
            while cursor < total_size or base < next_seq:
                now = time.monotonic()

                # Проверяем, есть ли прогресс
                if base == last_base:
                    if now - no_progress_time > 20.0:  # Увеличен таймаут
                        if base > 0:
                            # Если хоть что-то передалось, продолжаем
                            no_progress_time = now
                        else:
                            raise ConnectionLostError("No progress for 20 seconds")
                else:
                    last_base = base
                    no_progress_time = now

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
                        self.packets_sent += 1
                    except (BlockingIOError, OSError) as e:
                        if sys.platform == "win32" and "10035" in str(e):
                            time.sleep(0.01)
                            break
                        break

                    next_seq += 1
                    cursor += len(chunk)
                    can_send -= 1
                    sent_count += 1
                    last_send_time = now

                # Progress callback
                if progress_callback and cursor - last_prog > max(total_size // 50, 8192):
                    progress_callback(min(cursor, total_size))
                    last_prog = cursor

                # ── 2. Читаем ACK (non-blocking) ──
                moved = False
                ack_received = False

                # Читаем все доступные ACK
                for _ in range(500):  # Увеличено количество
                    r, _, _ = select.select([sock], [], [], 0)
                    if not r:
                        break
                    try:
                        ap, _ = sock.recvfrom(64)
                    except (BlockingIOError, OSError):
                        break
                    if len(ap) < _HDR_SIZE:
                        continue
                    s, t = _HDR.unpack_from(ap)

                    if t == PacketType.ACK.value:
                        ack_received = True
                        if s > base:
                            # Кумулятивный ACK
                            for k in range(base, s):
                                packets.pop(k, None)
                            base = s
                            last_ack_time = now
                            moved = True
                            retransmit_count = 0

                    elif t == PacketType.NACK.value and s in packets:
                        # Selective retransmit
                        try:
                            sendto(packets[s], addr)
                        except OSError:
                            pass

                # Если окно двинулось — сразу шлём ещё
                if moved:
                    continue

                # ── 3. Обработка таймаутов и ретрансмиссий ──
                now = time.monotonic()

                if now - last_ack_time > _CONNECTION_TIMEOUT:
                    raise ConnectionLostError(
                        f"No ACK for {_CONNECTION_TIMEOUT}s — connection lost"
                    )

                # Проверяем, нужно ли делать ретрансмиссию
                if packets and now - last_retransmit_time > UDP_TIMEOUT * 2:  # Удвоен таймаут
                    retransmit_count += 1
                    last_retransmit_time = now

                    if retransmit_count > _MAX_RETRANSMISSIONS:
                        # Проверяем, есть ли прогресс
                        if base > 0 or ack_received:
                            retransmit_count = _MAX_RETRANSMISSIONS // 2
                            print(f"Warning: High retransmission count ({retransmit_count})")
                        else:
                            raise ConnectionLostError("Too many retransmissions")

                    # Отправляем все неподтвержденные пакеты
                    packets_to_send = sorted(packets.keys())
                    for k in packets_to_send[:128]:  # Ограничиваем количество
                        try:
                            sendto(packets[k], addr)
                        except OSError:
                            pass

                    time.sleep(0.002)  # Небольшая пауза

                elif in_flight >= UDP_WINDOW_SIZE:
                    # Окно полное — ждём ACK
                    if sys.platform == "win32":
                        time.sleep(0.005)
                    r, _, _ = select.select([sock], [], [], 0.01)
                    if r:
                        continue

        except ConnectionLostError:
            raise
        except Exception as e:
            raise ConnectionLostError(f"Send error: {e}")

        # ── FIN ──
        fin_seq = next_seq
        fin_pkt = self._pack(fin_seq, PacketType.FIN.value)
        fin_acked = False

        for attempt in range(_FIN_RETRIES):
            try:
                self._send_raw(fin_pkt, addr)
            except ConnectionLostError:
                break

            # Ждем ACK на FIN
            for _ in range(20):
                r, _, _ = select.select([sock], [], [], _FIN_TIMEOUT / 20)
                if not r:
                    continue
                try:
                    ap, _ = sock.recvfrom(64)
                except (BlockingIOError, OSError):
                    continue
                if len(ap) >= _HDR_SIZE:
                    s, t = _HDR.unpack_from(ap)
                    if t == PacketType.ACK.value and s == fin_seq + 1:
                        fin_acked = True
                        break
            if fin_acked:
                break
            time.sleep(0.1)

        if not fin_acked:
            print("Warning: FIN not acknowledged")

    # ══════════════════════════════════════════════════════
    #  RECV STREAM (для получения файлов)
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
        last_progress_time = time.monotonic()
        last_total = 0
        no_data_count = 0

        while True:
            now = time.monotonic()

            # Проверяем таймаут соединения
            if now - last_pkt_time > _CONNECTION_TIMEOUT:
                if total_received >= total_size:
                    break
                print(f"\nConnection timeout ({_CONNECTION_TIMEOUT}s no data)")
                break

            # Проверяем, есть ли прогресс
            if total_received == last_total:
                if now - last_progress_time > 30.0:  # Увеличен таймаут
                    if total_received >= total_size:
                        break
                    if total_received > 0:
                        # Если хоть что-то получили, продолжаем
                        last_progress_time = now
                    else:
                        print(f"\nNo progress for 30 seconds")
                        break
            else:
                last_total = total_received
                last_progress_time = now
                no_data_count = 0

            # Читаем все доступные пакеты
            packets_read = 0
            while packets_read < 4096:  # Увеличено количество за раз
                try:
                    pkt, addr = sock.recvfrom(65536)
                except (BlockingIOError, OSError):
                    break

                packets_read += 1
                no_data_count = 0

                if self.dest_addr is None:
                    self.dest_addr = addr
                elif addr != self.dest_addr:
                    continue

                if len(pkt) < _HDR_SIZE:
                    continue

                seq, ptype = _HDR.unpack_from(pkt)
                last_pkt_time = now

                if ptype == PacketType.CMD.value:
                    continue

                # FIN
                if ptype == PacketType.FIN.value:
                    if write_buf:
                        writer.write(bytes(write_buf))
                        write_buf.clear()
                    fin_ack = _HDR.pack(seq + 1, PacketType.ACK.value)
                    for _ in range(30):  # Увеличено количество попыток
                        try:
                            self._send_raw(fin_ack, addr)
                        except ConnectionLostError:
                            pass
                        time.sleep(0.05)
                    return total_received

                if ptype != PacketType.DATA.value:
                    continue

                data = pkt[_HDR_SIZE:]

                if seq == expected:
                    # In-order
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

                elif seq > expected and seq < expected + UDP_WINDOW_SIZE * 4:  # Увеличен буфер
                    # Out of order
                    ooo[seq] = data
                    # Отправляем NACK для пропущенного пакета
                    if pkts_since_ack % 3 == 0:  # Чаще отправляем NACK
                        try:
                            self._send_raw(
                                _HDR.pack(expected, PacketType.NACK.value),
                                addr
                            )
                        except ConnectionLostError:
                            break

            # После чтения пакетов - записываем буфер
            if len(write_buf) >= _FLUSH_SIZE or (total_received >= total_size and write_buf):
                writer.write(bytes(write_buf))
                write_buf.clear()

            if progress_callback and pkts_since_ack > 0:
                progress_callback(total_received)

            # Отправляем ACK
            if pkts_since_ack >= UDP_ACK_INTERVAL // 2 or now - last_ack_time > 0.02:  # Чаще ACK
                if self.dest_addr:
                    try:
                        self._send_raw(
                            _HDR.pack(expected, PacketType.ACK.value),
                            self.dest_addr
                        )
                        self.last_ack_sent = expected
                    except ConnectionLostError:
                        break
                pkts_since_ack = 0
                last_ack_time = now

            # Если нет данных, небольшая пауза
            if packets_read == 0:
                no_data_count += 1
                if no_data_count > 100:
                    time.sleep(0.01)
                else:
                    time.sleep(0.001)

        if write_buf:
            writer.write(bytes(write_buf))
        return total_received