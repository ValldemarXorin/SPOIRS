"""Reliable UDP — максимально стабильная версия для Windows-Linux."""

import socket
import struct
import time
import select
import sys
import threading
from typing import Optional, Tuple, Dict, Callable

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
_FLUSH_SIZE = 256 * 1024  # 256KB
_CONNECTION_TIMEOUT = 120.0  # 2 минуты
_FIN_RETRIES = 200
_FIN_TIMEOUT = 2.0

# Параметры для надежности
_SEND_BURST = 32  # Маленький burst для надежности
_MAX_RETRANSMISSIONS = 500  # Очень много попыток
_KEEP_ALIVE_INTERVAL = 5.0  # Keep-alive каждые 5 секунд


class ConnectionLostError(Exception):
    pass


class RUDPSocket:
    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock = sock
        self.dest_addr = dest_addr
        self.running = True
        self.last_activity = time.time()

        # Максимальные буферы
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            for size in [64 * 1024 * 1024, 32 * 1024 * 1024, 16 * 1024 * 1024]:
                try:
                    self.sock.setsockopt(socket.SOL_SOCKET, opt, size)
                    break
                except OSError:
                    continue

        if sys.platform == "win32":
            self.sock.setblocking(False)

        # Для отладки
        self.packets_sent = 0
        self.packets_received = 0
        self.retransmissions = 0

    def _pack(self, seq: int, ptype: int, data: bytes = b"") -> bytes:
        return _HDR.pack(seq, ptype) + data

    def _send_raw(self, data: bytes, addr: Tuple[str, int], retry=True) -> bool:
        for attempt in range(30 if retry else 1):
            try:
                self.sock.sendto(data, addr)
                self.last_activity = time.time()
                return True
            except (BlockingIOError, OSError) as e:
                if sys.platform == "win32" and "10035" in str(e):
                    time.sleep(0.01)
                    continue
                if not retry:
                    return False
                time.sleep(0.01)
            except Exception:
                if not retry:
                    return False
                time.sleep(0.01)
        return False

    # ── команды ───────────────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")

        addr = self.dest_addr
        pkt = self._pack(0, PacketType.CMD.value, text.encode())

        for attempt in range(UDP_RETRY_LIMIT * 5):
            self._send_raw(pkt, addr)

            # Ждем ответ
            t0 = time.time()
            while time.time() - t0 < 5.0:
                r, _, _ = select.select([self.sock], [], [], 1.0)
                if not r:
                    continue

                try:
                    rp, ra = self.sock.recvfrom(65536)
                except (BlockingIOError, OSError):
                    continue

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

    # ═══════════════════════════════════════════════════
    #  Отправка потока данных
    # ═══════════════════════════════════════════════════

    def send_stream(self, reader, total_size: int,
                    progress_callback: Callable[[int], None] = None) -> None:
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")

        addr = self.dest_addr
        sock = self.sock

        base = 0
        next_seq = 0
        packets = {}
        cursor = 0
        eof = False
        last_ack_time = time.time()
        last_keep_alive = time.time()
        no_progress_time = time.time()
        last_cursor = 0

        try:
            while cursor < total_size or base < next_seq:
                now = time.time()

                # Keep-alive
                if now - last_keep_alive > _KEEP_ALIVE_INTERVAL:
                    try:
                        sock.sendto(b"", addr)
                    except:
                        pass
                    last_keep_alive = now

                # Проверка прогресса
                if cursor == last_cursor:
                    if now - no_progress_time > 30.0:
                        if base > 0 or cursor > 0:
                            no_progress_time = now
                        else:
                            raise ConnectionLostError("No progress")
                else:
                    last_cursor = cursor
                    no_progress_time = now

                # Отправка новых пакетов
                in_flight = next_seq - base
                can_send = UDP_WINDOW_SIZE - in_flight
                sent_count = 0

                while not eof and can_send > 0 and sent_count < _SEND_BURST:
                    chunk = reader.read(UDP_PAYLOAD_SIZE)
                    if not chunk:
                        eof = True
                        cursor = total_size
                        break

                    pkt = self._pack(next_seq, PacketType.DATA.value) + chunk
                    packets[next_seq] = pkt

                    if self._send_raw(pkt, addr, retry=False):
                        self.packets_sent += 1
                        next_seq += 1
                        cursor += len(chunk)
                        can_send -= 1
                        sent_count += 1
                    else:
                        time.sleep(0.001)
                        break

                # Прогресс
                if progress_callback and cursor - last_cursor > 1024*1024:
                    progress_callback(min(cursor, total_size))

                # Чтение ACK
                for _ in range(100):
                    r, _, _ = select.select([sock], [], [], 0)
                    if not r:
                        break

                    try:
                        ack_data, _ = sock.recvfrom(64)
                    except (BlockingIOError, OSError):
                        break

                    if len(ack_data) < _HDR_SIZE:
                        continue

                    seq, type_ = _HDR.unpack_from(ack_data)

                    if type_ == PacketType.ACK.value and seq > base:
                        for i in range(base, seq):
                            packets.pop(i, None)
                        base = seq
                        last_ack_time = now
                        self.retransmissions = 0

                    elif type_ == PacketType.NACK.value and seq in packets:
                        self._send_raw(packets[seq], addr, retry=False)
                        self.retransmissions += 1

                # Таймаут - ретрансмиссия
                if packets and now - last_ack_time > UDP_TIMEOUT * 3:
                    self.retransmissions += 1
                    if self.retransmissions > _MAX_RETRANSMISSIONS:
                        if base > 0:
                            self.retransmissions = _MAX_RETRANSMISSIONS // 2
                        else:
                            raise ConnectionLostError("Too many retransmissions")

                    # Отправляем все неподтвержденные
                    for seq in sorted(packets.keys())[:64]:
                        self._send_raw(packets[seq], addr, retry=False)

                    last_ack_time = now
                    time.sleep(0.01)

                # Небольшая пауза если ничего не происходит
                if sent_count == 0 and not packets:
                    time.sleep(0.01)

        except Exception as e:
            raise ConnectionLostError(f"Send error: {e}")

        # FIN
        fin_seq = next_seq
        fin_pkt = self._pack(fin_seq, PacketType.FIN.value)

        for attempt in range(_FIN_RETRIES):
            self._send_raw(fin_pkt, addr)

            r, _, _ = select.select([sock], [], [], _FIN_TIMEOUT/10)
            if r:
                try:
                    ack, _ = sock.recvfrom(64)
                    if len(ack) >= _HDR_SIZE:
                        s, t = _HDR.unpack_from(ack)
                        if t == PacketType.ACK.value and s == fin_seq + 1:
                            break
                except:
                    pass
            time.sleep(0.05)

    # ═══════════════════════════════════════════════════
    #  Получение потока данных
    # ═══════════════════════════════════════════════════

    def recv_stream(self, writer, total_size: int = 0,
                    progress_callback: Callable[[int], None] = None) -> int:
        sock = self.sock

        expected = 0
        ooo = {}
        total_received = 0
        last_packet_time = time.time()
        last_ack_time = 0
        pkts_since_ack = 0
        write_buf = bytearray()
        last_progress = 0
        no_data_count = 0
        last_keep_alive = time.time()

        while True:
            now = time.time()

            # Keep-alive
            if now - last_keep_alive > _KEEP_ALIVE_INTERVAL and self.dest_addr:
                try:
                    sock.sendto(b"", self.dest_addr)
                except:
                    pass
                last_keep_alive = now

            # Таймаут
            if now - last_packet_time > _CONNECTION_TIMEOUT:
                if total_received >= total_size:
                    break
                print(f"\nTimeout after {_CONNECTION_TIMEOUT}s")
                break

            # Чтение пакетов
            packets_read = 0
            while packets_read < 1000:
                try:
                    pkt, addr = sock.recvfrom(65536)
                except (BlockingIOError, OSError):
                    break

                packets_read += 1
                no_data_count = 0
                last_packet_time = now

                if self.dest_addr is None:
                    self.dest_addr = addr
                elif addr != self.dest_addr:
                    continue

                if len(pkt) < _HDR_SIZE:
                    continue

                # Пустой пакет (keep-alive)
                if len(pkt) == _HDR_SIZE:
                    continue

                seq, ptype = _HDR.unpack_from(pkt)
                self.packets_received += 1

                if ptype == PacketType.FIN.value:
                    if write_buf:
                        writer.write(bytes(write_buf))
                    # Отвечаем много раз
                    fin_ack = self._pack(seq + 1, PacketType.ACK.value)
                    for _ in range(50):
                        self._send_raw(fin_ack, addr, retry=False)
                        time.sleep(0.01)
                    return total_received

                if ptype != PacketType.DATA.value:
                    continue

                data = pkt[_HDR_SIZE:]

                if seq == expected:
                    write_buf.extend(data)
                    total_received += len(data)
                    expected += 1
                    pkts_since_ack += 1

                    # Буфер
                    while expected in ooo:
                        write_buf.extend(ooo.pop(expected))
                        total_received += len(ooo[expected])
                        expected += 1
                        pkts_since_ack += 1

                elif seq > expected:
                    if seq < expected + UDP_WINDOW_SIZE * 8:
                        ooo[seq] = data
                        # NACK
                        if pkts_since_ack % 5 == 0:
                            nack = self._pack(expected, PacketType.NACK.value)
                            self._send_raw(nack, addr, retry=False)

            # Запись
            if len(write_buf) >= _FLUSH_SIZE or total_received >= total_size:
                if write_buf:
                    writer.write(bytes(write_buf))
                    write_buf.clear()

            # Прогресс
            if progress_callback and total_received - last_progress > 1024*1024:
                progress_callback(total_received)
                last_progress = total_received

            # ACK
            if pkts_since_ack >= UDP_ACK_INTERVAL // 2 or now - last_ack_time > 0.05:
                if self.dest_addr:
                    ack = self._pack(expected, PacketType.ACK.value)
                    self._send_raw(ack, self.dest_addr, retry=False)
                pkts_since_ack = 0
                last_ack_time = now

            # Пауза если нет данных
            if packets_read == 0:
                no_data_count += 1
                if no_data_count > 50:
                    time.sleep(0.05)
                else:
                    time.sleep(0.005)

        # Остаток
        if write_buf:
            writer.write(bytes(write_buf))

        return total_received