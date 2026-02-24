"""Модуль Reliable UDP для реализации собственного протокола передачи данных."""

import socket
import struct
import time
import select
from typing import Optional, Tuple, Dict, Callable

from .protocol import (
    UDP_PAYLOAD_SIZE, UDP_HEADER_SIZE, UDP_WINDOW_SIZE,
    UDP_TIMEOUT, PacketType, UDP_RETRY_LIMIT
)


class RUDPSocket:
    """Класс-обертка для реализации Reliable UDP."""

    def __init__(self, sock: socket.socket, dest_addr: Tuple[str, int] = None):
        self.sock = sock
        self.dest_addr = dest_addr
        try:
            buff_size = 50 * 1024 * 1024
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buff_size)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buff_size)
        except socket.error:
            pass

    def _pack_packet(self, seq_num: int, p_type_val: int, data: bytes) -> bytes:
        return struct.pack('!IB', seq_num, p_type_val) + data

    def _unpack_header(self, packet: bytes) -> Tuple[int, int, bytes]:
        if len(packet) < UDP_HEADER_SIZE:
            return -1, -1, b''
        seq_num, type_val = struct.unpack('!IB', packet[:UDP_HEADER_SIZE])
        return seq_num, type_val, packet[UDP_HEADER_SIZE:]

    def _sendto_safe(self, data: bytes, addr: tuple) -> bool:
        for _ in range(50):
            try:
                self.sock.sendto(data, addr)
                return True
            except BlockingIOError:
                time.sleep(0.001)
            except OSError as e:
                if e.errno in (10035, 11):
                    time.sleep(0.001)
                else:
                    return False
        return False

    # ------------------------------------------------------------------ #
    #  Команды                                                             #
    # ------------------------------------------------------------------ #

    def send_command(self, text: str) -> Optional[str]:
        data = text.encode()
        seq = 0
        pkt = self._pack_packet(seq, PacketType.CMD.value, data)
        ack_received = False

        for attempt in range(UDP_RETRY_LIMIT):
            if not ack_received and self.dest_addr:
                self._sendto_safe(pkt, self.dest_addr)

            start_wait = time.time()
            wait_time = 2.0 if ack_received else 0.5
            while time.time() - start_wait < wait_time:
                ready = select.select([self.sock], [], [], 0.05)
                if ready[0]:
                    try:
                        resp_pkt, addr = self.sock.recvfrom(65536)
                        if self.dest_addr and addr != self.dest_addr:
                            continue
                        r_seq, r_type, r_data = self._unpack_header(resp_pkt)
                        if r_type == PacketType.CMD.value:
                            decoded = r_data.decode(errors='ignore')
                            if decoded == "ACK_CMD":
                                ack_received = True
                                continue
                            return decoded
                    except (socket.timeout, BlockingIOError, OSError):
                        pass
        return None

    def recv_command(self) -> Tuple[str, Tuple[str, int]]:
        try:
            pkt, addr = self.sock.recvfrom(65536)
        except (BlockingIOError, socket.timeout, OSError):
            return "", ("", 0)
        seq, p_type, data = self._unpack_header(pkt)
        if p_type == PacketType.CMD.value:
            msg = data.decode(errors='ignore')
            if msg.startswith("OK ") or msg.startswith("ERROR "):
                return "", addr
            resp = self._pack_packet(seq, PacketType.CMD.value, b"ACK_CMD")
            self._sendto_safe(resp, addr)
            return msg, addr
        return "", addr

    # ------------------------------------------------------------------ #
    #  send_stream                                                         #
    # ------------------------------------------------------------------ #

    def send_stream(self, reader, total_size: int,
                    progress_callback: Callable[[int], None] = None) -> None:
        """
        Отправка файла со скользящим окном.
        Цикл завершается когда все байты ОТПРАВЛЕНЫ (file_cursor) и
        все пакеты ПОДТВЕРЖДЕНЫ (base >= next_seq_num), затем шлёт FIN.
        """
        base = 0
        next_seq_num = 0
        window_size = UDP_WINDOW_SIZE
        packets: Dict[int, bytes] = {}
        file_cursor = 0
        last_ack_time = time.time()
        burst_limit = 32

        try:
            self.sock.setblocking(True)
            self.sock.settimeout(None)
        except OSError:
            pass

        # Основной цикл — пока не отправлено всё И не подтверждено всё
        while file_cursor < total_size or base < next_seq_num:

            # 1. Отправляем новые пакеты пока есть место в окне
            packets_sent = 0
            while (next_seq_num < base + window_size
                   and file_cursor < total_size
                   and packets_sent < burst_limit):
                chunk = reader.read(UDP_PAYLOAD_SIZE)
                if not chunk:
                    file_cursor = total_size  # принудительно завершаем чтение
                    break
                pkt = self._pack_packet(next_seq_num, PacketType.DATA.value, chunk)
                packets[next_seq_num] = pkt
                if self.dest_addr:
                    self._sendto_safe(pkt, self.dest_addr)
                next_seq_num += 1
                file_cursor += len(chunk)
                packets_sent += 1

            if progress_callback:
                progress_callback(file_cursor)

            # 2. Читаем ACK-и (до 15 мс)
            deadline = time.time() + 0.015
            while time.time() < deadline:
                ready = select.select([self.sock], [], [], 0)
                if not ready[0]:
                    break
                try:
                    self.sock.settimeout(0.002)
                    ack_pkt, addr = self.sock.recvfrom(1024)
                    self.sock.settimeout(None)
                    if self.dest_addr and addr != self.dest_addr:
                        continue
                    ack_seq, p_type, _ = self._unpack_header(ack_pkt)
                    if p_type == PacketType.ACK.value and ack_seq > base:
                        for i in range(base, ack_seq):
                            packets.pop(i, None)
                        base = ack_seq
                        last_ack_time = time.time()
                except (socket.timeout, BlockingIOError):
                    self.sock.settimeout(None)
                except socket.error:
                    break

            # 3. Ретрансмиссия при таймауте
            if time.time() - last_ack_time > UDP_TIMEOUT and self.dest_addr:
                for seq_r in range(base, next_seq_num):
                    if seq_r in packets:
                        self._sendto_safe(packets[seq_r], self.dest_addr)
                last_ack_time = time.time()

        # 4. Все данные подтверждены — отправляем FIN
        if self.dest_addr:
            fin_pkt = self._pack_packet(next_seq_num, PacketType.FIN.value, b'')
            for attempt in range(20):
                self._sendto_safe(fin_pkt, self.dest_addr)
                ready = select.select([self.sock], [], [], 0.2)
                if ready[0]:
                    try:
                        self.sock.settimeout(0.2)
                        ack_pkt, _ = self.sock.recvfrom(1024)
                        self.sock.settimeout(None)
                        ack_seq, p_type, _ = self._unpack_header(ack_pkt)
                        if p_type == PacketType.ACK.value and ack_seq == next_seq_num + 1:
                            break  # FIN подтверждён
                    except (socket.timeout, BlockingIOError):
                        self.sock.settimeout(None)
                    except socket.error:
                        break
            # Даже без FIN-ACK считаем передачу завершённой

    # ------------------------------------------------------------------ #
    #  recv_stream                                                         #
    # ------------------------------------------------------------------ #

    def recv_stream(self, writer, total_size: int = 0,
                    progress_callback: Callable[[int], None] = None) -> int:
        expected_seq = 0
        received_buffer: Dict[int, bytes] = {}
        total_bytes = 0
        last_pkt_time = time.time()
        timeout_limit = 15.0
        ack_interval = 8   # ACK каждые 8 пакетов — чаще, чтобы окно двигалось
        packets_since_ack = 0
        last_ack_time = time.time()

        try:
            self.sock.setblocking(True)
            self.sock.settimeout(None)
        except OSError:
            pass

        while True:
            if time.time() - last_pkt_time > timeout_limit:
                print("RUDP recv timeout")
                break

            ready = select.select([self.sock], [], [], 0.5)
            if not ready[0]:
                if time.time() - last_ack_time > 0.2 and self.dest_addr:
                    ack = self._pack_packet(expected_seq, PacketType.ACK.value, b'')
                    self._sendto_safe(ack, self.dest_addr)
                    last_ack_time = time.time()
                continue

            try:
                self.sock.settimeout(0.5)
                pkt, addr = self.sock.recvfrom(65536)
                self.sock.settimeout(None)
            except (socket.timeout, BlockingIOError):
                self.sock.settimeout(None)
                continue
            except socket.error:
                break

            if self.dest_addr is None:
                self.dest_addr = addr
            elif addr != self.dest_addr:
                continue

            seq, p_type, data = self._unpack_header(pkt)
            last_pkt_time = time.time()

            if p_type == PacketType.FIN.value:
                ack = self._pack_packet(seq + 1, PacketType.ACK.value, b'')
                self._sendto_safe(ack, addr)
                break

            if p_type == PacketType.DATA.value:
                if seq == expected_seq:
                    writer.write(data)
                    total_bytes += len(data)
                    expected_seq += 1
                    packets_since_ack += 1

                    while expected_seq in received_buffer:
                        buf_data = received_buffer.pop(expected_seq)
                        writer.write(buf_data)
                        total_bytes += len(buf_data)
                        expected_seq += 1
                        packets_since_ack += 1

                    if progress_callback:
                        progress_callback(total_bytes)

                elif seq > expected_seq:
                    if seq < expected_seq + UDP_WINDOW_SIZE:
                        received_buffer[seq] = data
                    packets_since_ack = ack_interval

                now = time.time()
                if packets_since_ack >= ack_interval or (now - last_ack_time > 0.03):
                    ack = self._pack_packet(expected_seq, PacketType.ACK.value, b'')
                    self._sendto_safe(ack, addr)
                    packets_since_ack = 0
                    last_ack_time = now

        return total_bytes
