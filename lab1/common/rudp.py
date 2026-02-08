"""Модуль Reliable UDP для реализации собственного протокола передачи данных."""
import socket
import struct
import time
import select
from typing import Optional, Tuple, Dict
from .protocol import (
    UDP_PAYLOAD_SIZE, UDP_HEADER_SIZE, UDP_WINDOW_SIZE,
    UDP_TIMEOUT, PacketType, UDP_RETRY_LIMIT
)


class RUDPSocket:
    """Класс-обертка для реализации Reliable UDP."""

    def __init__(self, sock: socket.socket, dest_addr: Tuple[str, int] = None):
        self.sock = sock
        self.dest_addr = dest_addr
        self.sock.setblocking(False)
        try:
            # Увеличиваем буферы до 50MB, чтобы ядро не отбрасывало пакеты при нашей агрессивной отправке
            buff_size = 50 * 1024 * 1024
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buff_size)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buff_size)
        except socket.error:
            pass

    def _pack_packet(self, seq_num: int, p_type_val: int, data: bytes) -> bytes:
        """Упаковывает пакет с заголовком."""
        return struct.pack('!IB', seq_num, p_type_val) + data

    def _unpack_header(self, packet: bytes) -> Tuple[int, int, bytes]:
        """Распаковывает заголовок пакета."""
        if len(packet) < UDP_HEADER_SIZE:
            return -1, -1, b''
        seq_num, type_val = struct.unpack('!IB', packet[:UDP_HEADER_SIZE])
        return seq_num, type_val, packet[UDP_HEADER_SIZE:]

    def send_command(self, text: str) -> Optional[str]:
        """Отправляет текстовую команду и ждет ответа."""
        data = text.encode()
        seq = 0
        pkt = self._pack_packet(seq, PacketType.CMD.value, data)
        ack_received = False

        for attempt in range(UDP_RETRY_LIMIT):
            if not ack_received and self.dest_addr:
                self.sock.sendto(pkt, self.dest_addr)

            wait_time = 2.0 if ack_received else 0.5
            start_wait = time.time()
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
                    except socket.error:
                        pass
        return None

    def recv_command(self) -> Tuple[str, Tuple[str, int]]:
        """Принимает команду от клиента."""
        try:
            pkt, addr = self.sock.recvfrom(65536)
        except BlockingIOError:
            return "", ("", 0)

        seq, p_type, data = self._unpack_header(pkt)
        if p_type == PacketType.CMD.value:
            msg = data.decode(errors='ignore')
            if msg.startswith("OK ") or msg.startswith("ERROR "):
                return "", addr
            resp = self._pack_packet(seq, PacketType.CMD.value, b"ACK_CMD")
            self.sock.sendto(resp, addr)
            return msg, addr
        return "", addr

    def send_stream(self, reader, total_size: int) -> None:
        """Агрессивная отправка файла."""
        base = 0
        next_seq_num = 0
        window_size = UDP_WINDOW_SIZE
        packets: Dict[int, bytes] = {}
        file_cursor = 0
        last_send_time = time.time()

        # Настройка "взрывной" отправки
        burst_limit = 100  # Сколько пакетов слать без проверки ACK

        while base * UDP_PAYLOAD_SIZE < total_size:

            # 1. Burst Sending Loop
            # Отправляем пачку пакетов за раз, чтобы не дергать проверку сокета слишком часто
            packets_sent_in_burst = 0
            while next_seq_num < base + window_size and file_cursor < total_size:
                chunk = reader.read(UDP_PAYLOAD_SIZE)
                if not chunk:
                    break

                pkt = self._pack_packet(next_seq_num, PacketType.DATA.value, chunk)
                packets[next_seq_num] = pkt
                if self.dest_addr:
                    self.sock.sendto(pkt, self.dest_addr)

                next_seq_num += 1
                file_cursor += len(chunk)
                packets_sent_in_burst += 1

                # Если отправили пачку, прерываемся проверить ACK
                if packets_sent_in_burst >= burst_limit:
                    break

            # 2. Process ACKs (Non-blocking bulk read)
            # Пытаемся вычитать ВСЕ доступные ACK разом
            try:
                while True:
                    ack_pkt, addr = self.sock.recvfrom(1024)  # ACK маленький
                    if self.dest_addr and addr != self.dest_addr:
                        continue

                    ack_seq, p_type, _ = self._unpack_header(ack_pkt)
                    if p_type == PacketType.ACK.value:
                        if ack_seq > base:
                            # Быстрая очистка словаря (Python 3.7+ делает это быстро)
                            for i in range(base, ack_seq):
                                packets.pop(i, None)
                            base = ack_seq
                            last_send_time = time.time()
            except BlockingIOError:
                pass
            except socket.error:
                pass

            # 3. Retransmission Logic
            if time.time() - last_send_time > UDP_TIMEOUT:
                # Если таймаут - шлем всё окно агрессивно
                for seq in range(base, next_seq_num):
                    if seq in packets:
                        self.sock.sendto(packets[seq], self.dest_addr)
                last_send_time = time.time()

        # 4. FIN
        fin_pkt = self._pack_packet(next_seq_num, PacketType.FIN.value, b'')
        for _ in range(30):
            if self.dest_addr:
                self.sock.sendto(fin_pkt, self.dest_addr)
            ready = select.select([self.sock], [], [], 0.05)
            if ready[0]:
                try:
                    ack_pkt, _ = self.sock.recvfrom(1024)
                    ack_seq, p_type, _ = self._unpack_header(ack_pkt)
                    if p_type == PacketType.ACK.value and ack_seq == next_seq_num + 1:
                        break
                except socket.error:
                    pass

    def recv_stream(self, writer) -> int:
        """Агрессивный прием файла."""
        expected_seq = 0
        received_buffer: Dict[int, bytes] = {}
        total_bytes = 0

        last_pkt_time = time.time()
        timeout_limit = 10.0

        # Super Aggressive ACK Decimation
        ack_interval = 500  # Шлем ACK только каждые 500 пакетов!
        packets_since_ack = 0
        last_ack_time = time.time()

        while True:
            # Небольшая проверка таймаута
            if time.time() - last_pkt_time > timeout_limit:
                print("RUDP Timeout")
                break

            # Увеличиваем таймаут select, чтобы реже просыпаться впустую
            ready = select.select([self.sock], [], [], 0.5)
            if not ready[0]:
                continue

            try:
                # Читаем пакет
                pkt, addr = self.sock.recvfrom(65536)
                if self.dest_addr is None:
                    self.dest_addr = addr
                elif addr != self.dest_addr:
                    continue

                seq, p_type, data = self._unpack_header(pkt)
                last_pkt_time = time.time()

                if p_type == PacketType.FIN.value:
                    ack = self._pack_packet(seq + 1, PacketType.ACK.value, b'')
                    self.sock.sendto(ack, addr)
                    break

                if p_type == PacketType.DATA.value:
                    if seq == expected_seq:
                        writer.write(data)
                        total_bytes += len(data)
                        expected_seq += 1
                        packets_since_ack += 1

                        # Process buffer
                        while expected_seq in received_buffer:
                            data = received_buffer.pop(expected_seq)
                            writer.write(data)
                            total_bytes += len(data)
                            expected_seq += 1
                            packets_since_ack += 1

                    elif seq > expected_seq:
                        if seq < expected_seq + UDP_WINDOW_SIZE:
                            received_buffer[seq] = data
                            # Если out-of-order, форсируем ACK быстрее
                            packets_since_ack = ack_interval

                    # Отправка ACK (только если накопилось или прошло время)
                    current_time = time.time()
                    if packets_since_ack >= ack_interval or (current_time - last_ack_time > 0.05):
                        ack = self._pack_packet(expected_seq, PacketType.ACK.value, b'')
                        self.sock.sendto(ack, addr)
                        packets_since_ack = 0
                        last_ack_time = current_time

            except socket.error:
                break

        return total_bytes