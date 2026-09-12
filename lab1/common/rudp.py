"""Reliable UDP (RUDP) — настоящая реализация на чистом UDP с sliding window."""

import socket
import time
import threading
import select
import struct
import heapq
from typing import Optional, Tuple, Callable, Dict, Any
from dataclasses import dataclass, field
from enum import IntEnum
from collections import deque

from common.protocol import PacketType, UDP_PAYLOAD_SIZE, UDP_WINDOW_SIZE, UDP_TIMEOUT, UDP_RETRY_LIMIT


class ConnectionLostError(Exception):
    pass


class RudpState(IntEnum):
    CLOSED = 0
    CONNECTING = 1
    ESTABLISHED = 2
    CLOSING = 3
    FIN_WAIT = 4


@dataclass
class Packet:
    seq: int
    ptype: PacketType
    payload: bytes = b""

    def pack(self) -> bytes:
        return struct.pack("!IB", self.seq, self.ptype.value) + self.payload

    @staticmethod
    def unpack(data: bytes) -> "Packet":
        if len(data) < 5:
            raise ValueError("Packet too small")
        seq, ptype_val = struct.unpack("!IB", data[:5])
        return Packet(seq, PacketType(ptype_val), data[5:])


@dataclass
class SentPacket:
    packet: Packet
    send_time: float
    retries: int = 0
    acked: bool = False


@dataclass
class RudpConfig:
    window_size: int = UDP_WINDOW_SIZE
    payload_size: int = UDP_PAYLOAD_SIZE
    base_timeout: float = UDP_TIMEOUT
    max_retries: int = UDP_RETRY_LIMIT
    rto_alpha: float = 0.875
    rto_beta: float = 1.0
    min_rto: float = 0.05
    max_rto: float = 5.0


class RudpSocket:
    """
    RUDP сокет с sliding window, кумулятивными ACK, fast retransmit.
    
    Архитектура:
    - Отправитель: окно отправки (sent_packets), таймер ретрансляции, RTO оценка
    - Получатель: буфер out-of-order (recv_buffer), expected_seq, кумулятивные ACK
    - Фоновый поток: обработка таймаутов, отправка ACK, очистка старых пакетов
    """

    def __init__(
        self,
        sock: socket.socket,
        dest_addr: Optional[Tuple[str, int]] = None,
        config: Optional[RudpConfig] = None,
        bind_addr: Optional[Tuple[str, int]] = None,
    ):
        self.sock = sock
        self.dest_addr = dest_addr
        self.config = config or RudpConfig()
        self.bind_addr = bind_addr

        self._lock = threading.RLock()
        self._state = RudpState.CLOSED
        self._closed = False

        # Sequence numbers
        self._send_seq = 0
        self._recv_seq = 0
        self._expected_seq = 0

        # Sender state
        self._sent_packets: Dict[int, SentPacket] = {}
        self._send_base = 0
        self._next_seq = 0
        self._send_window = self.config.window_size
        self._send_cv = threading.Condition(self._lock)

        # Receiver state
        self._recv_buffer: Dict[int, bytes] = {}
        self._recv_cv = threading.Condition(self._lock)

        # RTT estimation (Jacobson/Karels)
        self._srtt = 0.1
        self._rttvar = 0.05
        self._rto = self.config.base_timeout

        # Stats
        self.packets_sent = 0
        self.packets_received = 0
        self.retransmissions = 0
        self.dup_acks = 0
        self.last_ack_seq = -1

        # Callbacks
        self._on_data_received: Optional[Callable[[bytes], None]] = None
        self._on_connection_lost: Optional[Callable[[], None]] = None

        # Background thread
        self._worker_thread: Optional[threading.Thread] = None
        self._worker_running = False

        # Для привязки к конкретному адресу (для ЛР4 MSG_PEEK)
        self._peer_filter: Optional[Tuple[str, int]] = None

    def set_peer_filter(self, addr: Tuple[str, int]) -> None:
        """Ограничить приём пакетов только от указанного адреса (для MSG_PEEK архитектуры)."""
        with self._lock:
            self._peer_filter = addr

    def set_data_callback(self, callback: Callable[[bytes], None]) -> None:
        self._on_data_received = callback

    def set_connection_lost_callback(self, callback: Callable[[], None]) -> None:
        self._on_connection_lost = callback

    def connect(self, addr: Tuple[str, int], timeout: float = 10.0) -> bool:
        """Установка соединения (3-way handshake упрощённый: SYN -> SYN-ACK)."""
        with self._lock:
            if self._state != RudpState.CLOSED:
                return False
            self.dest_addr = addr
            self._state = RudpState.CONNECTING
            self._send_base = self._send_seq
            self._next_seq = self._send_seq

        # Отправляем SYN (CMD пакет с пустым payload)
        syn = Packet(self._send_seq, PacketType.CMD, b"SYN")
        self._send_packet(syn)
        self._send_seq += 1

        # Ждём SYN-ACK
        start = time.time()
        while time.time() - start < timeout:
            with self._lock:
                if self._state == RudpState.ESTABLISHED:
                    self._start_worker()
                    return True
            time.sleep(0.01)

        with self._lock:
            self._state = RudpState.CLOSED
        return False

    def accept(self, timeout: float = 10.0) -> bool:
        """Ожидание входящего соединения (для сервера)."""
        with self._lock:
            if self._state != RudpState.CLOSED:
                return False
            self._state = RudpState.CONNECTING

        start = time.time()
        while time.time() - start < timeout:
            with self._lock:
                if self._state == RudpState.ESTABLISHED:
                    self._start_worker()
                    return True
            time.sleep(0.01)

        with self._lock:
            self._state = RudpState.CLOSED
        return False

    def _start_worker(self) -> None:
        if self._worker_thread and self._worker_thread.is_alive():
            return
        self._worker_running = True
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()

    def _worker_loop(self) -> None:
        """Фоновый поток: обработка входящих пакетов, таймауты, ACK."""
        while self._worker_running and not self._closed:
            try:
                # select с таймаутом для проверки закрытия
                r, _, _ = select.select([self.sock], [], [], 0.05)
                if r:
                    self._handle_receive()
                self._check_timeouts()
                self._send_pending_acks()
            except Exception as e:
                if self._worker_running:
                    print(f"[RUDP] Worker error: {e}")
        self._worker_running = False

    def _handle_receive(self) -> None:
        try:
            data, addr = self.sock.recvfrom(65536)
        except (BlockingIOError, OSError):
            return

        # Фильтр по адресу (для ЛР4 MSG_PEEK)
        if self._peer_filter and addr != self._peer_filter:
            return

        if len(data) < 5:
            return

        try:
            pkt = Packet.unpack(data)
        except ValueError:
            return

        with self._lock:
            if pkt.ptype == PacketType.DATA:
                self._handle_data(pkt, addr)
            elif pkt.ptype == PacketType.ACK:
                self._handle_ack(pkt)
            elif pkt.ptype == PacketType.NACK:
                self._handle_nack(pkt)
            elif pkt.ptype == PacketType.FIN:
                self._handle_fin(pkt, addr)
            elif pkt.ptype == PacketType.CMD:
                self._handle_cmd(pkt, addr)

    def _handle_data(self, pkt: Packet, addr: Tuple[str, int]) -> None:
        """Обработка DATA пакета: sliding window приём."""
        self.packets_received += 1

        if self._state == RudpState.CONNECTING and pkt.payload == b"SYN":
            # Входящий SYN
            self.dest_addr = addr
            self._expected_seq = pkt.seq + 1
            self._recv_seq = pkt.seq
            syn_ack = Packet(self._send_seq, PacketType.CMD, b"SYN-ACK")
            self._send_packet(syn_ack)
            self._send_seq += 1
            self._state = RudpState.ESTABLISHED
            return

        if self._state != RudpState.ESTABLISHED:
            return

        # Обновляем dest_addr если не установлен
        if self.dest_addr is None:
            self.dest_addr = addr

        # Sliding window приём
        if pkt.seq == self._expected_seq:
            # В порядке
            self._deliver_data(pkt.payload)
            self._expected_seq += 1

            # Проверяем буфер на следующие пакеты
            while self._expected_seq in self._recv_buffer:
                payload = self._recv_buffer.pop(self._expected_seq)
                self._deliver_data(payload)
                self._expected_seq += 1

            # Отправляем ACK
            self._send_ack(self._expected_seq)

        elif pkt.seq > self._expected_seq:
            # Out-of-order — буферизуем если в окне
            if pkt.seq < self._expected_seq + self.config.window_size:
                if pkt.seq not in self._recv_buffer:
                    self._recv_buffer[pkt.seq] = pkt.payload
            # NACK для запроса пропущенного
            nack = Packet(self._expected_seq, PacketType.NACK)
            self._send_packet(nack)

        else:
            # Старый пакет (дубль) — просто ACK
            self._send_ack(self._expected_seq)

    def _handle_ack(self, pkt: Packet) -> None:
        """Обработка ACK: sliding window отправки."""
        ack_seq = pkt.seq

        if ack_seq <= self._send_base:
            # Дубликат ACK
            if ack_seq == self._last_ack_seq:
                self.dup_acks += 1
                if self.dup_acks >= 3:
                    # Fast retransmit
                    self._fast_retransmit(ack_seq)
            else:
                self.dup_acks = 1
            self._last_ack_seq = ack_seq
            return

        self.dup_acks = 0
        self._last_ack_seq = ack_seq

        # Кумулятивный ACK: подтверждаем все до ack_seq
        newly_acked = 0
        for seq in list(self._sent_packets.keys()):
            if seq < ack_seq:
                sp = self._sent_packets.pop(seq)
                if not sp.acked:
                    sp.acked = True
                    newly_acked += 1
                    # RTT estimation
                    rtt = time.time() - sp.send_time
                    self._update_rto(rtt)

        if newly_acked > 0:
            self._send_base = ack_seq
            with self._send_cv:
                self._send_cv.notify_all()

    def _handle_nack(self, pkt: Packet) -> None:
        """Обработка NACK: немедленная ретрансляция запрашиваемого пакета."""
        nack_seq = pkt.seq
        if nack_seq in self._sent_packets:
            sp = self._sent_packets[nack_seq]
            if not sp.acked:
                self._retransmit(sp)

    def _handle_fin(self, pkt: Packet, addr: Tuple[str, int]) -> None:
        """Обработка FIN."""
        fin_ack = Packet(pkt.seq + 1, PacketType.ACK)
        self._send_packet(fin_ack)
        with self._lock:
            self._state = RudpState.CLOSED
            self._closed = True

    def _handle_cmd(self, pkt: Packet, addr: Tuple[str, int]) -> None:
        """Обработка команд (SYN, SYN-ACK)."""
        if pkt.payload == b"SYN":
            self.dest_addr = addr
            self._expected_seq = pkt.seq + 1
            syn_ack = Packet(self._send_seq, PacketType.CMD, b"SYN-ACK")
            self._send_packet(syn_ack)
            self._send_seq += 1
            with self._lock:
                self._state = RudpState.ESTABLISHED
        elif pkt.payload == b"SYN-ACK":
            with self._lock:
                self._state = RudpState.ESTABLISHED

    def _deliver_data(self, data: bytes) -> None:
        if self._on_data_received and data:
            try:
                self._on_data_received(data)
            except Exception as e:
                print(f"[RUDP] Data callback error: {e}")

    def _send_ack(self, ack_seq: int) -> None:
        if self.dest_addr is None:
            return
        ack = Packet(ack_seq, PacketType.ACK)
        self._send_packet(ack)

    def _send_pending_acks(self) -> None:
        """Периодическая отправка ACK (каждые N пакетов или по таймеру)."""
        # В данной реализации ACK отправляется сразу при приёме DATA
        pass

    def _check_timeouts(self) -> None:
        """Проверка таймаутов ретрансляции."""
        now = time.time()
        with self._lock:
            for seq, sp in list(self._sent_packets.items()):
                if sp.acked:
                    continue
                if now - sp.send_time >= self._rto:
                    if sp.retries >= self.config.max_retries:
                        # Превышено число попыток — разрыв соединения
                        self._connection_lost()
                        return
                    self._retransmit(sp)

    def _retransmit(self, sp: SentPacket) -> None:
        sp.send_time = time.time()
        sp.retries += 1
        self.retransmissions += 1
        try:
            self.sock.sendto(sp.packet.pack(), self.dest_addr)
        except OSError:
            pass

    def _fast_retransmit(self, seq: int) -> None:
        """Fast retransmit по 3 DUP ACK."""
        if seq in self._sent_packets:
            sp = self._sent_packets[seq]
            if not sp.acked:
                self._retransmit(sp)
                # Уменьшаем окно (TCP Tahoe style)
                self._send_window = max(self._send_window // 2, 1)

    def _update_rto(self, rtt: float) -> None:
        """Jacobson/Karels RTO estimation."""
        if self._srtt == 0:
            self._srtt = rtt
            self._rttvar = rtt / 2
        else:
            self._rttvar = self.config.rto_beta * self._rttvar + self.config.rto_alpha * abs(self._srtt - rtt)
            self._srtt = self.config.rto_alpha * self._srtt + (1 - self.config.rto_alpha) * rtt
        self._rto = min(max(self._srtt + 4 * self._rttvar, self.config.min_rto), self.config.max_rto)

    def _connection_lost(self) -> None:
        with self._lock:
            self._state = RudpState.CLOSED
            self._closed = True
            if self._on_connection_lost:
                try:
                    self._on_connection_lost()
                except Exception:
                    pass

    def _send_packet(self, pkt: Packet) -> bool:
        if self.dest_addr is None:
            return False
        try:
            self.sock.sendto(pkt.pack(), self.dest_addr)
            self.packets_sent += 1
            return True
        except OSError:
            return False

    # ═══════════════════════════════════════════════════
    #  Публичный API для отправки/получения потока данных
    # ═══════════════════════════════════════════════════

    def send_stream(
        self,
        reader: Callable[[int], bytes],
        total_size: int,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> None:
        """Отправка файла через RUDP с sliding window."""
        if self.dest_addr is None:
            raise ConnectionLostError("Not connected")

        with self._lock:
            if self._state != RudpState.ESTABLISHED:
                raise ConnectionLostError("Not established")

        sent = 0
        last_progress = 0

        while sent < total_size:
            with self._send_cv:
                # Ждём места в окне
                while self._next_seq - self._send_base >= self._send_window:
                    if self._closed:
                        raise ConnectionLostError("Connection lost")
                    self._send_cv.wait(timeout=self._rto)
                    if self._closed:
                        raise ConnectionLostError("Connection lost")

                # Читаем данные
                chunk_size = min(self.config.payload_size, total_size - sent)
                chunk = reader(chunk_size)
                if not chunk:
                    break

                # Создаём и отправляем пакет
                pkt = Packet(self._next_seq, PacketType.DATA, chunk)
                sp = SentPacket(pkt, time.time())
                self._sent_packets[self._next_seq] = sp

                if not self._send_packet(pkt):
                    raise ConnectionLostError("Send failed")

                self._next_seq += 1
                sent += len(chunk)

            # Прогресс
            if progress_callback and sent - last_progress > 1024 * 1024:
                progress_callback(sent)
                last_progress = sent

        # Ждём подтверждения всех пакетов
        with self._send_cv:
            while self._send_base < self._next_seq:
                if self._closed:
                    raise ConnectionLostError("Connection lost")
                self._send_cv.wait(timeout=self._rto * 2)

        # Отправляем FIN
        fin = Packet(self._next_seq, PacketType.FIN)
        self._send_packet(fin)
        self._next_seq += 1

        if progress_callback:
            progress_callback(total_size)

    def recv_stream(
        self,
        writer: Callable[[bytes], None],
        total_size: int = 0,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> int:
        """Получение файла через RUDP."""
        # Для получения используем callback, который вызывается в _handle_data
        # Здесь просто ждём завершения (FIN или total_size)
        received = 0
        start_time = time.time()

        def data_cb(data: bytes):
            nonlocal received
            writer(data)
            received += len(data)
            if progress_callback:
                progress_callback(received)

        self.set_data_callback(data_cb)

        # Ждём завершения
        while True:
            with self._lock:
                if self._closed:
                    break
                if total_size > 0 and received >= total_size:
                    break
            time.sleep(0.1)
            if time.time() - start_time > 300:  # 5 мин общий таймаут
                break

        return received

    def send_command(self, text: str, timeout: float = 10.0) -> Optional[str]:
        """Отправка команды (CMD пакет) с ожиданием ответа."""
        if self.dest_addr is None:
            raise RuntimeError("dest_addr not set")

        response = None
        response_event = threading.Event()

        def cmd_callback(data: bytes):
            nonlocal response
            response = data.decode(errors="ignore").strip()
            response_event.set()

        old_callback = self._on_data_received
        self.set_data_callback(cmd_callback)

        try:
            cmd_pkt = Packet(self._send_seq, PacketType.CMD, (text + "\n").encode())
            self._send_packet(cmd_pkt)
            self._send_seq += 1

            if not response_event.wait(timeout):
                return None
            return response
        finally:
            self.set_data_callback(old_callback)

    def close(self) -> None:
        """Закрытие соединения."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._worker_running = False
            if self._state == RudpState.ESTABLISHED:
                fin = Packet(self._next_seq, PacketType.FIN)
                self._send_packet(fin)
            self._state = RudpState.CLOSED

    def is_connected(self) -> bool:
        with self._lock:
            return self._state == RudpState.ESTABLISHED and not self._closed


def create_rudp_socket(
    bind_addr: Tuple[str, int] = ("0.0.0.0", 0),
    dest_addr: Optional[Tuple[str, int]] = None,
    buffer_size: int = 8 * 1024 * 1024,
) -> Tuple[socket.socket, RudpSocket]:
    """Создаёт UDP сокет и RUDP обёртку."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Увеличиваем буферы
    for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
        target = buffer_size
        while target >= 256 * 1024:
            try:
                sock.setsockopt(socket.SOL_SOCKET, opt, target)
                break
            except OSError:
                target //= 2

    sock.setblocking(False)
    sock.bind(bind_addr)

    rudp = RudpSocket(sock, dest_addr)
    return sock, rudp