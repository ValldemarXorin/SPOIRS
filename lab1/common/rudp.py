"""Модуль Reliable UDP (скользящее окно) — оптимизированная версия."""

import socket
import struct
import time
import select
from typing import Optional, Tuple, Dict, Callable

from .protocol import (
    UDP_PAYLOAD_SIZE, UDP_HEADER_SIZE, UDP_WINDOW_SIZE,
    UDP_TIMEOUT, PacketType, UDP_RETRY_LIMIT,
)


class RUDPSocket:
    """Reliable UDP: sliding-window ARQ."""

    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock = sock
        self.dest_addr = dest_addr
        try:
            buf = 64 * 1024 * 1024          # 64 MB OS-буфер
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buf)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buf)
        except socket.error:
            pass

    # ── helpers ────────────────────────────────────────────

    def _pack_packet(self, seq_num: int, p_type_val: int, data: bytes) -> bytes:
        return struct.pack('!IB', seq_num, p_type_val) + data

    def _unpack_header(self, packet: bytes):
        if len(packet) < UDP_HEADER_SIZE:
            return -1, -1, b""
        seq_num, type_val = struct.unpack('!IB', packet[:UDP_HEADER_SIZE])
        return seq_num, type_val, packet[UDP_HEADER_SIZE:]

    def _sendto_safe(self, data: bytes, addr: tuple) -> bool:
        for _ in range(10):
            try:
                self.sock.sendto(data, addr)
                return True
            except BlockingIOError:
                time.sleep(0.0002)
                continue
            except OSError as e:
                if getattr(e, "errno", None) in (11, 10035):
                    time.sleep(0.0002)
                    continue
                return False
        return False

    def _save_restore_sock(self):
        """Контекстный менеджер — сохраняет/восстанавливает blocking+timeout."""
        import contextlib

        @contextlib.contextmanager
        def _ctx():
            orig_block = self.sock.getblocking()
            orig_to    = self.sock.gettimeout()
            try:
                yield
            finally:
                try:
                    self.sock.setblocking(orig_block)
                    self.sock.settimeout(orig_to)
                except OSError:
                    pass

        return _ctx()

    # ── CMD ────────────────────────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
        data = text.encode()
        pkt  = self._pack_packet(0, PacketType.CMD.value, data)
        ack_received = False

        with self._save_restore_sock():
            self.sock.setblocking(False)

            for _ in range(UDP_RETRY_LIMIT):
                if not ack_received and self.dest_addr:
                    self._sendto_safe(pkt, self.dest_addr)

                wait = 1.0 if ack_received else 0.3
                t0   = time.time()

                while time.time() - t0 < wait:
                    ready = select.select([self.sock], [], [], 0.05)
                    if not ready[0]:
                        continue
                    try:
                        resp_pkt, addr = self.sock.recvfrom(65536)
                    except (BlockingIOError, OSError):
                        break
                    if self.dest_addr and addr != self.dest_addr:
                        continue
                    _, r_type, r_data = self._unpack_header(resp_pkt)
                    if r_type != PacketType.CMD.value:
                        continue
                    decoded = r_data.decode(errors="ignore")
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
        seq, p_type, data = self._unpack_header(pkt)
        if p_type != PacketType.CMD.value:
            return "", addr
        msg = data.decode(errors="ignore")
        if msg.startswith("OK ") or msg.startswith("ERROR "):
            return "", addr
        resp = self._pack_packet(seq, PacketType.CMD.value, b"ACK_CMD")
        self._sendto_safe(resp, addr)
        return msg, addr

    # ── send_stream (клиент -> сервер, upload) ─────────────

    def send_stream(
        self,
        reader,
        total_size: int,
        progress_callback: Callable[[int], None] = None,
    ) -> None:
        """
        Sliding-window отправка.

        Ключевые изменения vs оригинал:
        - burst_limit = 512 (было 64)
        - ACK-читалка НЕБЛОКИРУЮЩАЯ (select timeout=0), без sleep
        - Сокет переключается в blocking только во внутреннем recvfrom,
          а весь остальной код использует select+nonblocking
        - Состояние сокета восстанавливается через finally
        """
        base         = 0
        next_seq_num = 0
        window_size  = UDP_WINDOW_SIZE
        packets: Dict[int, bytes] = {}
        file_cursor  = 0
        last_ack_time = time.time()

        # Увеличенный burst: отправляем до 512 пакетов за одну итерацию
        BURST = 512

        with self._save_restore_sock():
            try:
                self.sock.setblocking(False)
            except OSError:
                pass

            while file_cursor < total_size or base < next_seq_num:

                # ── фаза 1: отправить новые пакеты ──────────────
                sent_this_round = 0
                while (
                    next_seq_num < base + window_size
                    and file_cursor < total_size
                    and sent_this_round < BURST
                ):
                    chunk = reader.read(UDP_PAYLOAD_SIZE)
                    if not chunk:
                        file_cursor = total_size
                        break

                    pkt = self._pack_packet(
                        next_seq_num, PacketType.DATA.value, chunk
                    )
                    packets[next_seq_num] = pkt
                    if self.dest_addr:
                        self._sendto_safe(pkt, self.dest_addr)
                    next_seq_num += 1
                    file_cursor  += len(chunk)
                    sent_this_round += 1

                if progress_callback:
                    progress_callback(file_cursor)

                # ── фаза 2: вычитать ВСЕ доступные ACK (неблокирующий) ──
                while True:
                    ready = select.select([self.sock], [], [], 0)
                    if not ready[0]:
                        break
                    try:
                        ack_pkt, addr = self.sock.recvfrom(1024)
                    except (BlockingIOError, OSError):
                        break
                    if self.dest_addr and addr != self.dest_addr:
                        continue
                    ack_seq, p_type, _ = self._unpack_header(ack_pkt)
                    if p_type == PacketType.ACK.value and ack_seq > base:
                        for i in range(base, ack_seq):
                            packets.pop(i, None)
                        base = ack_seq
                        last_ack_time = time.time()

                # ── фаза 3: retransmit при timeout ──────────────
                now = time.time()
                if now - last_ack_time > UDP_TIMEOUT and packets and self.dest_addr:
                    resent = 0
                    for seq_r in range(base, next_seq_num):
                        if seq_r in packets:
                            self._sendto_safe(packets[seq_r], self.dest_addr)
                            resent += 1
                            if resent >= BURST:
                                break
                    last_ack_time = now

                # ── фаза 4: если окно заполнено — ждём ACK (короткий poll) ──
                if next_seq_num >= base + window_size and base < next_seq_num:
                    ready = select.select([self.sock], [], [], 0.005)
                    if ready[0]:
                        try:
                            ack_pkt, addr = self.sock.recvfrom(1024)
                        except (BlockingIOError, OSError):
                            pass
                        else:
                            if not (self.dest_addr and addr != self.dest_addr):
                                ack_seq, p_type, _ = self._unpack_header(ack_pkt)
                                if p_type == PacketType.ACK.value and ack_seq > base:
                                    for i in range(base, ack_seq):
                                        packets.pop(i, None)
                                    base = ack_seq
                                    last_ack_time = time.time()

            # ── FIN handshake ────────────────────────────────────
            if self.dest_addr:
                fin_seq = next_seq_num
                fin_pkt = self._pack_packet(fin_seq, PacketType.FIN.value, b"")
                for _ in range(20):
                    self._sendto_safe(fin_pkt, self.dest_addr)
                    ready = select.select([self.sock], [], [], 0.2)
                    if not ready[0]:
                        continue
                    try:
                        ack_pkt, addr = self.sock.recvfrom(1024)
                    except (BlockingIOError, OSError):
                        continue
                    if self.dest_addr and addr != self.dest_addr:
                        continue
                    ack_seq, p_type, _ = self._unpack_header(ack_pkt)
                    if p_type == PacketType.ACK.value and ack_seq == fin_seq + 1:
                        break

    # ── recv_stream (сервер -> клиент, download) ───────────

    def recv_stream(
        self,
        writer,
        total_size: int = 0,
        progress_callback: Callable[[int], None] = None,
    ) -> int:
        """
        Sliding-window приём.

        Ключевые изменения:
        - ack_interval = 2 (было 8) — ACK-и отправляются гораздо чаще
        - select timeout = 0.05s (было 0.5s) — быстрее реагируем на паузы
        - periodic ACK отправляется каждые 20ms (было 200ms)
        - Игнорируем CMD-пакеты от handshake
        """
        expected_seq     = 0
        received_buffer: Dict[int, bytes] = {}
        total_bytes      = 0
        last_pkt_time    = time.time()
        TIMEOUT_LIMIT    = 60.0

        ACK_INTERVAL     = 2          # ACK каждые N in-order пакетов
        packets_since_ack = 0
        last_ack_time    = time.time()

        with self._save_restore_sock():
            try:
                self.sock.setblocking(False)
            except OSError:
                pass

            while True:
                now = time.time()
                if now - last_pkt_time > TIMEOUT_LIMIT:
                    print("RUDP recv timeout")
                    break

                ready = select.select([self.sock], [], [], 0.05)

                # Периодически подтверждаем, чтобы сервер не застрял
                if not ready[0]:
                    if now - last_ack_time > 0.02 and self.dest_addr:
                        ack = self._pack_packet(
                            expected_seq, PacketType.ACK.value, b""
                        )
                        self._sendto_safe(ack, self.dest_addr)
                        last_ack_time = now
                    continue

                # Вычитываем ВСЕ доступные пакеты за один проход
                while True:
                    try:
                        pkt, addr = self.sock.recvfrom(65536)
                    except (BlockingIOError, OSError):
                        break

                    if self.dest_addr is None:
                        self.dest_addr = addr
                    elif addr != self.dest_addr:
                        continue

                    seq, p_type, data = self._unpack_header(pkt)
                    last_pkt_time = now

                    if p_type == PacketType.CMD.value:
                        continue          # остатки handshake

                    if p_type == PacketType.FIN.value:
                        ack = self._pack_packet(seq + 1, PacketType.ACK.value, b"")
                        for _ in range(3):
                            self._sendto_safe(ack, addr)
                        return total_bytes

                    if p_type != PacketType.DATA.value:
                        continue

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
                        packets_since_ack = ACK_INTERVAL  # форсируем ACK

                    # Отправляем ACK часто
                    now2 = time.time()
                    if (packets_since_ack >= ACK_INTERVAL
                            or now2 - last_ack_time > 0.02):
                        ack = self._pack_packet(
                            expected_seq, PacketType.ACK.value, b""
                        )
                        self._sendto_safe(ack, addr)
                        packets_since_ack = 0
                        last_ack_time = now2

        return total_bytes
