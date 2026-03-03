"""Aggressive Reliable UDP для локальной сети.

Пакет 8KB, окно 4096 пакетов (≈32 MB), ACK раз в 64 пакета.
Заточено под максимальную скорость на низком RTT.
"""

import socket
import struct
import time
import select
from typing import Optional, Tuple, Dict, Callable

from .protocol import (
    UDP_PAYLOAD_SIZE,
    UDP_HEADER_SIZE,
    UDP_WINDOW_SIZE,
    UDP_TIMEOUT,
    PacketType,
    UDP_RETRY_LIMIT,
)


_HDR = struct.Struct("!IB")
_BURST = 1024          # до 1024 пакетов за итерацию
_ACK_EVERY = 64        # ACK раз в 64 пакета
_READ_CHUNK = 4 * 1024 * 1024


class RUDPSocket:
    def __init__(self, sock: socket.socket, dest_addr: Optional[Tuple[str, int]] = None):
        self.sock = sock
        self.dest_addr = dest_addr

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
        for _ in range(4):
            try:
                self.sock.sendto(data, addr)
                return True
            except (BlockingIOError, InterruptedError):
                time.sleep(0.00001)
            except OSError:
                return False
        return False

    # ── командный канал ───────────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")

        addr = self.dest_addr
        pkt = self._pack(0, PacketType.CMD.value, text.encode())

        for _ in range(UDP_RETRY_LIMIT):
            self._send(pkt, addr)
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
                data = rp[UDP_HEADER_SIZE:]
                msg = data.decode(errors="ignore")
                if msg == "ACK_CMD":
                    continue
                return msg
        return None

    # ── send_stream: максимально агрессивная отправка ─────

    def send_stream(self, reader, total_size: int,
                    progress_callback: Callable[[int], None] = None) -> None:
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")

        addr = self.dest_addr
        sock = self.sock
        sendto = sock.sendto

        base = 0
        next_seq = 0
        packets: Dict[int, bytes] = {}
        cursor = 0
        eof = False
        last_ack = time.monotonic()
        win = UDP_WINDOW_SIZE
        last_prog = 0

        while cursor < total_size or base < next_seq:
            # 1. Заполняем окно
            can_send = min(_BURST, base + win - next_seq)
            n = 0
            while not eof and n < can_send:
                chunk = reader.read(UDP_PAYLOAD_SIZE)
                if not chunk:
                    eof = True
                    cursor = total_size
                    break
                pkt = self._pack(next_seq, PacketType.DATA.value, chunk)
                packets[next_seq] = pkt
                try:
                    sendto(pkt, addr)
                except (BlockingIOError, InterruptedError):
                    time.sleep(0.00001)
                    try:
                        sendto(pkt, addr)
                    except OSError:
                        pass
                except OSError:
                    pass
                next_seq += 1
                cursor += len(chunk)
                n += 1

            if progress_callback and cursor - last_prog > max(total_size // 100, 1):
                progress_callback(min(cursor, total_size))
                last_prog = cursor

            # 2. Читаем ACK'и
            moved = False
            while True:
                r, _, _ = select.select([sock], [], [], 0)
                if not r:
                    break
                try:
                    ap, _ = sock.recvfrom(64)
                except OSError:
                    break
                if len(ap) < UDP_HEADER_SIZE:
                    continue
                s, t = _HDR.unpack_from(ap)
                if t != PacketType.ACK.value:
                    continue
                if s > base:
                    for k in range(base, s):
                        packets.pop(k, None)
                    base = s
                    last_ack = time.monotonic()
                    moved = True

            if moved:
                continue

            # 3. Таймаут — переотправка части окна
            now = time.monotonic()
            if packets and now - last_ack > UDP_TIMEOUT:
                cnt = 0
                for k in sorted(packets.keys()):
                    try:
                        sendto(packets[k], addr)
                    except OSError:
                        pass
                    cnt += 1
                    if cnt >= 512:
                        break
                last_ack = now
            else:
                time.sleep(0.0001)

        # FIN
        fin_seq = next_seq
        fin_pkt = self._pack(fin_seq, PacketType.FIN.value)
        for _ in range(25):
            self._send(fin_pkt, addr)
            r, _, _ = select.select([sock], [], [], 0.2)
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

    # ── recv_stream: быстрый приём ────────────────────────

    def recv_stream(self, writer, total_size: int = 0,
                    progress_callback: Callable[[int], None] = None) -> int:
        sock = self.sock

        expected = 0
        ooo: Dict[int, bytes] = {}
        total = 0
        last_pkt = time.monotonic()
        last_ack = time.monotonic()
        cnt_ack = 0
        write_buf = bytearray()
        FLUSH = 1024 * 1024

        while True:
            now = time.monotonic()
            if now - last_pkt > 30.0:
                break

            r, _, _ = select.select([sock], [], [], 0.05)
            if not r:
                if now - last_ack > 0.05 and self.dest_addr is not None:
                    self._send(_HDR.pack(expected, PacketType.ACK.value), self.dest_addr)
                    last_ack = now
                continue

            try:
                pkt, addr = sock.recvfrom(65536)
            except OSError:
                continue

            if self.dest_addr is None:
                self.dest_addr = addr
            elif addr != self.dest_addr:
                continue

            if len(pkt) < UDP_HEADER_SIZE:
                continue

            seq, ptype = _HDR.unpack_from(pkt)
            last_pkt = time.monotonic()

            if ptype == PacketType.CMD.value:
                continue

            if ptype == PacketType.FIN.value:
                if write_buf:
                    writer.write(bytes(write_buf))
                    write_buf.clear()
                ack = _HDR.pack(seq + 1, PacketType.ACK.value)
                for _ in range(5):
                    self._send(ack, addr)
                return total

            if ptype != PacketType.DATA.value:
                continue

            data = pkt[UDP_HEADER_SIZE:]

            if seq == expected:
                write_buf.extend(data)
                total += len(data)
                expected += 1
                cnt_ack += 1

                while expected in ooo:
                    d = ooo.pop(expected)
                    write_buf.extend(d)
                    total += len(d)
                    expected += 1
                    cnt_ack += 1

                if len(write_buf) >= FLUSH:
                    writer.write(bytes(write_buf))
                    write_buf.clear()

                if progress_callback:
                    progress_callback(total)

            elif seq > expected and seq < expected + UDP_WINDOW_SIZE * 4:
                ooo.setdefault(seq, data)
                cnt_ack = _ACK_EVERY

            if cnt_ack >= _ACK_EVERY or time.monotonic() - last_ack > 0.01:
                self._send(_HDR.pack(expected, PacketType.ACK.value), addr)
                cnt_ack = 0
                last_ack = time.monotonic()

        if write_buf:
            writer.write(bytes(write_buf))
        return total
        """Aggressive Reliable UDP для локальной сети.

        Пакет 8KB, окно 4096 пакетов (≈32 MB), ACK раз в 64 пакета.
        Заточено под максимальную скорость на низком RTT.
        """

        import socket
        import struct
        import time
        import select
        from typing import Optional, Tuple, Dict, Callable

        from .protocol import (
            UDP_PAYLOAD_SIZE,
            UDP_HEADER_SIZE,
            UDP_WINDOW_SIZE,
            UDP_TIMEOUT,
            PacketType,
            UDP_RETRY_LIMIT,
        )

        _HDR = struct.Struct("!IB")
        _BURST = 1024  # до 1024 пакетов за итерацию
        _ACK_EVERY = 64  # ACK раз в 64 пакета
        _READ_CHUNK = 4 * 1024 * 1024

        class RUDPSocket:
            def __init__(self, sock: socket.socket, dest_addr: Optional[Tuple[str, int]] = None):
                self.sock = sock
                self.dest_addr = dest_addr

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
                for _ in range(4):
                    try:
                        self.sock.sendto(data, addr)
                        return True
                    except (BlockingIOError, InterruptedError):
                        time.sleep(0.00001)
                    except OSError:
                        return False
                return False

            # ── командный канал ───────────────────────────────────

            def send_command(self, text: str) -> Optional[str]:
                if not self.dest_addr:
                    raise RuntimeError("dest_addr not set")

                addr = self.dest_addr
                pkt = self._pack(0, PacketType.CMD.value, text.encode())

                for _ in range(UDP_RETRY_LIMIT):
                    self._send(pkt, addr)
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
                        data = rp[UDP_HEADER_SIZE:]
                        msg = data.decode(errors="ignore")
                        if msg == "ACK_CMD":
                            continue
                        return msg
                return None

            # ── send_stream: максимально агрессивная отправка ─────

            def send_stream(self, reader, total_size: int,
                            progress_callback: Callable[[int], None] = None) -> None:
                if not self.dest_addr:
                    raise RuntimeError("dest_addr not set")

                addr = self.dest_addr
                sock = self.sock
                sendto = sock.sendto

                base = 0
                next_seq = 0
                packets: Dict[int, bytes] = {}
                cursor = 0
                eof = False
                last_ack = time.monotonic()
                win = UDP_WINDOW_SIZE
                last_prog = 0

                while cursor < total_size or base < next_seq:
                    # 1. Заполняем окно
                    can_send = min(_BURST, base + win - next_seq)
                    n = 0
                    while not eof and n < can_send:
                        chunk = reader.read(UDP_PAYLOAD_SIZE)
                        if not chunk:
                            eof = True
                            cursor = total_size
                            break
                        pkt = self._pack(next_seq, PacketType.DATA.value, chunk)
                        packets[next_seq] = pkt
                        try:
                            sendto(pkt, addr)
                        except (BlockingIOError, InterruptedError):
                            time.sleep(0.00001)
                            try:
                                sendto(pkt, addr)
                            except OSError:
                                pass
                        except OSError:
                            pass
                        next_seq += 1
                        cursor += len(chunk)
                        n += 1

                    if progress_callback and cursor - last_prog > max(total_size // 100, 1):
                        progress_callback(min(cursor, total_size))
                        last_prog = cursor

                    # 2. Читаем ACK'и
                    moved = False
                    while True:
                        r, _, _ = select.select([sock], [], [], 0)
                        if not r:
                            break
                        try:
                            ap, _ = sock.recvfrom(64)
                        except OSError:
                            break
                        if len(ap) < UDP_HEADER_SIZE:
                            continue
                        s, t = _HDR.unpack_from(ap)
                        if t != PacketType.ACK.value:
                            continue
                        if s > base:
                            for k in range(base, s):
                                packets.pop(k, None)
                            base = s
                            last_ack = time.monotonic()
                            moved = True

                    if moved:
                        continue

                    # 3. Таймаут — переотправка части окна
                    now = time.monotonic()
                    if packets and now - last_ack > UDP_TIMEOUT:
                        cnt = 0
                        for k in sorted(packets.keys()):
                            try:
                                sendto(packets[k], addr)
                            except OSError:
                                pass
                            cnt += 1
                            if cnt >= 512:
                                break
                        last_ack = now
                    else:
                        time.sleep(0.0001)

                # FIN
                fin_seq = next_seq
                fin_pkt = self._pack(fin_seq, PacketType.FIN.value)
                for _ in range(25):
                    self._send(fin_pkt, addr)
                    r, _, _ = select.select([sock], [], [], 0.2)
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

            # ── recv_stream: быстрый приём ────────────────────────

            def recv_stream(self, writer, total_size: int = 0,
                            progress_callback: Callable[[int], None] = None) -> int:
                sock = self.sock

                expected = 0
                ooo: Dict[int, bytes] = {}
                total = 0
                last_pkt = time.monotonic()
                last_ack = time.monotonic()
                cnt_ack = 0
                write_buf = bytearray()
                FLUSH = 1024 * 1024

                while True:
                    now = time.monotonic()
                    if now - last_pkt > 30.0:
                        break

                    r, _, _ = select.select([sock], [], [], 0.05)
                    if not r:
                        if now - last_ack > 0.05 and self.dest_addr is not None:
                            self._send(_HDR.pack(expected, PacketType.ACK.value), self.dest_addr)
                            last_ack = now
                        continue

                    try:
                        pkt, addr = sock.recvfrom(65536)
                    except OSError:
                        continue

                    if self.dest_addr is None:
                        self.dest_addr = addr
                    elif addr != self.dest_addr:
                        continue

                    if len(pkt) < UDP_HEADER_SIZE:
                        continue

                    seq, ptype = _HDR.unpack_from(pkt)
                    last_pkt = time.monotonic()

                    if ptype == PacketType.CMD.value:
                        continue

                    if ptype == PacketType.FIN.value:
                        if write_buf:
                            writer.write(bytes(write_buf))
                            write_buf.clear()
                        ack = _HDR.pack(seq + 1, PacketType.ACK.value)
                        for _ in range(5):
                            self._send(ack, addr)
                        return total

                    if ptype != PacketType.DATA.value:
                        continue

                    data = pkt[UDP_HEADER_SIZE:]

                    if seq == expected:
                        write_buf.extend(data)
                        total += len(data)
                        expected += 1
                        cnt_ack += 1

                        while expected in ooo:
                            d = ooo.pop(expected)
                            write_buf.extend(d)
                            total += len(d)
                            expected += 1
                            cnt_ack += 1

                        if len(write_buf) >= FLUSH:
                            writer.write(bytes(write_buf))
                            write_buf.clear()

                        if progress_callback:
                            progress_callback(total)

                    elif seq > expected and seq < expected + UDP_WINDOW_SIZE * 4:
                        ooo.setdefault(seq, data)
                        cnt_ack = _ACK_EVERY

                    if cnt_ack >= _ACK_EVERY or time.monotonic() - last_ack > 0.01:
                        self._send(_HDR.pack(expected, PacketType.ACK.value), addr)
                        cnt_ack = 0
                        last_ack = time.monotonic()

                if write_buf:
                    writer.write(bytes(write_buf))
                return total
