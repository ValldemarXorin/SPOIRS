"""
NACK-based Reliable UDP — blast protocol.

SEND: fire all packets as fast as possible (no waiting for ACK).
RECV: collect packets, track gaps.
After FIN: receiver sends NACK (list of missing seqs).
Sender retransmits only missing packets.
Repeat until all received → DONE.

On loopback: typically 0 losses → 1 pass → max throughput.
"""

import socket
import struct
import time
import select
import io
from typing import Optional, Tuple, Dict, Callable, Set

from .protocol import (
    UDP_PAYLOAD_SIZE, UDP_HEADER_SIZE,
    UDP_TIMEOUT, PacketType, UDP_RETRY_LIMIT,
)

_HDR = struct.Struct("!IB")
_READ_CHUNK = 4 * 1024 * 1024

# Throttle: max packets to send before a micro-yield
# Prevents OS buffer overflow while keeping speed high
_SEND_BURST = 2048


class RUDPSocket:

    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock      = sock
        self.dest_addr = dest_addr
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try: self.sock.setsockopt(socket.SOL_SOCKET, opt, 8 * 1024 * 1024)
            except OSError: pass

    def _pack(self, seq: int, ptype: int, data: bytes = b"") -> bytes:
        return _HDR.pack(seq, ptype) + data

    def _send(self, data: bytes, addr: tuple) -> bool:
        try:
            self.sock.sendto(data, addr); return True
        except BlockingIOError:
            time.sleep(0.0001)
            try: self.sock.sendto(data, addr); return True
            except: return False
        except: return False

    # ── CMD ────────────────────────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
        pkt = self._pack(0, PacketType.CMD.value, text.encode())
        ack = False
        for _ in range(UDP_RETRY_LIMIT):
            if not ack and self.dest_addr: self._send(pkt, self.dest_addr)
            wait = 1.0 if ack else 0.3; t0 = time.monotonic()
            while time.monotonic() - t0 < wait:
                r, _, _ = select.select([self.sock], [], [], 0.05)
                if not r: continue
                try: rp, ra = self.sock.recvfrom(65536)
                except: break
                if self.dest_addr and ra != self.dest_addr: continue
                if len(rp) < _HDR.size: continue
                _, rt = _HDR.unpack_from(rp)
                if rt != PacketType.CMD.value: continue
                d = rp[_HDR.size:].decode(errors="ignore")
                if d == "ACK_CMD": ack = True; continue
                return d
        return None

    def recv_command(self) -> Tuple[str, Tuple[str, int]]:
        try: pkt, addr = self.sock.recvfrom(65536)
        except: return "", ("", 0)
        if len(pkt) < _HDR.size: return "", addr
        s, t = _HDR.unpack_from(pkt)
        if t != PacketType.CMD.value: return "", addr
        msg = pkt[_HDR.size:].decode(errors="ignore")
        if msg.startswith("OK ") or msg.startswith("ERROR "): return "", addr
        self._send(self._pack(s, PacketType.CMD.value, b"ACK_CMD"), addr)
        return msg, addr

    # ── send_stream (BLAST + NACK retransmit) ──────────────

    def send_stream(self, reader, total_size: int,
                    progress_callback: Callable[[int], None] = None) -> None:
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")
        addr   = self.dest_addr
        sock   = self.sock
        sendto = sock.sendto
        psize  = UDP_PAYLOAD_SIZE
        DATA   = PacketType.DATA.value
        FIN    = PacketType.FIN.value
        NACK   = PacketType.NACK.value
        DONE   = PacketType.DONE.value

        # Phase 1: read entire file into packet list
        packets = []
        seq = 0
        cursor = 0
        file_buf = b""
        fb_pos = 0
        while cursor < total_size:
            if fb_pos >= len(file_buf):
                file_buf = reader.read(_READ_CHUNK)
                fb_pos = 0
                if not file_buf: break
            end = min(fb_pos + psize, len(file_buf))
            chunk = file_buf[fb_pos:end]
            fb_pos = end
            packets.append(_HDR.pack(seq, DATA) + chunk)
            seq += 1
            cursor += len(chunk)
        total_pkts = seq

        # Phase 2: blast all packets
        for i in range(total_pkts):
            try: sendto(packets[i], addr)
            except BlockingIOError:
                time.sleep(0.00005)
                try: sendto(packets[i], addr)
                except: pass
            except: pass
            if i > 0 and i % _SEND_BURST == 0:
                time.sleep(0.0001)  # micro-yield to not overflow OS buffer
                if progress_callback:
                    progress_callback(min((i + 1) * psize, total_size))

        if progress_callback:
            progress_callback(total_size)

        # Phase 3: send FIN, wait for NACK or DONE
        fin_pkt = _HDR.pack(total_pkts, FIN)
        for round_num in range(50):
            # Send FIN
            for _ in range(3):
                self._send(fin_pkt, addr)

            # Wait for response (NACK or DONE)
            deadline = time.monotonic() + UDP_TIMEOUT
            while time.monotonic() < deadline:
                r, _, _ = select.select([sock], [], [], 0.05)
                if not r: continue
                try: rp, ra = sock.recvfrom(65536)
                except: continue
                if ra != addr: continue
                if len(rp) < _HDR.size: continue
                rs, rt = _HDR.unpack_from(rp)

                if rt == DONE:
                    return  # All received!

                if rt == NACK:
                    # Parse NACK: payload = list of 4-byte seq numbers
                    nack_data = rp[_HDR.size:]
                    missing_count = len(nack_data) // 4
                    missing = struct.unpack(f"!{missing_count}I", nack_data[:missing_count*4])

                    # Retransmit missing packets
                    for ms in missing:
                        if 0 <= ms < total_pkts:
                            try: sendto(packets[ms], addr)
                            except BlockingIOError:
                                time.sleep(0.00005)
                                try: sendto(packets[ms], addr)
                                except: pass
                            except: pass
                    # Send FIN again after retransmit
                    break  # go to next round

    # ── recv_stream (collect + NACK) ───────────────────────

    def recv_stream(self, writer, total_size: int = 0,
                    progress_callback: Callable[[int], None] = None) -> int:
        sock     = self.sock
        DATA     = PacketType.DATA.value
        FIN      = PacketType.FIN.value
        NACK     = PacketType.NACK.value
        DONE     = PacketType.DONE.value

        received: Dict[int, bytes] = {}
        total_pkts = None
        total_bytes = 0
        last_pkt = time.monotonic()
        addr = self.dest_addr

        while True:
            now = time.monotonic()
            if now - last_pkt > 30.0: break

            r, _, _ = select.select([sock], [], [], 0.05)
            if not r: continue

            # Drain all available packets
            while True:
                try: pkt, paddr = sock.recvfrom(65536)
                except: break

                if addr is None:
                    addr = paddr
                    self.dest_addr = addr
                elif paddr != addr: continue

                if len(pkt) < _HDR.size: continue
                s, t = _HDR.unpack_from(pkt)
                last_pkt = time.monotonic()

                if t == PacketType.CMD.value: continue

                if t == DATA:
                    if s not in received:
                        received[s] = pkt[_HDR.size:]
                    continue

                if t == FIN:
                    total_pkts = s
                    # Check completeness
                    if total_pkts is not None:
                        missing = []
                        for i in range(total_pkts):
                            if i not in received:
                                missing.append(i)

                        if not missing:
                            # All received! Write to file and send DONE
                            for i in range(total_pkts):
                                data = received.get(i, b"")
                                writer.write(data)
                                total_bytes += len(data)
                            if progress_callback:
                                progress_callback(total_bytes)
                            done_pkt = self._pack(0, DONE)
                            for _ in range(5):
                                self._send(done_pkt, addr)
                            return total_bytes
                        else:
                            # Send NACK with missing seq numbers
                            # Max ~1000 seqs per NACK packet (4000 bytes)
                            for batch_start in range(0, len(missing), 1000):
                                batch = missing[batch_start:batch_start+1000]
                                nack_payload = struct.pack(f"!{len(batch)}I", *batch)
                                nack_pkt = self._pack(len(missing), NACK, nack_payload)
                                self._send(nack_pkt, addr)
                    continue

                r2, _, _ = select.select([sock], [], [], 0)
                if not r2: break

        # Timeout: write what we have
        if received:
            max_seq = max(received.keys()) + 1 if received else 0
            for i in range(max_seq):
                data = received.get(i, b"")
                if data:
                    writer.write(data)
                    total_bytes += len(data)
        return total_bytes
