"""P2P Chat: main application class with recv/send loops."""

import socket
import select
import threading
import time
import sys
import uuid
from dataclasses import dataclass, field
from typing import Optional, Callable, List, Tuple, Set, Dict
from enum import Enum

from lab6.network import NetworkManager
from lab6.discovery import PeerDiscovery
from lab6.protocol import ChatMessage, MessageType, parse_message, format_timestamp


class SendMode(Enum):
    BROADCAST = "broadcast"
    MULTICAST = "multicast"


# Reliable delivery presets (RTO in seconds)
# Normal network: fast RTO, few retries.
NORMAL_PRESET = dict(initial_rto=1.0, min_rto=0.5, max_rto=30.0, max_retries=5, backoff=2.0)
# Low-throughput fallback: patient RTOs so we don't hammer a slow link,
# and many retries before declaring a message lost.
LOW_THROUGHPUT_PRESET = dict(initial_rto=5.0, min_rto=2.0, max_rto=120.0, max_retries=12, backoff=2.0)


@dataclass
class PendingMessage:
    """A sent chat message waiting for delivery confirmations (ACKs)."""
    msg: ChatMessage
    broadcast: bool
    recipients: List[str]          # peers this message must be delivered to
    created: float
    last_sent: float
    retries: int
    acked_by: Set[str] = field(default_factory=set)
    rto: float = 1.0


class P2PChat:
    """Main P2P chat application."""

    def __init__(
        self,
        port: int = 50000,
        multicast_group: str = "239.255.0.1",
        interface_name: Optional[str] = None,
        interface_ip: Optional[str] = None,
        name: str = "Anonymous",
        reliable: bool = True,
        low_throughput: bool = False,
        initial_rto: Optional[float] = None,
        min_rto: Optional[float] = None,
        max_rto: Optional[float] = None,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
    ):
        self.port = port
        self.multicast_group = multicast_group
        self.interface_name = interface_name
        self.interface_ip = interface_ip
        self.name = name
        self.instance_id = uuid.uuid4().hex[:12]  # Unique per-process instance

        self.network = NetworkManager(port, multicast_group, interface_name, interface_ip)
        self.discovery: Optional[PeerDiscovery] = None

        self._running = False
        self._recv_thread: Optional[threading.Thread] = None
        self._send_thread: Optional[threading.Thread] = None
        self._reliability_thread: Optional[threading.Thread] = None

        self._send_mode = SendMode.BROADCAST
        self._seq = 0
        self._output_callback: Optional[Callable[[str], None]] = None
        self._lock = threading.Lock()
        # Dedup: (instance_id, seq) seen recently (both sockets get same datagram)
        self._seen: set = set()
        self._seen_max = 1000

        # --- Reliable delivery (buffer + resend) ---
        self._reliable_enabled = reliable
        self._low_throughput = False
        # User-supplied overrides (survive preset switches via /slow)
        self._overrides: Dict[str, object] = {}
        if initial_rto is not None:
            self._overrides["initial_rto"] = initial_rto
        if min_rto is not None:
            self._overrides["min_rto"] = min_rto
        if max_rto is not None:
            self._overrides["max_rto"] = max_rto
        if max_retries is not None:
            self._overrides["max_retries"] = max_retries
        if backoff_factor is not None:
            self._overrides["backoff"] = backoff_factor
        self._apply_preset(NORMAL_PRESET)
        if low_throughput:
            self.set_low_throughput(True)
        self._rto = max(self._initial_rto, self._min_rto)
        # Jacobson/Karels RTT estimation state
        self._srtt: Optional[float] = None
        self._rttvar: Optional[float] = None
        # Buffer of sent messages awaiting ACKs: (instance_id, seq) -> PendingMessage
        self._pending: Dict[Tuple[str, int], PendingMessage] = {}
        self._pending_lock = threading.Lock()

    def set_output_callback(self, callback: Callable[[str], None]) -> None:
        """Set callback for output messages (for CLI integration)."""
        self._output_callback = callback

    def _output(self, msg: str) -> None:
        if self._output_callback:
            self._output_callback(msg)
        else:
            print(msg)

    def start(self) -> bool:
        """Initialize network and start chat loops."""
        if not self.network.initialize():
            return False

        local_ip = self.network.interface.ip if self.network.interface else "0.0.0.0"
        self.discovery = PeerDiscovery(
            local_ip=local_ip,
            local_name=self.name,
            instance_id=self.instance_id,
            hello_interval=5.0,
            peer_timeout=30.0,
            send_callback=self._send_discovery_message,
        )

        self._running = True
        self.discovery.start()

        self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._send_thread = threading.Thread(target=self._send_loop, daemon=True)
        self._reliability_thread = threading.Thread(target=self._reliability_loop, daemon=True)

        self._recv_thread.start()
        self._send_thread.start()
        self._reliability_thread.start()

        self._output(f"Chat started as '{self.name}' on {local_ip}:{self.port}")
        self._output(f"Mode: {self._send_mode.value.upper()}")
        self._output(f"Reliable delivery: {'ON' if self._reliable_enabled else 'OFF'}"
                     f" (RTO={self._rto:.1f}s, max retries={self._max_retries})")
        self._output("Type /help for commands")
        return True

    def stop(self) -> None:
        """Stop chat and cleanup."""
        self._running = False
        if self.discovery:
            self.discovery.stop()
        if self._recv_thread:
            self._recv_thread.join(timeout=2.0)
        if self._send_thread:
            self._send_thread.join(timeout=2.0)
        if self._reliability_thread:
            self._reliability_thread.join(timeout=2.0)
        self.network.cleanup()
        self._output("Chat stopped")

    def _send_discovery_message(self, msg: ChatMessage, broadcast: bool) -> None:
        """Callback for PeerDiscovery to send messages."""
        msg.instance_id = self.instance_id
        self._send_message(msg, broadcast=broadcast)

    def _next_seq(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq

    def _raw_send(self, msg: ChatMessage, broadcast: bool) -> None:
        """Low-level send: put the datagram on the wire (no reliability)."""
        bcast_sock, mcast_sock = self.network.get_sockets()
        data = msg.to_json().encode("utf-8")

        if broadcast and bcast_sock:
            try:
                bcast_addr = self.network.get_broadcast_addr()
                bcast_sock.sendto(data, bcast_addr)
            except OSError:
                pass

        if not broadcast and mcast_sock:
            try:
                mcast_addr = self.network.get_multicast_addr()
                mcast_sock.sendto(data, mcast_addr)
            except OSError:
                pass

    def _send_message(self, msg: ChatMessage, broadcast: bool) -> None:
        """Best-effort send (used by discovery: HELLO/BYE/IGNORE repeat anyway)."""
        self._raw_send(msg, broadcast)

    def _enqueue_send(self, msg: ChatMessage, broadcast: bool) -> None:
        """Send message and keep it in the buffer until every known peer ACKs it.

        If there are no known peers yet we still send it once (best-effort) —
        nothing to wait for.
        """
        self._raw_send(msg, broadcast)
        if not self._reliable_enabled:
            return

        recipients = []
        if self.discovery:
            recipients = [p.ip for p in self.discovery.get_peers()]
        if not recipients:
            return

        pm = PendingMessage(
            msg=msg,
            broadcast=broadcast,
            recipients=recipients,
            created=time.time(),
            last_sent=time.time(),
            retries=0,
            rto=self._rto,
        )
        with self._pending_lock:
            self._pending[(msg.instance_id, msg.seq)] = pm

    def _send_ack(self, msg: ChatMessage) -> None:
        """Unicast an ACK back to the original sender for a received chat message."""
        if not msg.from_ip:
            return
        ack = ChatMessage.create_ack(
            self.network.interface.ip if self.network.interface else "0.0.0.0",
            self.name,
            seq=msg.seq,
            ack_seq=msg.seq,
            ack_instance_id=msg.instance_id,
            instance_id=self.instance_id,
        )
        bcast_sock, mcast_sock = self.network.get_sockets()
        sock = bcast_sock or mcast_sock
        if not sock:
            return
        try:
            sock.sendto(ack.to_json().encode("utf-8"), (msg.from_ip, self.port))
        except OSError:
            pass

    def _handle_ack(self, msg: ChatMessage) -> None:
        """Register delivery confirmation from a peer and adapt the RTO."""
        if msg.ack_seq is None:
            return
        key = (msg.ack_instance_id, msg.ack_seq)
        with self._pending_lock:
            pm = self._pending.get(key)
            if not pm or msg.from_ip in pm.acked_by:
                return
            pm.acked_by.add(msg.from_ip)
            rtt = time.time() - pm.last_sent
            self._update_rto(rtt)
            # Delivered to everyone we were waiting for — free the buffer slot.
            if all(ip in pm.acked_by for ip in pm.recipients):
                del self._pending[key]
                self._output(f"[reliable] msg #{pm.msg.seq} delivered to all peers")

    def send_chat(self, text: str) -> None:
        """Send chat message (through the reliable buffer)."""
        msg = ChatMessage.create_msg(
            self.network.interface.ip if self.network.interface else "0.0.0.0",
            self.name,
            text,
            self._next_seq(),
            instance_id=self.instance_id,
        )
        if self._send_mode == SendMode.BROADCAST:
            self._enqueue_send(msg, broadcast=True)
        else:
            self._enqueue_send(msg, broadcast=False)

    def set_name(self, name: str) -> None:
        """Change display name."""
        old_name = self.name
        self.name = name
        if self.discovery:
            self.discovery.local_name = name
        self._output(f"Name changed: {old_name} -> {name}")

    def set_mode(self, mode: SendMode) -> None:
        """Change send mode."""
        self._send_mode = mode
        self._output(f"Mode: {mode.value.upper()}")

    def ignore_peer(self, ip: str) -> bool:
        """Ignore a peer."""
        if self.discovery:
            return self.discovery.ignore_peer(ip)
        return False

    def unignore_peer(self, ip: str) -> bool:
        """Unignore a peer."""
        if self.discovery:
            return self.discovery.unignore_peer(ip)
        return False

    def list_peers(self) -> List[str]:
        """Get formatted peer list."""
        if not self.discovery:
            return ["Discovery not initialized"]
        peers = self.discovery.get_peers()
        if not peers:
            return ["No peers found"]
        lines = []
        for p in peers:
            status = []
            if p.via_bcast:
                status.append("BCAST")
            if p.via_mcast:
                status.append("MCAST")
            if p.ignored:
                status.append("IGNORED")
            status_str = f" [{', '.join(status)}]" if status else ""
            last = time.time() - p.last_seen
            lines.append(f"  {p.ip:<15} {p.name:<15} {last:>5.0f}s ago{status_str}")
        return lines

    def _recv_loop(self) -> None:
        """Receive loop using select on both sockets."""
        bcast_sock, mcast_sock = self.network.get_sockets()
        if not bcast_sock or not mcast_sock:
            return

        socks = [bcast_sock, mcast_sock]

        while self._running:
            try:
                ready, _, _ = select.select(socks, [], [], 0.5)
            except (ValueError, OSError):
                break

            for sock in ready:
                try:
                    data, addr = sock.recvfrom(65536)
                except (BlockingIOError, OSError):
                    continue

                sender_ip = addr[0]

                msg = parse_message(data, sender_ip)
                if not msg:
                    continue

                # Skip our own messages (by instance_id, not IP — allows same-host peers)
                if msg.instance_id == self.instance_id:
                    continue

                # ACK messages → reliability layer (never deduped)
                if msg.type == MessageType.ACK.value:
                    self._handle_ack(msg)
                    continue

                # Dedup: same datagram is delivered to BOTH bcast and mcast sockets
                key = (msg.instance_id, msg.seq)
                if key in self._seen:
                    # Duplicate of a chat message → re-ACK so the sender
                    # stops retransmitting (its first ACK may have been lost).
                    if msg.type == MessageType.MSG.value:
                        self._send_ack(msg)
                    continue
                if len(self._seen) >= self._seen_max:
                    self._seen.clear()
                self._seen.add(key)

                # Handle discovery messages
                via_bcast = sock is bcast_sock
                if self.discovery and msg.type in (MessageType.HELLO.value, MessageType.BYE.value,
                                                    MessageType.IGNORE.value, MessageType.UNIGNORE.value):
                    changed = self.discovery.handle_message(msg, via_bcast)
                    if changed and msg.type == MessageType.HELLO.value:
                        self._output(f">>> {msg.from_name} ({msg.from_ip}) joined")
                    elif changed and msg.type == MessageType.BYE.value:
                        self._output(f"<<< {msg.from_name} ({msg.from_ip}) left")
                    continue

                # Chat message: confirm delivery FIRST (even if we display nothing),
                # then show it unless ignored.
                if msg.type == MessageType.MSG.value:
                    self._send_ack(msg)
                    if self.discovery and self.discovery.is_ignored(sender_ip):
                        continue
                    ts = format_timestamp(msg.timestamp)
                    self._output(f"[{ts}] {msg.from_name} ({msg.from_ip}): {msg.text}")

    def _send_loop(self) -> None:
        """Send loop - reads from stdin (handled by CLI)."""
        # This is a placeholder - actual input handling is in CLI
        while self._running:
            time.sleep(1.0)

    # ------------------------------------------------------------------ #
    #  Reliable delivery: resend buffer + adaptive RTO                    #
    # ------------------------------------------------------------------ #

    # Override keys (from constructor/CLI) → instance attribute names.
    _OVERRIDE_ATTRS = {
        "initial_rto": "_initial_rto",
        "min_rto": "_min_rto",
        "max_rto": "_max_rto",
        "max_retries": "_max_retries",
        "backoff": "_backoff_factor",
    }

    def _apply_preset(self, preset: Dict) -> None:
        self._initial_rto = preset["initial_rto"]
        self._min_rto = preset["min_rto"]
        self._max_rto = preset["max_rto"]
        self._max_retries = preset["max_retries"]
        self._backoff_factor = preset["backoff"]
        # Re-apply user overrides on top of the preset.
        for key, val in getattr(self, "_overrides", {}).items():
            attr = self._OVERRIDE_ATTRS.get(key)
            if attr:
                setattr(self, attr, val)

    def _clamp_rto(self, rto: float) -> float:
        return max(self._min_rto, min(rto, self._max_rto))

    def _update_rto(self, rtt: float) -> None:
        """Jacobson/Karels RTT estimation → adaptive RTO.

        On a slow link ACKs arrive late (large RTT samples) so the RTO grows
        by itself; if ACKs stop coming entirely the backoff in the reliability
        loop takes over.
        """
        with self._lock:
            if self._srtt is None:
                self._srtt = rtt
                self._rttvar = rtt / 2.0
            else:
                alpha = 0.125
                beta = 0.25
                err = rtt - self._srtt
                self._rttvar = (1.0 - beta) * self._rttvar + beta * abs(err)
                self._srtt = (1.0 - alpha) * self._srtt + alpha * err
            self._rto = self._clamp_rto(self._srtt + 4.0 * self._rttvar)

    def _reliability_loop(self) -> None:
        """Watch the send buffer: retransmit un-ACKed messages, drop hopeless ones."""
        while self._running:
            time.sleep(0.2)
            if not self._reliable_enabled:
                continue
            now = time.time()
            to_remove: List[Tuple[str, int]] = []
            resend: List[PendingMessage] = []
            with self._pending_lock:
                for key, pm in list(self._pending.items()):
                    missing = [ip for ip in pm.recipients if ip not in pm.acked_by]
                    if not missing:
                        to_remove.append(key)
                        continue
                    if now - pm.last_sent < pm.rto:
                        continue
                    if pm.retries >= self._max_retries:
                        to_remove.append(key)
                        self._output(
                            f"[reliable] msg #{pm.msg.seq} FAILED after {pm.retries} retries"
                            f" (no ACK from {', '.join(missing)})")
                        continue
                    # Retransmit with exponential backoff (RTO doubles each try).
                    pm.retries += 1
                    pm.last_sent = now
                    pm.rto = self._clamp_rto(pm.rto * self._backoff_factor)
                    resend.append(pm)
            for key in to_remove:
                self._pending.pop(key, None)
            for pm in resend:
                self._raw_send(pm.msg, pm.broadcast)
                if pm.retries == 1:
                    self._output(
                        f"[reliable] msg #{pm.msg.seq} not confirmed, resending "
                        f"(retry 1/{self._max_retries}, next in {pm.rto:.0f}s)")

    # ------------------------- control API ---------------------------- #

    def set_reliable(self, enabled: bool) -> None:
        """Toggle reliable delivery (ACK-based retransmission)."""
        self._reliable_enabled = enabled
        self._output(f"Reliable delivery: {'ON' if enabled else 'OFF'}")

    def set_low_throughput(self, enabled: bool) -> None:
        """Toggle low-throughput fallback: patient RTOs, more retries.

        Use on slow/unstable links so the buffer does not hammer the network
        and does not give up after a few seconds.
        """
        self._low_throughput = enabled
        with self._lock:
            self._apply_preset(LOW_THROUGHPUT_PRESET if enabled else NORMAL_PRESET)
            self._rto = max(self._rto, self._min_rto)
        self._output(
            f"Low-throughput mode: {'ON' if enabled else 'OFF'} "
            f"(RTO {self._initial_rto:.0f}s..{self._max_rto:.0f}s, "
            f"max retries {self._max_retries})")

    def buffer_info(self) -> List[str]:
        """Snapshot of the resend buffer."""
        with self._pending_lock:
            if not self._pending:
                return ["Resend buffer is empty"]
            lines = []
            now = time.time()
            for (_, seq), pm in self._pending.items():
                missing = [ip for ip in pm.recipients if ip not in pm.acked_by]
                text = pm.msg.text[:30].replace("\n", " ")
                lines.append(
                    f"  #{seq} {text!r:34} age={now - pm.created:5.1f}s "
                    f"retries={pm.retries} rto={pm.rto:5.1f}s unacked={','.join(missing)}")
            return lines

    def rto_info(self) -> List[str]:
        """Current RTO / reliability statistics."""
        with self._lock:
            srtt = f"{self._srtt * 1000:.0f}ms" if self._srtt is not None else "n/a"
            rttvar = f"{self._rttvar * 1000:.0f}ms" if self._rttvar is not None else "n/a"
            pending = len(self._pending) if hasattr(self, "_pending") else 0
            return [
                f"Reliable delivery: {'ON' if self._reliable_enabled else 'OFF'}"
                f"   Low-throughput mode: {'ON' if self._low_throughput else 'OFF'}",
                f"RTO: {self._rto:.1f}s   SRTT: {srtt}   RTTVAR: {rttvar}",
                f"Bounds: min {self._min_rto:.1f}s .. max {self._max_rto:.1f}s   "
                f"max retries: {self._max_retries}   buffer: {pending}",
            ]