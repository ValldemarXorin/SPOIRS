"""P2P Chat: main application class with recv/send loops."""

import socket
import select
import threading
import time
import sys
import uuid
from typing import Optional, Callable, List, Tuple
from enum import Enum

from lab6.network import NetworkManager
from lab6.discovery import PeerDiscovery
from lab6.protocol import ChatMessage, MessageType, parse_message, format_timestamp


class SendMode(Enum):
    BROADCAST = "broadcast"
    MULTICAST = "multicast"


class P2PChat:
    """Main P2P chat application."""

    def __init__(
        self,
        port: int = 50000,
        multicast_group: str = "239.255.0.1",
        interface_name: Optional[str] = None,
        interface_ip: Optional[str] = None,
        name: str = "Anonymous",
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

        self._send_mode = SendMode.BROADCAST
        self._seq = 0
        self._output_callback: Optional[Callable[[str], None]] = None
        self._lock = threading.Lock()
        # Dedup: (instance_id, seq) seen recently (both sockets get same datagram)
        self._seen: set = set()
        self._seen_max = 1000

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

        self._recv_thread.start()
        self._send_thread.start()

        self._output(f"Chat started as '{self.name}' on {local_ip}:{self.port}")
        self._output(f"Mode: {self._send_mode.value.upper()}")
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

    def _send_message(self, msg: ChatMessage, broadcast: bool) -> None:
        """Send message via appropriate socket."""
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

    def send_chat(self, text: str) -> None:
        """Send chat message."""
        msg = ChatMessage.create_msg(
            self.network.interface.ip if self.network.interface else "0.0.0.0",
            self.name,
            text,
            self._next_seq(),
            instance_id=self.instance_id,
        )
        if self._send_mode == SendMode.BROADCAST:
            self._send_message(msg, broadcast=True)
        else:
            self._send_message(msg, broadcast=False)

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

                # Dedup: same datagram is delivered to BOTH bcast and mcast sockets
                key = (msg.instance_id, msg.seq)
                if key in self._seen:
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

                # Ignore check
                if self.discovery and self.discovery.is_ignored(sender_ip):
                    continue

                # Chat message
                if msg.type == MessageType.MSG.value:
                    ts = format_timestamp(msg.timestamp)
                    self._output(f"[{ts}] {msg.from_name} ({msg.from_ip}): {msg.text}")

    def _send_loop(self) -> None:
        """Send loop - reads from stdin (handled by CLI)."""
        # This is a placeholder - actual input handling is in CLI
        while self._running:
            time.sleep(1.0)