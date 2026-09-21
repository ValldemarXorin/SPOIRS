"""Peer discovery: PING heartbeat, peer registry, TTL cleanup."""

import time
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable, List

from lab6.protocol import ChatMessage, MessageType


@dataclass
class PeerInfo:
    ip: str
    name: str
    last_seen: float = field(default_factory=time.time)

    def is_expired(self, timeout: float = 10.0) -> bool:
        return time.time() - self.last_seen > timeout


class PeerDiscovery:
    """Auto-discovery via periodic PING beacons (2.5s) and TTL cleanup (10s)."""

    def __init__(
        self,
        local_ip: str,
        local_name: str,
        instance_id: Optional[str] = None,
        ping_interval: float = 2.5,
        peer_timeout: float = 10.0,
        send_callback: Optional[Callable[[ChatMessage], None]] = None,
    ):
        self.local_ip = local_ip
        self.local_name = local_name
        self.instance_id = instance_id or "local"
        self.ping_interval = ping_interval
        self.peer_timeout = peer_timeout
        self.send_callback = send_callback  # (message) -> None, sent in current mode

        self._peers: Dict[str, PeerInfo] = {}
        self._lock = threading.RLock()
        self._running = False
        self._ping_thread: Optional[threading.Thread] = None
        self._cleanup_thread: Optional[threading.Thread] = None
        self._seq = 0

    def start(self) -> None:
        """Start discovery loops and send the initial PING."""
        self._running = True
        self._ping_thread = threading.Thread(target=self._ping_loop, daemon=True)
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._ping_thread.start()
        self._cleanup_thread.start()
        self._send_ping()

    def stop(self) -> None:
        self._running = False
        if self._ping_thread:
            self._ping_thread.join(timeout=1.0)
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=1.0)

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _ping_loop(self) -> None:
        while self._running:
            time.sleep(self.ping_interval)
            if self._running:
                self._send_ping()

    def _cleanup_loop(self) -> None:
        while self._running:
            time.sleep(5.0)
            self._cleanup_expired()

    def _send_ping(self) -> None:
        ping = ChatMessage.create_ping(self.local_ip, self.local_name, self._next_seq(),
                                       instance_id=self.instance_id)
        if self.send_callback:
            try:
                self.send_callback(ping)
            except Exception:
                pass

    def _cleanup_expired(self) -> None:
        with self._lock:
            expired = [ip for ip, peer in self._peers.items() if peer.is_expired(self.peer_timeout)]
            for ip in expired:
                del self._peers[ip]

    def handle_ping(self, msg: ChatMessage) -> None:
        """Register or refresh a peer from a PING beacon."""
        if msg.instance_id == self.instance_id:
            return  # Ignore own messages (allows same-host peers)
        with self._lock:
            self._peers[msg.from_ip] = PeerInfo(ip=msg.from_ip, name=msg.from_name, last_seen=time.time())

    def get_peers(self) -> List[PeerInfo]:
        with self._lock:
            return list(self._peers.values())

    def peer_count(self) -> int:
        with self._lock:
            return len(self._peers)