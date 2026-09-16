"""Peer discovery: HELLO announcements, peer registry, ignore list, TTL cleanup."""

import time
import threading
import random
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable, List
from enum import Enum

from lab6.protocol import ChatMessage, MessageType


@dataclass
class PeerInfo:
    ip: str
    name: str
    last_seen: float = field(default_factory=time.time)
    ignored: bool = False
    via_bcast: bool = False
    via_mcast: bool = False

    def is_expired(self, timeout: float = 30.0) -> bool:
        return time.time() - self.last_seen > timeout


class PeerDiscovery:
    """Manages peer discovery via periodic HELLO messages."""

    def __init__(
        self,
        local_ip: str,
        local_name: str,
        instance_id: Optional[str] = None,
        hello_interval: float = 5.0,
        peer_timeout: float = 30.0,
        send_callback: Optional[Callable[[ChatMessage, bool], None]] = None,
    ):
        self.local_ip = local_ip
        self.local_name = local_name
        self.instance_id = instance_id or "local"
        self.hello_interval = hello_interval
        self.peer_timeout = peer_timeout
        self.send_callback = send_callback  # (message, is_broadcast) -> None

        self._peers: Dict[str, PeerInfo] = {}
        self._lock = threading.RLock()
        self._running = False
        self._hello_thread: Optional[threading.Thread] = None
        self._cleanup_thread: Optional[threading.Thread] = None
        self._seq = random.randint(1, 1000)

    def start(self) -> None:
        """Start discovery loops."""
        self._running = True
        self._hello_thread = threading.Thread(target=self._hello_loop, daemon=True)
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._hello_thread.start()
        self._cleanup_thread.start()
        # Send initial HELLO
        self._send_hello()

    def stop(self) -> None:
        """Stop discovery and send BYE."""
        self._running = False
        if self._hello_thread:
            self._hello_thread.join(timeout=1.0)
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=1.0)
        # Send BYE
        bye = ChatMessage.create_bye(self.local_ip, self.local_name, self._next_seq(),
                                     instance_id=self.instance_id)
        self._send(bye, broadcast=True)
        self._send(bye, broadcast=False)

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _hello_loop(self) -> None:
        while self._running:
            time.sleep(self.hello_interval)
            if self._running:
                self._send_hello()

    def _cleanup_loop(self) -> None:
        while self._running:
            time.sleep(5.0)
            if self._running:
                self._cleanup_expired()

    def _send_hello(self) -> None:
        hello = ChatMessage.create_hello(self.local_ip, self.local_name, self._next_seq(),
                                         instance_id=self.instance_id)
        self._send(hello, broadcast=True)
        self._send(hello, broadcast=False)

    def _send(self, msg: ChatMessage, broadcast: bool) -> None:
        if self.send_callback:
            try:
                self.send_callback(msg, broadcast)
            except Exception:
                pass

    def _cleanup_expired(self) -> None:
        with self._lock:
            expired = [ip for ip, peer in self._peers.items() if peer.is_expired(self.peer_timeout)]
            for ip in expired:
                del self._peers[ip]

    def handle_message(self, msg: ChatMessage, via_bcast: bool) -> bool:
        """Process incoming discovery message. Returns True if peer list changed."""
        if msg.instance_id == self.instance_id:
            return False  # Ignore own messages (by instance, allows same-host peers)

        with self._lock:
            if msg.type == MessageType.HELLO.value:
                return self._handle_hello(msg, via_bcast)
            elif msg.type == MessageType.BYE.value:
                return self._handle_bye(msg)
            elif msg.type == MessageType.IGNORE.value:
                return self._handle_ignore(msg)
            elif msg.type == MessageType.UNIGNORE.value:
                return self._handle_unignore(msg)
        return False

    def _handle_hello(self, msg: ChatMessage, via_bcast: bool) -> bool:
        peer = self._peers.get(msg.from_ip)
        if peer:
            peer.last_seen = time.time()
            peer.name = msg.from_name
            if via_bcast:
                peer.via_bcast = True
            else:
                peer.via_mcast = True
            return False
        else:
            self._peers[msg.from_ip] = PeerInfo(
                ip=msg.from_ip,
                name=msg.from_name,
                via_bcast=via_bcast,
                via_mcast=not via_bcast,
            )
            return True

    def _handle_bye(self, msg: ChatMessage) -> bool:
        if msg.from_ip in self._peers:
            del self._peers[msg.from_ip]
            return True
        return False

    def _handle_ignore(self, msg: ChatMessage) -> bool:
        target_ip = msg.target_ip
        if target_ip and target_ip in self._peers:
            self._peers[target_ip].ignored = True
            return True
        # Also ignore the sender if they're telling us to ignore someone
        if msg.from_ip in self._peers:
            self._peers[msg.from_ip].ignored = True
            return True
        return False

    def _handle_unignore(self, msg: ChatMessage) -> bool:
        target_ip = msg.target_ip
        if target_ip and target_ip in self._peers:
            self._peers[target_ip].ignored = False
            return True
        return False

    def ignore_peer(self, ip: str) -> bool:
        """Locally ignore a peer and broadcast IGNORE."""
        with self._lock:
            if ip in self._peers:
                self._peers[ip].ignored = True
                ignore_msg = ChatMessage.create_ignore(
                    self.local_ip, self.local_name, ip, self._next_seq(),
                    instance_id=self.instance_id)
                self._send(ignore_msg, broadcast=True)
                self._send(ignore_msg, broadcast=False)
                return True
        return False

    def unignore_peer(self, ip: str) -> bool:
        """Locally unignore a peer and broadcast UNIGNORE."""
        with self._lock:
            if ip in self._peers:
                self._peers[ip].ignored = False
                unignore_msg = ChatMessage.create_unignore(
                    self.local_ip, self.local_name, ip, self._next_seq(),
                    instance_id=self.instance_id)
                self._send(unignore_msg, broadcast=True)
                self._send(unignore_msg, broadcast=False)
                return True
        return False

    def is_ignored(self, ip: str) -> bool:
        with self._lock:
            peer = self._peers.get(ip)
            return peer.ignored if peer else False

    def get_peers(self) -> List[PeerInfo]:
        with self._lock:
            return list(self._peers.values())

    def get_peer(self, ip: str) -> Optional[PeerInfo]:
        with self._lock:
            return self._peers.get(ip)

    def peer_count(self) -> int:
        with self._lock:
            return len(self._peers)