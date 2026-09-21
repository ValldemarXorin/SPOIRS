"""Message protocol for P2P chat: simple JSON serialization (PING/TEXT/ACK)."""

import json
import time
from dataclasses import dataclass
from typing import Optional
from enum import Enum


class MessageType(Enum):
    PING = "PING"  # heartbeat / auto-discovery
    TEXT = "TEXT"  # chat message
    ACK = "ACK"    # delivery confirmation (reliable delivery, lab6 <-> lab6)


@dataclass
class ChatMessage:
    type: str
    from_ip: str
    from_name: str
    content: str
    timestamp: float
    seq: int

    # Optional fields
    instance_id: Optional[str] = None  # Unique sender instance (same-host filtering)
    ack_seq: Optional[int] = None  # For ACK: seq of the confirmed message
    ack_instance_id: Optional[str] = None  # For ACK: instance of the confirmed sender

    def to_json(self) -> str:
        """lr6.py-compatible wire format, extended with reliability fields.

        lr6.py reads only type/sender/ip/content; the rest is ignored by it.
        """
        d = {
            "type": self.type,
            "sender": self.from_name,
            "ip": self.from_ip,
            "content": self.content,
            "from_ip": self.from_ip,
            "from_name": self.from_name,
            "timestamp": self.timestamp,
            "seq": self.seq,
        }
        if self.instance_id is not None:
            d["instance_id"] = self.instance_id
        if self.ack_seq is not None:
            d["ack_seq"] = self.ack_seq
        if self.ack_instance_id is not None:
            d["ack_instance_id"] = self.ack_instance_id
        return json.dumps(d, separators=(",", ":"))

    @classmethod
    def from_json(cls, data: str) -> Optional["ChatMessage"]:
        try:
            d = json.loads(data)
            return cls(
                type=d.get("type", ""),
                from_ip=d.get("from_ip") or d.get("ip", ""),
                from_name=d.get("from_name") or d.get("sender", ""),
                content=d.get("content", ""),
                timestamp=d.get("timestamp", time.time()),
                seq=d.get("seq", 0),
                instance_id=d.get("instance_id"),
                ack_seq=d.get("ack_seq"),
                ack_instance_id=d.get("ack_instance_id"),
            )
        except (json.JSONDecodeError, KeyError):
            return None

    @classmethod
    def create_ping(cls, from_ip: str, from_name: str, seq: int,
                    instance_id: Optional[str] = None) -> "ChatMessage":
        return cls(
            type=MessageType.PING.value,
            from_ip=from_ip,
            from_name=from_name,
            content="",
            timestamp=time.time(),
            seq=seq,
            instance_id=instance_id,
        )

    @classmethod
    def create_text(cls, from_ip: str, from_name: str, content: str, seq: int,
                    instance_id: Optional[str] = None) -> "ChatMessage":
        return cls(
            type=MessageType.TEXT.value,
            from_ip=from_ip,
            from_name=from_name,
            content=content,
            timestamp=time.time(),
            seq=seq,
            instance_id=instance_id,
        )

    @classmethod
    def create_ack(cls, from_ip: str, from_name: str, seq: int,
                   ack_seq: int, ack_instance_id: str,
                   instance_id: Optional[str] = None) -> "ChatMessage":
        """Delivery confirmation for the message (ack_instance_id, ack_seq)."""
        return cls(
            type=MessageType.ACK.value,
            from_ip=from_ip,
            from_name=from_name,
            content="",
            timestamp=time.time(),
            seq=seq,
            instance_id=instance_id,
            ack_seq=ack_seq,
            ack_instance_id=ack_instance_id,
        )


def parse_message(data: bytes, sender_ip: str) -> Optional[ChatMessage]:
    """Parse incoming message, validate sender IP matches."""
    try:
        msg = ChatMessage.from_json(data.decode("utf-8"))
        if msg and msg.from_ip == sender_ip:
            return msg
    except Exception:
        pass
    return None


def format_timestamp(ts: float) -> str:
    """Format timestamp for display."""
    return time.strftime("%H:%M:%S", time.localtime(ts))