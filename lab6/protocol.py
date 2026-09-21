"""Message protocol for P2P chat: simple JSON serialization (PING/TEXT/ACK)."""

import json
import time
from dataclasses import dataclass, asdict
from typing import Optional
from enum import Enum


class MessageType(Enum):
    PING = "ping"  # heartbeat / auto-discovery
    TEXT = "text"  # chat message
    ACK = "ack"    # delivery confirmation (reliable delivery)


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
        d = asdict(self)
        return json.dumps({k: v for k, v in d.items() if v is not None}, separators=(",", ":"))

    @classmethod
    def from_json(cls, data: str) -> Optional["ChatMessage"]:
        try:
            d = json.loads(data)
            return cls(
                type=d.get("type", ""),
                from_ip=d.get("from_ip", ""),
                from_name=d.get("from_name", ""),
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