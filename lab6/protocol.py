"""Message protocol for P2P chat: JSON serialization."""

import json
import time
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from enum import Enum


class MessageType(Enum):
    MSG = "msg"           # Chat message
    HELLO = "hello"       # Discovery announcement
    BYE = "bye"           # Leaving
    IGNORE = "ignore"     # Force ignore a peer
    UNIGNORE = "unignore" # Remove from ignore list


@dataclass
class ChatMessage:
    type: str
    from_ip: str
    from_name: str
    text: str
    timestamp: float
    seq: int

    # Optional fields for specific types
    target_ip: Optional[str] = None  # For IGNORE/UNIGNORE

    def to_json(self) -> str:
        d = asdict(self)
        # Remove None values
        return json.dumps({k: v for k, v in d.items() if v is not None}, separators=(",", ":"))

    @classmethod
    def from_json(cls, data: str) -> Optional["ChatMessage"]:
        try:
            d = json.loads(data)
            return cls(
                type=d.get("type", ""),
                from_ip=d.get("from_ip", ""),
                from_name=d.get("from_name", ""),
                text=d.get("text", ""),
                timestamp=d.get("timestamp", time.time()),
                seq=d.get("seq", 0),
                target_ip=d.get("target_ip"),
            )
        except (json.JSONDecodeError, KeyError):
            return None

    @classmethod
    def create_msg(cls, from_ip: str, from_name: str, text: str, seq: int) -> "ChatMessage":
        return cls(
            type=MessageType.MSG.value,
            from_ip=from_ip,
            from_name=from_name,
            text=text,
            timestamp=time.time(),
            seq=seq,
        )

    @classmethod
    def create_hello(cls, from_ip: str, from_name: str, seq: int) -> "ChatMessage":
        return cls(
            type=MessageType.HELLO.value,
            from_ip=from_ip,
            from_name=from_name,
            text="",
            timestamp=time.time(),
            seq=seq,
        )

    @classmethod
    def create_bye(cls, from_ip: str, from_name: str, seq: int) -> "ChatMessage":
        return cls(
            type=MessageType.BYE.value,
            from_ip=from_ip,
            from_name=from_name,
            text="",
            timestamp=time.time(),
            seq=seq,
        )

    @classmethod
    def create_ignore(cls, from_ip: str, from_name: str, target_ip: str, seq: int) -> "ChatMessage":
        return cls(
            type=MessageType.IGNORE.value,
            from_ip=from_ip,
            from_name=from_name,
            text="",
            timestamp=time.time(),
            seq=seq,
            target_ip=target_ip,
        )

    @classmethod
    def create_unignore(cls, from_ip: str, from_name: str, target_ip: str, seq: int) -> "ChatMessage":
        return cls(
            type=MessageType.UNIGNORE.value,
            from_ip=from_ip,
            from_name=from_name,
            text="",
            timestamp=time.time(),
            seq=seq,
            target_ip=target_ip,
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