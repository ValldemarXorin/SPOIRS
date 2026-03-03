"""Протокол обмена сообщениями."""

from dataclasses import dataclass
from typing import Optional, List
from enum import Enum


class CommandType(Enum):
    ECHO = 1
    TIME = 2
    QUIT = 3
    UPLOAD = 4
    DOWNLOAD = 5
    RESUME_UPLOAD = 6
    RESUME_DOWNLOAD = 7
    UNKNOWN = 8


class PacketType(Enum):
    DATA = 0
    ACK = 1
    FIN = 2
    CMD = 3


@dataclass
class Command:
    type: CommandType
    args: List[str]
    raw: str
    protocol: str = "TCP"


@dataclass
class Response:
    success: bool
    message: str
    data: Optional[bytes] = None


COMMAND_TERMINATOR = b"
"
BUFFER_SIZE = 1024 * 1024
ENCODING = "utf-8"

# ── UDP ───────────────────────────────────────────────────
# Агрессивные настройки под локальную сеть
UDP_PACKET_SIZE = 8192
UDP_HEADER_SIZE = 5
UDP_PAYLOAD_SIZE = UDP_PACKET_SIZE - UDP_HEADER_SIZE  # 8187
UDP_WINDOW_SIZE = 4096       # 4096 × 8KB ≈ 32 MB in flight
UDP_TIMEOUT = 0.3
UDP_RETRY_LIMIT = 40


def parse_command(raw_line: str, default_proto: str = "TCP") -> "Command":
    line = raw_line.strip()
    if not line:
        return Command(CommandType.UNKNOWN, [], raw_line, default_proto)

    parts = line.split()
    cmd_name = parts[0].upper()

    protocol = default_proto
    clean: List[str] = []
    for a in parts[1:]:
        al = a.lower()
        if al == "--udp":
            protocol = "UDP"
        elif al == "--tcp":
            protocol = "TCP"
        else:
            clean.append(a)

    mapping = {
        "ECHO": CommandType.ECHO,
        "TIME": CommandType.TIME,
        "QUIT": CommandType.QUIT,
        "EXIT": CommandType.QUIT,
        "CLOSE": CommandType.QUIT,
        "UPLOAD": CommandType.UPLOAD,
        "DOWNLOAD": CommandType.DOWNLOAD,
        "RESUME_UPLOAD": CommandType.RESUME_UPLOAD,
        "RESUME_DOWNLOAD": CommandType.RESUME_DOWNLOAD,
    }

    ct = mapping.get(cmd_name, CommandType.UNKNOWN)
    args = [" ".join(clean)] if ct == CommandType.ECHO and clean else clean
    return Command(ct, args, raw_line, protocol)


def format_response(response: "Response") -> bytes:
    prefix = "OK" if response.success else "ERROR"
    return f"{prefix} {response.message}
".encode(ENCODING)
