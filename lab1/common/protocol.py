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
    NACK = 4  # Selective NACK for fast retransmit


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


COMMAND_TERMINATOR = b"\n"
BUFFER_SIZE = 1024 * 1024
ENCODING = "utf-8"

# ── UDP ─────────────────────────────────────────────────
# Оптимальный размер пакета:
# - MTU Ethernet = 1500, IP header = 20, UDP header = 8 → max payload = 1472
# - Но в локальной сети jumbo frames до 9000 байт
# - Для максимальной скорости без фрагментации на стандартном Ethernet: 1472
# - Для LAN с jumbo frames: 8192
# - Мы используем 8192 (jumbo) для максимальной пропускной способности в LAN
#   Если сеть не поддерживает jumbo — уменьшить до 1472
UDP_PACKET_SIZE = 8192
UDP_HEADER_SIZE = 5          # 4 bytes seq + 1 byte type
UDP_PAYLOAD_SIZE = UDP_PACKET_SIZE - UDP_HEADER_SIZE  # 8187
UDP_WINDOW_SIZE = 4096       # 4096 × 8KB ≈ 32 MB in flight
UDP_TIMEOUT = 0.15           # retransmit timeout (seconds)
UDP_RETRY_LIMIT = 40
UDP_ACK_INTERVAL = 128       # ACK every N packets
UDP_BURST_SIZE = 2048        # packets per send burst


def parse_command(raw_line: str, default_proto: str = "TCP") -> Command:
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


def format_response(response: Response) -> bytes:
    prefix = "OK" if response.success else "ERROR"
    return f"{prefix} {response.message}\n".encode(ENCODING)