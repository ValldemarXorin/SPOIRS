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
    NACK = 4


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
# Размер пакета: 65507 - максимум для UDP, но фрагментация IP плоха.
# Для localhost/LAN без фрагментации:
#   - Стандартный Ethernet MTU=1500 → payload 1472 (без фрагментации)
#   - Jumbo frame MTU=9000 → payload ~8960
#   - Localhost: MTU=65535, фрагментация в kernel быстрая
# Для максимальной скорости на localhost используем крупные пакеты,
# kernel сам разберёт фрагментацию эффективнее чем мы по 1472.
UDP_PACKET_SIZE = 32768          # 32KB payload+header
UDP_HEADER_SIZE = 5              # 4 bytes seq + 1 byte type
UDP_PAYLOAD_SIZE = UDP_PACKET_SIZE - UDP_HEADER_SIZE  # 32763

UDP_WINDOW_SIZE = 2048           # пакетов в скользящем окне (≈64MB in flight)
UDP_TIMEOUT = 0.05               # retransmit timeout — агрессивный
UDP_RETRY_LIMIT = 40
UDP_ACK_INTERVAL = 64            # ACK каждые N пакетов
UDP_BURST_SIZE = 256             # пакетов за одну итерацию send


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