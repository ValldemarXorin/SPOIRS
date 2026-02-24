"""Протокол обмена сообщениями между клиентом и сервером."""

from dataclasses import dataclass
from typing import Optional, List
from enum import Enum


class CommandType(Enum):
    """Типы поддерживаемых команд."""

    ECHO = 1
    TIME = 2
    QUIT = 3
    UPLOAD = 4
    DOWNLOAD = 5
    RESUME_UPLOAD = 6
    RESUME_DOWNLOAD = 7
    UNKNOWN = 8


class PacketType(Enum):
    """Типы пакетов RUDP."""

    DATA = 0
    ACK = 1
    FIN = 2
    CMD = 3


@dataclass
class Command:
    """Распарсенная команда."""

    type: CommandType
    args: List[str]
    raw: str
    protocol: str = 'TCP'  # 'TCP' или 'UDP'


@dataclass
class Response:
    """Ответ сервера."""

    success: bool
    message: str
    data: Optional[bytes] = None


# Константы TCP
COMMAND_TERMINATOR = b'\n'
BUFFER_SIZE = 1024 * 1024  # 1MB буфер для TCP чтения
ENCODING = 'utf-8'


# Константы UDP
# ВАЖНО: 65000 подходит только для localhost. В реальной сети это приводит к IP-фрагментации
# и массовым потерям/ретрансмитам. Делаем MTU-friendly размер.
UDP_PACKET_SIZE = 1400
UDP_HEADER_SIZE = 5
UDP_PAYLOAD_SIZE = UDP_PACKET_SIZE - UDP_HEADER_SIZE

# Размер окна (пакеты), умеренный для скорости/стабильности
UDP_WINDOW_SIZE = 512

# Таймаут ретрансмитов (сек.)
UDP_TIMEOUT = 0.4

# Повторы для команд (CMD)
UDP_RETRY_LIMIT = 40


def parse_command(raw_line: str, default_proto: str = 'TCP') -> Command:
    """Парсит строку команды в структуру Command."""

    line = raw_line.strip()
    if not line:
        return Command(CommandType.UNKNOWN, [], raw_line, default_proto)

    parts = line.split()
    if not parts:
        return Command(CommandType.UNKNOWN, [], raw_line, default_proto)

    cmd_name = parts[0].upper()
    protocol = default_proto

    clean_args: List[str] = []
    for arg in parts[1:]:
        if arg.lower() == '--udp':
            protocol = 'UDP'
        elif arg.lower() == '--tcp':
            protocol = 'TCP'
        else:
            clean_args.append(arg)

    command_map = {
        'ECHO': CommandType.ECHO,
        'TIME': CommandType.TIME,
        'QUIT': CommandType.QUIT,
        'EXIT': CommandType.QUIT,
        'CLOSE': CommandType.QUIT,
        'UPLOAD': CommandType.UPLOAD,
        'DOWNLOAD': CommandType.DOWNLOAD,
        'RESUME_UPLOAD': CommandType.RESUME_UPLOAD,
        'RESUME_DOWNLOAD': CommandType.RESUME_DOWNLOAD,
    }

    cmd_type = command_map.get(cmd_name, CommandType.UNKNOWN)

    args = clean_args
    if cmd_type == CommandType.ECHO and clean_args:
        args = [" ".join(clean_args)]

    return Command(cmd_type, args, raw_line, protocol)


def format_response(response: Response) -> bytes:
    """Форматирует ответ сервера в байты."""

    prefix = "OK" if response.success else "ERROR"
    return f"{prefix} {response.message}\n".encode(ENCODING)
