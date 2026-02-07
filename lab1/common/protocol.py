"""Протокол обмена сообщениями между клиентом и сервером."""

from dataclasses import dataclass
from typing import Optional, Tuple
from enum import Enum, auto


class CommandType(Enum):
    """Типы поддерживаемых команд."""
    ECHO = auto()
    TIME = auto()
    QUIT = auto()
    UPLOAD = auto()
    DOWNLOAD = auto()
    RESUME_UPLOAD = auto()
    RESUME_DOWNLOAD = auto()
    UNKNOWN = auto()


@dataclass
class Command:
    """Распарсенная команда."""
    type: CommandType
    args: list
    raw: str


@dataclass
class Response:
    """Ответ сервера."""
    success: bool
    message: str
    data: Optional[bytes] = None


COMMAND_TERMINATOR = b'\n'
BUFFER_SIZE = 8192
ENCODING = 'utf-8'


def parse_command(raw_line: str) -> Command:
    """Парсит строку команды в структуру Command."""
    line = raw_line.strip()
    if not line:
        return Command(CommandType.UNKNOWN, [], raw_line)
    
    parts = line.split(maxsplit=1)
    cmd_name = parts[0].upper()
    args = parts[1].split() if len(parts) > 1 else []
    
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
    
    # Для ECHO сохраняем весь текст после команды
    if cmd_type == CommandType.ECHO and len(parts) > 1:
        args = [parts[1]]
    
    return Command(cmd_type, args, raw_line)


def format_response(response: Response) -> bytes:
    """Форматирует ответ для отправки."""
    prefix = "OK" if response.success else "ERROR"
    return f"{prefix} {response.message}\n".encode(ENCODING)