"""Управление файлами и сессиями передачи."""

import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Any
from pathlib import Path


@dataclass
class TransferSession:
    """Информация о сессии передачи файла."""

    filename: str
    total_size: int
    transferred: int
    start_time: float
    client_id: str  # IP:Port string
    is_upload: bool  # True если клиент загружает НА сервер

    temp_path: Optional[str] = None
    file_handle: Optional[Any] = None  # Открытый файл
    sock: Optional[Any] = None  # Сокет клиента (для TCP)

    # UDP upload (server receive)
    expected_seq: int = 0
    udp_recv_buffer: Dict[int, bytes] = field(default_factory=dict)

    # UDP download (server send)
    next_seq_num: int = 0
    window_base: int = 0
    udp_packets: Dict[int, bytes] = field(default_factory=dict)
    udp_eof: bool = False
    udp_last_ack_time: float = 0.0

    # FIN state (download)
    udp_fin_sent: bool = False
    udp_fin_seq: int = 0
    udp_fin_acked: bool = False
    udp_last_fin_time: float = 0.0
    udp_fin_tries: int = 0

    last_activity: float = 0.0


class FileManager:
    """Менеджер файлов сервера."""

    def __init__(self, storage_dir: str = "./server_files"):
        self.storage_dir = Path(storage_dir)
        self.temp_dir = self.storage_dir / ".temp"
        self.sessions: Dict[str, TransferSession] = {}
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def _sanitize_addr(self, client_addr: str) -> str:
        return client_addr.replace(":", "_")

    def get_file_path(self, filename: str) -> Path:
        safe_name = Path(filename).name
        return self.storage_dir / safe_name

    def get_temp_path(self, filename: str, client_addr: str) -> Path:
        safe_name = Path(filename).name
        safe_addr = self._sanitize_addr(client_addr)
        return self.temp_dir / f"{safe_addr}_{safe_name}.tmp"

    def file_exists(self, filename: str) -> bool:
        return self.get_file_path(filename).exists()

    def get_file_size(self, filename: str) -> int:
        path = self.get_file_path(filename)
        return path.stat().st_size if path.exists() else 0

    def create_session(self, filename: str, total_size: int,
                       client_id: str, is_upload: bool, sock=None) -> Optional[TransferSession]:
        """Создаёт и регистрирует новую сессию."""

        temp_path = str(self.get_temp_path(filename, client_id)) if is_upload else None

        session = TransferSession(
            filename=filename,
            total_size=total_size,
            transferred=0,
            start_time=time.time(),
            client_id=client_id,
            is_upload=is_upload,
            temp_path=temp_path,
            sock=sock,
            last_activity=time.time(),
            udp_last_ack_time=time.time(),
        )

        # Открываем файл сразу, чтобы не делать это в цикле
        try:
            if is_upload:
                # Пока без докачки/резюма на сервере (для простоты)
                session.file_handle = open(temp_path, 'wb')
            else:
                path = self.get_file_path(filename)
                session.file_handle = open(path, 'rb')
        except IOError as e:
            print(f"Error opening file for session: {e}")
            return None

        self.sessions[client_id] = session
        return session

    def get_session(self, client_id: str) -> Optional[TransferSession]:
        return self.sessions.get(client_id)

    def close_session(self, client_id: str) -> None:
        """Закрывает дескриптор файла и удаляет сессию."""

        session = self.sessions.get(client_id)
        if session and session.file_handle:
            try:
                session.file_handle.close()
            except Exception:
                pass

        self.sessions.pop(client_id, None)

    def complete_session(self, client_id: str) -> None:
        """Успешное завершение сессии (перенос файла)."""

        session = self.sessions.get(client_id)
        if not session:
            return

        if session.file_handle:
            try:
                session.file_handle.close()
            except Exception:
                pass
            session.file_handle = None

        if session.is_upload and session.temp_path:
            temp_path = Path(session.temp_path)
            final_path = self.get_file_path(session.filename)
            if temp_path.exists():
                if final_path.exists():
                    final_path.unlink()
                temp_path.rename(final_path)

        self.sessions.pop(client_id, None)

    def calculate_bitrate(self, session: TransferSession) -> float:
        elapsed = time.time() - session.start_time
        if elapsed <= 0:
            return 0.0
        return session.transferred / elapsed

    def format_bitrate(self, bitrate: float) -> str:
        if bitrate >= 1024 * 1024:
            return f"{bitrate / (1024 * 1024):.2f} MB/s"
        if bitrate >= 1024:
            return f"{bitrate / 1024:.2f} KB/s"
        return f"{bitrate:.2f} B/s"
