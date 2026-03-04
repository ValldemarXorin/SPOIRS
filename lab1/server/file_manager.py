"""Менеджер файлов и сессий передачи."""

import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Any
from pathlib import Path


@dataclass
class TransferSession:
    filename: str
    total_size: int
    transferred: int
    start_time: float
    client_id: str
    is_upload: bool = True
    temp_path: Optional[str] = None
    file_handle: Optional[Any] = None
    sock: Optional[Any] = None

    expected_seq: int = 0
    udp_recv_buffer: Dict[int, bytes] = field(default_factory=dict)

    udp_download_active: bool = False
    udp_client_addr: Optional[Any] = None

    last_activity: float = 0.0
    udp_last_ack_time: float = 0.0
    _last_pct: int = -10


class FileManager:
    def __init__(self, storage_dir: str = "./server_files"):
        self.storage_dir = Path(storage_dir)
        self.temp_dir = self.storage_dir / ".temp"
        self.sessions: Dict[str, TransferSession] = {}
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def _sanitize_addr(self, client_addr: str) -> str:
        return client_addr.replace(":", "_").replace("/", "_")

    def get_file_path(self, filename: str) -> Path:
        return self.storage_dir / Path(filename).name

    def get_temp_path(self, filename: str, client_addr: str) -> Path:
        safe_name = Path(filename).name
        safe_addr = self._sanitize_addr(client_addr)
        return self.temp_dir / f"{safe_addr}_{safe_name}.tmp"

    def file_exists(self, filename: str) -> bool:
        return self.get_file_path(filename).exists()

    def get_file_size(self, filename: str) -> int:
        path = self.get_file_path(filename)
        return path.stat().st_size if path.exists() else 0

    def create_session(self, filename, total_size, client_id, is_upload,
                       sock=None):
        temp_path = str(self.get_temp_path(filename, client_id)) if is_upload else None
        now = time.time()
        session = TransferSession(
            filename=filename, total_size=total_size,
            transferred=0, start_time=now, client_id=client_id,
            is_upload=is_upload, temp_path=temp_path, sock=sock,
            last_activity=now, udp_last_ack_time=now,
        )
        try:
            if is_upload:
                session.file_handle = open(temp_path, 'wb')
            else:
                session.file_handle = open(self.get_file_path(filename), 'rb')
        except IOError as e:
            print(f"Error opening file: {e}")
            return None
        self.sessions[client_id] = session
        return session

    def get_session(self, client_id):
        return self.sessions.get(client_id)

    def close_session(self, client_id):
        session = self.sessions.get(client_id)
        if session and session.file_handle:
            try:
                session.file_handle.close()
            except Exception:
                pass
        self.sessions.pop(client_id, None)

    def complete_session(self, client_id):
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
            tp = Path(session.temp_path)
            fp = self.get_file_path(session.filename)
            if tp.exists():
                if fp.exists():
                    fp.unlink()
                tp.rename(fp)
        self.sessions.pop(client_id, None)

    def calculate_bitrate(self, session):
        elapsed = time.time() - session.start_time
        return session.transferred / elapsed if elapsed > 0 else 0.0

    def format_bitrate(self, bitrate):
        if bitrate >= 1024 * 1024:
            return f"{bitrate / (1024 * 1024):.2f} MB/s"
        if bitrate >= 1024:
            return f"{bitrate / 1024:.2f} KB/s"
        return f"{bitrate:.2f} B/s"