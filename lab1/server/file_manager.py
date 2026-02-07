"""Управление файлами и сессиями передачи."""

import os
import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from pathlib import Path


@dataclass
class TransferSession:
    """Информация о сессии передачи файла."""
    filename: str
    total_size: int
    transferred: int
    start_time: float
    client_addr: str
    is_upload: bool
    temp_path: Optional[str] = None


class FileManager:
    """Менеджер файлов сервера."""
    
    def __init__(self, storage_dir: str = "./server_files"):
        self.storage_dir = Path(storage_dir)
        self.temp_dir = self.storage_dir / ".temp"
        self.sessions: Dict[str, TransferSession] = {}
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Создаёт необходимые директории."""
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def _sanitize_addr(self, client_addr: str) -> str:
        """Убирает недопустимые символы из адреса для имени файла."""
        # Windows не разрешает : в именах файлов
        return client_addr.replace(":", "_")
    
    def get_file_path(self, filename: str) -> Path:
        """Возвращает полный путь к файлу."""
        # Защита от path traversal атак
        safe_name = Path(filename).name
        return self.storage_dir / safe_name
    
    def get_temp_path(self, filename: str, client_addr: str) -> Path:
        """Возвращает путь к временному файлу."""
        safe_name = Path(filename).name
        safe_addr = self._sanitize_addr(client_addr)
        return self.temp_dir / f"{safe_addr}_{safe_name}.tmp"
    
    def file_exists(self, filename: str) -> bool:
        """Проверяет существование файла."""
        return self.get_file_path(filename).exists()
    
    def get_file_size(self, filename: str) -> int:
        """Возвращает размер файла."""
        path = self.get_file_path(filename)
        return path.stat().st_size if path.exists() else 0
    
    def create_session(self, filename: str, total_size: int, 
                       client_addr: str, is_upload: bool) -> TransferSession:
        """Создаёт новую сессию передачи."""
        session = TransferSession(
            filename=filename,
            total_size=total_size,
            transferred=0,
            start_time=time.time(),
            client_addr=client_addr,
            is_upload=is_upload,
            temp_path=str(self.get_temp_path(filename, client_addr))
        )
        session_key = self._make_session_key(filename, client_addr)
        self.sessions[session_key] = session
        return session
    
    def get_session(self, filename: str, 
                    client_addr: str) -> Optional[TransferSession]:
        """Возвращает существующую сессию."""
        key = self._make_session_key(filename, client_addr)
        return self.sessions.get(key)
    
    def complete_session(self, filename: str, client_addr: str) -> None:
        """Завершает сессию, перемещая файл из temp."""
        session = self.get_session(filename, client_addr)
        if session and session.is_upload and session.temp_path:
            temp_path = Path(session.temp_path)
            final_path = self.get_file_path(filename)
            if temp_path.exists():
                # Удаляем целевой файл, если существует
                if final_path.exists():
                    final_path.unlink()
                temp_path.rename(final_path)
        
        key = self._make_session_key(filename, client_addr)
        self.sessions.pop(key, None)
    
    def remove_session(self, filename: str, client_addr: str) -> None:
        """Удаляет сессию и временные файлы."""
        session = self.get_session(filename, client_addr)
        if session and session.temp_path:
            temp_path = Path(session.temp_path)
            if temp_path.exists():
                temp_path.unlink()
        
        key = self._make_session_key(filename, client_addr)
        self.sessions.pop(key, None)
    
    def _make_session_key(self, filename: str, client_addr: str) -> str:
        """Создаёт уникальный ключ сессии."""
        return f"{client_addr}:{filename}"
    
    def calculate_bitrate(self, session: TransferSession) -> float:
        """Вычисляет скорость передачи в байтах/сек."""
        elapsed = time.time() - session.start_time
        if elapsed <= 0:
            return 0.0
        return session.transferred / elapsed
    
    def format_bitrate(self, bitrate: float) -> str:
        """Форматирует скорость в человекочитаемый вид."""
        if bitrate >= 1024 * 1024:
            return f"{bitrate / (1024 * 1024):.2f} MB/s"
        elif bitrate >= 1024:
            return f"{bitrate / 1024:.2f} KB/s"
        return f"{bitrate:.2f} B/s"