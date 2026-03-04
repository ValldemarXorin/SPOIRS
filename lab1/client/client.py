"""TCP клиент для работы с сервером."""

import socket
import os
import time
from pathlib import Path
from typing import Optional, Tuple

from common.protocol import COMMAND_TERMINATOR, BUFFER_SIZE
from common.socket_utils import (
    create_client_socket, recv_until, 
    recv_exact, send_all
)


class FileTransferClient:
    """Клиент для передачи файлов."""
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self.connected = False
        self.download_dir = Path("./downloads")
        self.download_dir.mkdir(exist_ok=True)
    
    def connect(self) -> bool:
        """Устанавливает соединение с сервером."""
        try:
            self.socket = create_client_socket()
            self.socket.connect((self.host, self.port))
            self.connected = True
            
            # Читаем приветствие
            welcome = recv_until(self.socket, COMMAND_TERMINATOR)
            if welcome:
                print(welcome.decode().strip())
            return True
        except socket.error as e:
            print(f"Connection failed: {e}")
            return False
    
    def disconnect(self) -> None:
        """Закрывает соединение."""
        if self.socket:
            self.send_command("QUIT")
            self.socket.close()
            self.connected = False
    
    def send_command(self, command: str) -> Optional[str]:
        """Отправляет команду и получает ответ."""
        if not self.connected:
            return None
        
        try:
            send_all(self.socket, (command + "\n").encode())
            response = recv_until(self.socket, COMMAND_TERMINATOR)
            return response.decode().strip() if response else None
        except socket.error as e:
            print(f"Command failed: {e}")
            return None
    
    def upload_file(self, filepath: str) -> bool:
        """Загружает файл на сервер."""
        path = Path(filepath)
        if not path.exists():
            print(f"File not found: {filepath}")
            return False
        
        filename = path.name
        file_size = path.stat().st_size
        
        return self._do_upload(path, filename, file_size, offset=0)
    
    def resume_upload(self, filepath: str, offset: int) -> bool:
        """Продолжает загрузку файла с указанной позиции."""
        path = Path(filepath)
        if not path.exists():
            print(f"File not found: {filepath}")
            return False
        
        filename = path.name
        file_size = path.stat().st_size
        remaining = file_size - offset
        
        return self._do_upload(path, filename, remaining, offset)
    
    def _do_upload(self, path: Path, filename: str, 
                   size: int, offset: int) -> bool:
        """Выполняет загрузку файла."""
        if offset > 0:
            command = f"RESUME_UPLOAD {filename} {offset} {size}"
        else:
            command = f"UPLOAD {filename} {size}"
        
        send_all(self.socket, (command + "\n").encode())
        
        response = recv_until(self.socket, COMMAND_TERMINATOR)
        if not response or not response.startswith(b"READY"):
            print(f"Server not ready: {response}")
            return False
        
        start_time = time.time()
        sent = self._send_file_data(path, size, offset)
        elapsed = time.time() - start_time
        
        # Получаем финальный ответ
        final = recv_until(self.socket, COMMAND_TERMINATOR, timeout=10)
        if final:
            print(final.decode().strip())
        
        self._print_stats("Upload", sent, elapsed)
        return sent == size
    
    def _send_file_data(self, path: Path, size: int, offset: int) -> int:
        sent = 0
        retry_count = 0
        max_retries = 3  # 3 попытки за 90 секунд (30 сек каждая)
    
        try:
            with open(path, 'rb') as f:
                f.seek(offset)
                while sent < size:
                    chunk = f.read(min(BUFFER_SIZE, size - sent))
                    if not chunk:
                        break
                
                    try:
                        if not send_all(self.socket, chunk):
                            # Попытка восстановления
                            if retry_count < max_retries:
                                print(f"\nConnection lost. Retrying... ({retry_count+1}/{max_retries})")
                                time.sleep(30)
                                if self._reconnect():
                                    retry_count += 1
                                    continue
                            print("\nConnection lost. Type RESUME to continue.")
                            break
                    except socket.error:
                        # Аналогично
                        pass
                
                    sent += len(chunk)
                    self._print_progress(sent, size)
        except IOError as e:
            print(f"File read error: {e}")
        return sent

def _reconnect(self) -> bool:
    """Переподключается к серверу."""
    try:
        self.socket.close()
        self.socket = create_client_socket()
        self.socket.connect((self.host, self.port))
        return True
    except socket.error:
        return False
    
    def download_file(self, filename: str) -> bool:
        """Скачивает файл с сервера."""
        return self._do_download(filename, offset=0)
    
    def resume_download(self, filename: str, offset: int) -> bool:
        """Продолжает скачивание файла с указанной позиции."""
        return self._do_download(filename, offset)
    
    def _do_download(self, filename: str, offset: int) -> bool:
        """Выполняет скачивание файла."""
        if offset > 0:
            command = f"RESUME_DOWNLOAD {filename} {offset}"
        else:
            command = f"DOWNLOAD {filename}"
        
        send_all(self.socket, (command + "\n").encode())
        
        response = recv_until(self.socket, COMMAND_TERMINATOR)
        if not response:
            return False
        
        response_str = response.decode().strip()
        if response_str.startswith("ERROR"):
            print(response_str)
            return False
        
        file_size = self._parse_file_response(response_str)
        if file_size <= 0:
            print(f"Invalid response: {response_str}")
            return False
        
        start_time = time.time()
        received = self._receive_file_data(filename, file_size, offset)
        elapsed = time.time() - start_time
        
        self._print_stats("Download", received, elapsed)
        return received == file_size
    
    def _parse_file_response(self, response: str) -> int:
        """Парсит ответ сервера с размером файла."""
        parts = response.split()
        if len(parts) >= 2 and parts[0] == "FILE":
            try:
                return int(parts[1])
            except ValueError:
                pass
        return 0
    
    def _receive_file_data(self, filename: str, size: int, offset: int) -> int:
        """Принимает данные файла."""
        filepath = self.download_dir / filename
        mode = 'ab' if offset > 0 else 'wb'
        received = 0
        
        try:
            with open(filepath, mode) as f:
                while received < size:
                    chunk_size = min(BUFFER_SIZE, size - received)
                    data = recv_exact(self.socket, chunk_size, timeout=60)
                    if data is None:
                        print("\nConnection lost during download")
                        break
                    f.write(data)
                    received += len(data)
                    self._print_progress(received, size)
        except IOError as e:
            print(f"File write error: {e}")
        
        return received
    
    def _print_progress(self, current: int, total: int) -> None:
        """Выводит прогресс передачи."""
        percent = (current / total) * 100 if total > 0 else 0
        print(f"\rProgress: {percent:.1f}% ({current}/{total} bytes)", end="")
    
    def _print_stats(self, operation: str, bytes_transferred: int, 
                     elapsed: float) -> None:
        """Выводит статистику передачи."""
        print()  # Новая строка после прогресса
        if elapsed > 0:
            bitrate = bytes_transferred / elapsed
            if bitrate >= 1024 * 1024:
                print(f"{operation} complete: {bitrate / (1024*1024):.2f} MB/s")
            elif bitrate >= 1024:
                print(f"{operation} complete: {bitrate / 1024:.2f} KB/s")
            else:
                print(f"{operation} complete: {bitrate:.2f} B/s")


class InteractiveClient:
    """Интерактивный клиент с командной строкой."""
    
    def __init__(self, host: str, port: int):
        self.client = FileTransferClient(host, port)
        self.last_upload: Optional[Tuple[str, int]] = None
        self.last_download: Optional[Tuple[str, int]] = None
    
    def run(self) -> None:
        """Запускает интерактивный режим."""
        if not self.client.connect():
            return
        
        self._print_help()
        
        try:
            while True:
                try:
                    cmd = input("\n> ").strip()
                except EOFError:
                    break
                
                if not cmd:
                    continue
                
                if not self._process_input(cmd):
                    break
        except KeyboardInterrupt:
            print("\nInterrupted")
        finally:
            self.client.disconnect()
    
    def _print_help(self) -> None:
        """Выводит справку по командам."""
        print("\nAvailable commands:")
        print("  ECHO <text>        - Echo text back")
        print("  TIME               - Get server time")
        print("  UPLOAD <file>      - Upload file to server")
        print("  DOWNLOAD <file>    - Download file from server")
        print("  RESUME             - Resume last transfer")
        print("  QUIT               - Disconnect")
    
    def _process_input(self, cmd: str) -> bool:
        """Обрабатывает введённую команду."""
        parts = cmd.split(maxsplit=1)
        command = parts[0].upper()
        args = parts[1] if len(parts) > 1 else ""
        
        if command in ('QUIT', 'EXIT', 'CLOSE'):
            return False
        elif command == 'UPLOAD':
            return self._handle_upload(args)
        elif command == 'DOWNLOAD':
            return self._handle_download(args)
        elif command == 'RESUME':
            return self._handle_resume()
        else:
            response = self.client.send_command(cmd)
            if response:
                print(response)
            return True
    
    def _handle_upload(self, filepath: str) -> bool:
        """Обрабатывает команду загрузки."""
        if not filepath:
            print("Usage: UPLOAD <filepath>")
            return True
        
        if self.client.upload_file(filepath):
            self.last_upload = None
        else:
            # Сохраняем для возможной докачки
            path = Path(filepath)
            if path.exists():
                self.last_upload = (filepath, 0)  # TODO: получить offset
        return True
    
    def _handle_download(self, filename: str) -> bool:
        """Обрабатывает команду скачивания."""
        if not filename:
            print("Usage: DOWNLOAD <filename>")
            return True
        
        if self.client.download_file(filename):
            self.last_download = None
        else:
            self.last_download = (filename, 0)  # TODO: получить offset
        return True
    
    def _handle_resume(self) -> bool:
        """Обрабатывает команду докачки."""
        if self.last_upload:
            filepath, offset = self.last_upload
            self.client.resume_upload(filepath, offset)
        elif self.last_download:
            filename, offset = self.last_download
            self.client.resume_download(filename, offset)
        else:
            print("No transfer to resume")
        return True