"""TCP/UDP клиент для работы с сервером."""

import socket
import os
import time
from pathlib import Path
from typing import Optional, Tuple, Callable

from common.protocol import COMMAND_TERMINATOR, BUFFER_SIZE
from common.socket_utils import (
    create_client_socket, recv_until,
    recv_exact, send_all
)
from common.rudp import RUDPSocket


class FileTransferClient:
    """Клиент для передачи файлов."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.tcp_socket: Optional[socket.socket] = None
        self.connected = False
        self.download_dir = Path("./downloads")
        self.download_dir.mkdir(exist_ok=True)

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            buff_size = 50 * 1024 * 1024
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buff_size)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buff_size)
        except:
            pass

    def connect(self) -> bool:
        """Устанавливает TCP соединение с сервером."""
        try:
            self.tcp_socket = create_client_socket()
            self.tcp_socket.connect((self.host, self.port))
            self.connected = True
            welcome = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            if welcome:
                print(welcome.decode().strip())
            return True
        except socket.error as e:
            print(f"Connection failed: {e}")
            return False

    def disconnect(self) -> None:
        """Закрывает соединение."""
        if self.tcp_socket:
            self.send_command("QUIT")
            self.tcp_socket.close()
        self.connected = False
        self.udp_socket.close()

    def send_command(self, command: str, proto: str = 'TCP') -> Optional[str]:
        """Отправляет команду и получает ответ."""
        if proto == 'UDP':
            rudp = RUDPSocket(self.udp_socket, (self.host, self.port))
            return rudp.send_command(command)
        if not self.connected:
            return None
        try:
            send_all(self.tcp_socket, (command + "\n").encode())
            response = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            return response.decode().strip() if response else None
        except socket.error as e:
            print(f"Command failed: {e}")
            return None

    def upload_file(self, filepath: str, use_udp: bool = False) -> bool:
        """Загружает файл на сервер."""
        path = Path(filepath)
        if not path.exists():
            print(f"File not found: {filepath}")
            return False
        filename = path.name
        file_size = path.stat().st_size
        return self._do_upload(path, filename, file_size, offset=0, use_udp=use_udp)

    def resume_upload(self, filepath: str, offset: int, use_udp: bool = False) -> bool:
        """Продолжает загрузку файла с указанной позиции."""
        path = Path(filepath)
        if not path.exists():
            print(f"File not found: {filepath}")
            return False
        filename = path.name
        file_size = path.stat().st_size
        remaining = file_size - offset
        return self._do_upload(path, filename, remaining, offset, use_udp=use_udp)

    def _do_upload(self, path: Path, filename: str,
                   size: int, offset: int, use_udp: bool = False) -> bool:
        """Выполняет загрузку файла."""
        proto_flag = "--udp" if use_udp else "--tcp"
        if offset > 0:
            command = f"RESUME_UPLOAD {filename} {offset} {size} {proto_flag}"
        else:
            command = f"UPLOAD {filename} {size} {proto_flag}"

        if use_udp:
            rudp = RUDPSocket(self.udp_socket, (self.host, self.port))
            response = rudp.send_command(command)
            if not response or "READY" not in response:
                print(f"Server not ready (UDP): {response}")
                return False
        else:
            send_all(self.tcp_socket, (command + "\n").encode())
            raw_resp = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            response = raw_resp.decode().strip() if raw_resp else ""
            if not response or not response.startswith("READY"):
                print(f"Server not ready: {response}")
                return False

        print(f"Starting Upload via {'UDP' if use_udp else 'TCP'}...")
        start_time = time.time()

        if use_udp:
            try:
                with open(path, 'rb') as f:
                    f.seek(offset)
                    rudp = RUDPSocket(self.udp_socket, (self.host, self.port))
                    # Передаём колбэк прогресса: current_bytes -> None
                    rudp.send_stream(
                        f, size,
                        progress_callback=lambda sent: self._print_progress(sent, size)
                    )
                sent = size
            except Exception as e:
                print(f"UDP Upload error: {e}")
                sent = 0
        else:
            sent = self._send_file_data(path, size, offset)

        # Финальный ответ сервера по TCP
        final = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=10)
        if final:
            print(final.decode().strip())

        elapsed = time.time() - start_time
        self._print_stats("Upload", sent, elapsed)
        return sent == size

    def _send_file_data(self, path: Path, size: int, offset: int) -> int:
        """Отправляет данные файла (TCP)."""
        sent = 0
        try:
            with open(path, 'rb') as f:
                f.seek(offset)
                while sent < size:
                    chunk = f.read(min(BUFFER_SIZE, size - sent))
                    if not chunk:
                        break
                    if not send_all(self.tcp_socket, chunk):
                        print("Send failed")
                        break
                    sent += len(chunk)
                    self._print_progress(sent, size)
        except IOError as e:
            print(f"File read error: {e}")
        return sent

    def download_file(self, filename: str, use_udp: bool = False) -> bool:
        """Скачивает файл с сервера."""
        return self._do_download(filename, offset=0, use_udp=use_udp)

    def resume_download(self, filename: str, offset: int, use_udp: bool = False) -> bool:
        """Продолжает скачивание файла."""
        return self._do_download(filename, offset, use_udp=use_udp)

    def _do_download(self, filename: str, offset: int, use_udp: bool = False) -> bool:
        """Выполняет скачивание файла."""
        proto_flag = "--udp" if use_udp else "--tcp"
        if offset > 0:
            command = f"RESUME_DOWNLOAD {filename} {offset} {proto_flag}"
        else:
            command = f"DOWNLOAD {filename} {proto_flag}"

        file_size = 0
        if use_udp:
            rudp = RUDPSocket(self.udp_socket, (self.host, self.port))
            response_str = rudp.send_command(command)
            if not response_str:
                return False
        else:
            send_all(self.tcp_socket, (command + "\n").encode())
            response = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            if not response:
                return False
            response_str = response.decode().strip()
            if "ERROR" in response_str:
                print(response_str)
                return False

        file_size = self._parse_file_response(response_str)
        if file_size <= 0:
            print(f"Invalid response: {response_str}")
            return False

        print(f"Starting Download via {'UDP' if use_udp else 'TCP'}...")
        start_time = time.time()

        if use_udp:
            filepath = self.download_dir / filename
            mode = 'ab' if offset > 0 else 'wb'
            try:
                rudp = RUDPSocket(self.udp_socket, (self.host, self.port))
                with open(filepath, mode) as f:
                    # Передаём колбэк прогресса: current_bytes -> None
                    received = rudp.recv_stream(
                        f, total_size=file_size,
                        progress_callback=lambda recv: self._print_progress(recv, file_size)
                    )
            except Exception as e:
                print(f"UDP Download error: {e}")
                received = 0
        else:
            received = self._receive_file_data(filename, file_size, offset)

        elapsed = time.time() - start_time
        self._print_stats("Download", received, elapsed)
        return received == file_size

    def _parse_file_response(self, response: str) -> int:
        """
        Парсит ответ сервера.
        Форматы:
          UDP: "OK FILE <size>"
          TCP: "FILE <size>"
        """
        parts = response.split()
        if len(parts) >= 3 and parts[0] == "OK" and parts[1] == "FILE":
            try:
                return int(parts[2])
            except ValueError:
                pass
        if len(parts) >= 2 and parts[0] == "FILE":
            try:
                return int(parts[1])
            except ValueError:
                pass
        return 0

    def _receive_file_data(self, filename: str, size: int, offset: int) -> int:
        """Принимает данные файла (TCP)."""
        filepath = self.download_dir / filename
        mode = 'ab' if offset > 0 else 'wb'
        received = 0
        try:
            with open(filepath, mode) as f:
                while received < size:
                    chunk_size = min(BUFFER_SIZE, size - received)
                    data = recv_exact(self.tcp_socket, chunk_size, timeout=60)
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
        if total > 0:
            percent = (current / total) * 100
            print(f"\rProgress: {percent:.1f}% ({current}/{total} bytes)", end="", flush=True)

    def _print_stats(self, operation: str, bytes_transferred: int,
                     elapsed: float) -> None:
        print()  # Перевод строки после прогресса
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
        self.last_upload: Optional[Tuple[str, int, bool]] = None
        self.last_download: Optional[Tuple[str, int, bool]] = None

    def run(self) -> None:
        if not self.client.connect():
            pass
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
        print("\nAvailable commands:")
        print("  ECHO <text> [--udp]   - Echo text")
        print("  TIME [--udp]          - Get time")
        print("  UPLOAD <path> [--udp] - Upload file")
        print("  DOWNLOAD <name> [--udp]- Download file")
        print("  RESUME                - Resume last transfer")
        print("  QUIT                  - Disconnect")

    def _process_input(self, cmd: str) -> bool:
        parts = cmd.split()
        if not parts:
            return True
        command = parts[0].upper()
        use_udp = '--udp' in [p.lower() for p in parts]
        args_str = " ".join([p for p in parts[1:] if p.lower() != '--udp'])

        if command in ('QUIT', 'EXIT', 'CLOSE'):
            return False
        elif command == 'UPLOAD':
            return self._handle_upload(args_str, use_udp)
        elif command == 'DOWNLOAD':
            return self._handle_download(args_str, use_udp)
        elif command == 'RESUME':
            return self._handle_resume()
        else:
            response = self.client.send_command(cmd, 'UDP' if use_udp else 'TCP')
            if response:
                print(response)
        return True

    def _handle_upload(self, filepath: str, use_udp: bool) -> bool:
        if not filepath:
            print("Usage: UPLOAD <path> [--udp]")
            return True
        if self.client.upload_file(filepath, use_udp):
            self.last_upload = None
        else:
            self.last_upload = (filepath, 0, use_udp)
        return True

    def _handle_download(self, filename: str, use_udp: bool) -> bool:
        if not filename:
            print("Usage: DOWNLOAD <filename> [--udp]")
            return True
        if self.client.download_file(filename, use_udp):
            self.last_download = None
        else:
            self.last_download = (filename, 0, use_udp)
        return True

    def _handle_resume(self) -> bool:
        if self.last_upload:
            filepath, offset, udp = self.last_upload
            self.client.resume_upload(filepath, offset, udp)
        elif self.last_download:
            filename, offset, udp = self.last_download
            self.client.resume_download(filename, offset, udp)
        else:
            print("No transfer to resume")
        return True
