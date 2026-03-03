"""TCP/UDP клиент для работы с сервером."""

import socket
import time
from pathlib import Path
from typing import Optional, Tuple

from common.protocol import COMMAND_TERMINATOR, BUFFER_SIZE
from common.socket_utils import (
    create_client_socket, recv_until, recv_exact, send_all
)
from common.rudp import RUDPSocket


class FileTransferClient:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.tcp_socket: Optional[socket.socket] = None
        self.connected = False
        self.download_dir = Path("./downloads")
        self.download_dir.mkdir(exist_ok=True)

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            buf = 64 * 1024 * 1024
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buf)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buf)
        except Exception:
            pass

        self._progress_last_ts      = 0.0
        self._progress_last_percent = -1

    # ── connection ─────────────────────────────────────────

    def connect(self) -> bool:
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
        if self.tcp_socket:
            try:
                self.send_command("QUIT")
            except Exception:
                pass
            try:
                self.tcp_socket.close()
            except Exception:
                pass
        self.connected = False
        try:
            self.udp_socket.close()
        except Exception:
            pass

    # ── send command ───────────────────────────────────────

    def send_command(self, command: str, proto: str = "TCP") -> Optional[str]:
        if proto == "UDP":
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

    # ── upload ─────────────────────────────────────────────

    def upload_file(self, filepath: str, use_udp: bool = False) -> bool:
        path = Path(filepath)
        if not path.exists():
            print(f"File not found: {filepath}")
            return False
        return self._do_upload(path, path.name, path.stat().st_size, 0, use_udp)

    def resume_upload(self, filepath: str, offset: int,
                      use_udp: bool = False) -> bool:
        path = Path(filepath)
        if not path.exists():
            print(f"File not found: {filepath}")
            return False
        file_size = path.stat().st_size
        return self._do_upload(path, path.name, file_size - offset, offset, use_udp)

    def _do_upload(self, path: Path, filename: str,
                   size: int, offset: int, use_udp: bool) -> bool:
        proto_flag = "--udp" if use_udp else "--tcp"
        command = (f"RESUME_UPLOAD {filename} {offset} {size} {proto_flag}"
                   if offset > 0
                   else f"UPLOAD {filename} {size} {proto_flag}")

        if use_udp:
            rudp_cmd = RUDPSocket(self.udp_socket, (self.host, self.port))
            response = rudp_cmd.send_command(command)
            if not response or "READY" not in response:
                print(f"Server not ready (UDP): {response}")
                return False
        else:
            if not self.connected:
                print("TCP not connected")
                return False
            send_all(self.tcp_socket, (command + "\n").encode())
            raw = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            response = raw.decode().strip() if raw else ""
            if not response or not response.startswith("READY"):
                print(f"Server not ready: {response}")
                return False

        print(f"Starting Upload via {'UDP' if use_udp else 'TCP'}...")
        start_time = time.time()

        if use_udp:
            # Короткая пауза — даём серверу создать сессию
            time.sleep(0.05)
            try:
                with open(path, "rb") as f:
                    f.seek(offset)
                    rudp = RUDPSocket(self.udp_socket, (self.host, self.port))
                    rudp.send_stream(
                        f,
                        total_size=size,
                        progress_callback=lambda s: self._print_progress(s, size),
                    )
                sent = size
            except Exception as e:
                print(f"\nUDP Upload error: {e}")
                sent = 0
            elapsed = time.time() - start_time
            self._print_stats("Upload", sent, elapsed)
            return sent == size

        sent  = self._send_file_data(path, size, offset)
        final = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=10)
        if final:
            print(final.decode().strip())
        elapsed = time.time() - start_time
        self._print_stats("Upload", sent, elapsed)
        return sent == size

    def _send_file_data(self, path: Path, size: int, offset: int) -> int:
        sent = 0
        try:
            with open(path, "rb") as f:
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

    # ── download ───────────────────────────────────────────

    def download_file(self, filename: str, use_udp: bool = False) -> bool:
        return self._do_download(filename, 0, use_udp)

    def resume_download(self, filename: str, offset: int,
                        use_udp: bool = False) -> bool:
        return self._do_download(filename, offset, use_udp)

    def _do_download(self, filename: str, offset: int, use_udp: bool) -> bool:
        filename   = Path(filename).name
        proto_flag = "--udp" if use_udp else "--tcp"
        command    = (f"RESUME_DOWNLOAD {filename} {offset} {proto_flag}"
                      if offset > 0
                      else f"DOWNLOAD {filename} {proto_flag}")

        if use_udp:
            rudp_cmd     = RUDPSocket(self.udp_socket, (self.host, self.port))
            response_str = rudp_cmd.send_command(command)
            if not response_str:
                print("No UDP response")
                return False
        else:
            if not self.connected:
                print("TCP not connected")
                return False
            send_all(self.tcp_socket, (command + "\n").encode())
            response = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            if not response:
                print("No TCP response")
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
            mode     = "ab" if offset > 0 else "wb"
            time.sleep(0.05)   # даём серверу начать отправку
            try:
                rudp = RUDPSocket(self.udp_socket, (self.host, self.port))
                with open(filepath, mode) as f:
                    received = rudp.recv_stream(
                        f,
                        total_size=file_size,
                        progress_callback=lambda r: self._print_progress(r, file_size),
                    )
            except Exception as e:
                print(f"\nUDP Download error: {e}")
                received = 0
        else:
            received = self._receive_file_data(filename, file_size, offset)

        elapsed = time.time() - start_time
        self._print_stats("Download", received, elapsed)

        if received != file_size:
            print("Download incomplete (size mismatch)")
            return False
        return True

    # ── helpers ────────────────────────────────────────────

    def _parse_file_response(self, response: str) -> int:
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
        filepath = self.download_dir / filename
        received = 0
        try:
            with open(filepath, "ab" if offset > 0 else "wb") as f:
                while received < size:
                    data = recv_exact(self.tcp_socket,
                                      min(BUFFER_SIZE, size - received),
                                      timeout=60)
                    if data is None:
                        print("\nConnection lost")
                        break
                    f.write(data)
                    received += len(data)
                    self._print_progress(received, size)
        except IOError as e:
            print(f"File write error: {e}")
        return received

    def _print_progress(self, current: int, total: int) -> None:
        if total <= 0:
            return
        pct = int(current / total * 100)
        now = time.time()
        if (current == total
                or now - self._progress_last_ts >= 0.1
                or pct != self._progress_last_percent):
            print(f"\rProgress: {pct}% ({current}/{total} bytes)",
                  end="", flush=True)
            self._progress_last_ts      = now
            self._progress_last_percent = pct

    def _print_stats(self, op: str, n: int, elapsed: float) -> None:
        print()
        if elapsed > 0:
            bps = n / elapsed
            if bps >= 1 << 20:
                print(f"{op} complete: {bps/(1<<20):.2f} MB/s")
            elif bps >= 1024:
                print(f"{op} complete: {bps/1024:.2f} KB/s")
            else:
                print(f"{op} complete: {bps:.2f} B/s")


# ── interactive shell ──────────────────────────────────────

class InteractiveClient:

    def __init__(self, host: str, port: int):
        self.client        = FileTransferClient(host, port)
        self.last_upload:   Optional[Tuple[str, int, bool]] = None
        self.last_download: Optional[Tuple[str, int, bool]] = None

    def run(self) -> None:
        if not self.client.connect():
            print("Could not connect to server. Exiting.")
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
        print("\nAvailable commands:")
        print("  ECHO <text> [--udp]         - Echo text")
        print("  TIME [--udp]                - Get server time")
        print("  UPLOAD <file> [--udp]       - Upload file")
        print("  DOWNLOAD <file> [--udp]     - Download file")
        print("  RESUME                      - Resume last transfer")
        print("  QUIT                        - Disconnect")

    def _process_input(self, cmd: str) -> bool:
        parts   = cmd.split()
        command = parts[0].upper()
        use_udp = "--udp" in [p.lower() for p in parts]
        args    = " ".join(p for p in parts[1:] if p.lower() != "--udp")

        if command in ("QUIT", "EXIT", "CLOSE"):
            return False
        if command == "UPLOAD":
            return self._handle_upload(args, use_udp)
        if command == "DOWNLOAD":
            return self._handle_download(args, use_udp)
        if command == "RESUME":
            return self._handle_resume()

        resp = self.client.send_command(cmd, "UDP" if use_udp else "TCP")
        if resp:
            print(resp)
        return True

    def _handle_upload(self, filepath: str, use_udp: bool) -> bool:
        if not filepath:
            print("Usage: UPLOAD <filepath> [--udp]")
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
            print("Download failed or incomplete")
            self.last_download = (filename, 0, use_udp)
        return True

    def _handle_resume(self) -> bool:
        if self.last_upload:
            fp, off, udp = self.last_upload
            self.client.resume_upload(fp, off, udp)
        elif self.last_download:
            fn, off, udp = self.last_download
            self.client.resume_download(fn, off, udp)
        else:
            print("No transfer to resume")
        return True
