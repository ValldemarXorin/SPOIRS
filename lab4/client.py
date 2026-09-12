"""Lab 4 Variant 4 Client."""

import socket
import time
import sys
import select
import struct
import threading
from pathlib import Path
from typing import Optional, Callable

from common.protocol import (
    COMMAND_TERMINATOR, BUFFER_SIZE, PacketType
)
from common.socket_utils import create_udp_socket
from common.rudp import RudpSocket

_HDR = struct.Struct("!IB")


class Variant4Client:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.connected = False

        self.download_dir = Path("./downloads")
        self.download_dir.mkdir(exist_ok=True)

        self.udp_socket = create_udp_socket()
        self.udp_socket.setblocking(False)

        try:
            self.udp_socket.bind(("", 0))
        except OSError as e:
            print(f"UDP bind warning: {e}")

        self._rudp: Optional[RudpSocket] = None
        self._rudp_lock = threading.Lock()
        self._response_event = threading.Event()
        self._last_response: Optional[str] = None

    def connect(self) -> bool:
        """Устанавливает RUDP соединение."""
        try:
            self._rudp = RudpSocket(self.udp_socket, dest_addr=(self.host, self.port))
            self._rudp.set_peer_filter((self.host, self.port))

            def data_cb(data: bytes):
                self._last_response = data.decode(errors="ignore").strip()
                self._response_event.set()

            self._rudp.set_data_callback(data_cb)

            if not self._rudp.connect((self.host, self.port), timeout=10.0):
                print("Failed to connect via RUDP")
                return False

            self.connected = True
            # Читаем приветствие
            welcome = self._wait_response(5)
            if welcome:
                print(welcome)
            return True

        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def disconnect(self) -> None:
        self.connected = False
        with self._rudp_lock:
            if self._rudp:
                self._rudp.close()
                self._rudp = None
        try:
            self.udp_socket.close()
        except OSError:
            pass

    def _wait_response(self, timeout: float) -> Optional[str]:
        if self._response_event.wait(timeout):
            self._response_event.clear()
            resp = self._last_response
            self._last_response = None
            return resp
        return None

    def send_command(self, command: str) -> Optional[str]:
        if not self.connected or not self._rudp:
            return None
        try:
            return self._rudp.send_command(command, timeout=10)
        except Exception as e:
            print(f"Command failed: {e}")
            return None

    def upload_file(self, filepath: str) -> bool:
        path = Path(filepath)
        if not path.exists():
            print(f"Not found: {filepath}")
            return False
        return self._do_upload(path, path.name, path.stat().st_size, 0)

    def resume_upload(self, filepath: str, offset: int) -> bool:
        path = Path(filepath)
        if not path.exists():
            print(f"Not found: {filepath}")
            return False
        return self._do_upload(path, path.name, path.stat().st_size - offset, offset)

    def _do_upload(self, path: Path, filename: str, size: int, offset: int) -> bool:
        if not self._rudp:
            return False

        cmd = f"RESUME_UPLOAD {filename} {offset} {size}" if offset else f"UPLOAD {filename} {size}"
        print(f"Uploading {filename} ({size} bytes) via RUDP...")

        resp = self._rudp.send_command(cmd, timeout=10)
        if not resp or not resp.startswith("OK"):
            print(f"Not ready: {resp}")
            return False

        print("Server ready, sending file...")
        t0 = time.time()
        sent = 0

        def reader(chunk_size: int) -> bytes:
            nonlocal sent
            with open(path, "rb") as f:
                f.seek(offset + sent)
                chunk = f.read(min(chunk_size, size - sent))
                if chunk:
                    sent += len(chunk)
                    self._prog(sent, size)
                return chunk

        try:
            self._rudp.send_stream(reader, size, lambda x: self._prog(x, size))
            print("\nUpload completed successfully")
        except Exception as e:
            print(f"\nUpload error: {e}")
            return False

        self._stats("Upload", sent, time.time() - t0)
        return sent == size

    def download_file(self, filename: str) -> bool:
        return self._do_download(filename, 0)

    def resume_download(self, filename: str, offset: int) -> bool:
        return self._do_download(filename, offset)

    def _do_download(self, filename: str, offset: int) -> bool:
        if not self._rudp:
            return False

        filename = Path(filename).name
        cmd = f"RESUME_DOWNLOAD {filename} {offset}" if offset else f"DOWNLOAD {filename}"
        print(f"Downloading {filename} via RUDP...")

        resp = self._rudp.send_command(cmd, timeout=10)
        if not resp or "ERROR" in resp:
            print(resp or "No response")
            return False

        parts = resp.split()
        fsize = 0
        for i, p in enumerate(parts):
            if p == "FILE" and i + 1 < len(parts):
                try:
                    fsize = int(parts[i + 1])
                    break
                except ValueError:
                    pass

        if fsize <= 0:
            print(f"Bad response: {resp}")
            return False

        print(f"File size: {fsize} bytes")
        fp = self.download_dir / filename
        mode = "ab" if offset else "wb"
        t0 = time.time()
        received = 0

        def writer(data: bytes):
            nonlocal received
            with open(fp, mode) as f:
                f.write(data)
            received += len(data)
            self._prog(received, fsize)

        try:
            self._rudp.recv_stream(writer, fsize, lambda x: self._prog(x, fsize))
            print(f"\nDownload completed: {received} bytes")
        except Exception as e:
            print(f"\nDownload error: {e}")
            return False

        self._stats("Download", received, time.time() - t0)
        return received == fsize

    def _prog(self, cur: int, total: int) -> None:
        if total <= 0:
            return
        pct = int(cur / total * 100)
        now = time.time()
        if cur == total or now - getattr(self, "_prog_ts", 0) >= 0.2 or pct != getattr(self, "_prog_pct", -1):
            print(f"\rProgress: {pct}% ({cur}/{total})", end="", flush=True)
            self._prog_ts = now
            self._prog_pct = pct

    def _stats(self, op: str, n: int, t: float) -> None:
        print()
        if t <= 0:
            return
        bps = n / t
        if bps >= 1 << 20:
            print(f"{op}: {bps / (1 << 20):.2f} MB/s")
        elif bps >= 1024:
            print(f"{op}: {bps / 1024:.2f} KB/s")
        else:
            print(f"{op}: {bps:.2f} B/s")


class InteractiveClient:
    def __init__(self, host: str, port: int):
        self.client = Variant4Client(host, port)

    def run(self) -> None:
        if not self.client.connect():
            print("Cannot connect.")
            return
        self._help()
        try:
            while True:
                try:
                    cmd = input("\n> ").strip()
                except EOFError:
                    break
                if not cmd:
                    continue
                if not self._process(cmd):
                    break
        except KeyboardInterrupt:
            print("\nInterrupted")
        finally:
            self.client.disconnect()

    def _help(self) -> None:
        print("\nCommands:")
        for c in [
            "ECHO <text>",
            "TIME",
            "UPLOAD <path>",
            "DOWNLOAD <filename>",
            "RESUME_UPLOAD <path> <offset>",
            "RESUME_DOWNLOAD <filename> <offset>",
            "QUIT",
        ]:
            print(f"  {c}")

    def _process(self, cmd: str) -> bool:
        parts = cmd.split()
        command = parts[0].upper()

        if command in ("QUIT", "EXIT", "CLOSE"):
            return False

        if command == "UPLOAD":
            if len(parts) < 2:
                print("Usage: UPLOAD <path>")
                return True
            self.client.upload_file(parts[1])
            return True

        if command == "DOWNLOAD":
            if len(parts) < 2:
                print("Usage: DOWNLOAD <filename>")
                return True
            self.client.download_file(parts[1])
            return True

        if command == "RESUME_UPLOAD":
            if len(parts) < 3:
                print("Usage: RESUME_UPLOAD <path> <offset>")
                return True
            self.client.resume_upload(parts[1], int(parts[2]))
            return True

        if command == "RESUME_DOWNLOAD":
            if len(parts) < 3:
                print("Usage: RESUME_DOWNLOAD <filename> <offset>")
                return True
            self.client.resume_download(parts[1], int(parts[2]))
            return True

        resp = self.client.send_command(cmd)
        if resp:
            print(resp)
        return True


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Lab 4 Variant 4 Client")
    parser.add_argument("host", default="127.0.0.1", nargs="?")
    parser.add_argument("port", type=int, default=9000, nargs="?")
    args = parser.parse_args()

    client = InteractiveClient(args.host, args.port)
    client.run()


if __name__ == "__main__":
    main()