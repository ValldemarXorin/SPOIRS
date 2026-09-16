"""TCP/UDP клиент с быстрым RUDP upload/download."""

import socket
import time
import sys
import select
import struct
import threading
from pathlib import Path
from typing import Optional, Tuple, Callable

from lab1.common.protocol import (
    COMMAND_TERMINATOR, BUFFER_SIZE, PacketType
)
from lab1.common.socket_utils import (
    create_client_socket,
    recv_until,
    recv_exact,
    send_all,
    create_udp_socket,
)
from lab1.common.rudp import RudpSocket, create_rudp_socket

_HDR = struct.Struct("!IB")


class FileTransferClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

        self.tcp_socket: Optional[socket.socket] = None
        self.connected = False

        self.download_dir = Path("./downloads")
        self.download_dir.mkdir(exist_ok=True)

        self.udp_socket = create_udp_socket()
        self.udp_socket.setblocking(False)

        try:
            self.udp_socket.bind(("", 0))
        except OSError as e:
            print(f"UDP bind warning: {e}")

        self._prog_ts = 0.0
        self._prog_pct = -1
        self._rudp: Optional[RudpSocket] = None
        self._rudp_lock = threading.Lock()

    # ── connect / disconnect ──────────────────────────────

    def connect(self) -> bool:
        try:
            self.tcp_socket = create_client_socket()
            self.tcp_socket.connect((self.host, self.port))
            self.connected = True
            welcome = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=5)
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
            except OSError:
                pass
        self.connected = False
        with self._rudp_lock:
            if self._rudp:
                self._rudp.close()
                self._rudp = None
        try:
            self.udp_socket.close()
        except OSError:
            pass

    def _get_rudp(self) -> RudpSocket:
        """Получает или создаёт RUDP сокет для файловых передач."""
        with self._rudp_lock:
            if self._rudp is None:
                self._rudp = RudpSocket(self.udp_socket, dest_addr=(self.host, self.port))
                self._rudp.set_peer_filter((self.host, self.port))
            return self._rudp

    # ── commands ──────────────────────────────────────────

    def send_command(self, command: str, proto: str = "TCP") -> Optional[str]:
        if proto == "UDP":
            rudp = self._get_rudp()
            return rudp.send_command(command)

        if not self.connected:
            return None
        try:
            send_all(self.tcp_socket, (command + "\n").encode())
            resp = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=10)
            return resp.decode().strip() if resp else None
        except socket.error as e:
            print(f"Command failed: {e}")
            return None

    # ── upload ────────────────────────────────────────────

    def upload_file(self, filepath: str, use_udp: bool = False) -> bool:
        path = Path(filepath)
        if not path.exists():
            print(f"Not found: {filepath}")
            return False
        return self._do_upload(path, path.name, path.stat().st_size, 0, use_udp)

    def resume_upload(self, filepath: str, offset: int, use_udp: bool = False) -> bool:
        path = Path(filepath)
        if not path.exists():
            print(f"Not found: {filepath}")
            return False
        return self._do_upload(path, path.name, path.stat().st_size - offset, offset, use_udp)

    def _do_upload(
        self,
        path: Path,
        filename: str,
        size: int,
        offset: int,
        use_udp: bool,
    ) -> bool:
        flag = "--udp" if use_udp else "--tcp"
        cmd = (
            f"RESUME_UPLOAD {filename} {offset} {size} {flag}"
            if offset
            else f"UPLOAD {filename} {size} {flag}"
        )

        if use_udp:
            print("Uploading via RUDP (UDP)...")
            rudp = self._get_rudp()

            # Отправляем команду UPLOAD через RUDP
            resp = rudp.send_command(cmd, timeout=10)
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
                rudp.send_stream(reader, size, lambda x: self._prog(x, size))
                print("\nUpload completed successfully")
            except Exception as e:
                print(f"\nUpload error: {e}")
                return False

            self._stats("Upload", sent, time.time() - t0)
            return sent == size

        # TCP upload
        if not self.connected:
            return False
        send_all(self.tcp_socket, (cmd + "\n").encode())
        raw = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=10)
        resp = raw.decode().strip() if raw else ""
        if not resp or not resp.startswith("READY"):
            print(f"Not ready: {resp}")
            return False

        print("Uploading via TCP...")
        t0 = time.time()
        sent = self._send_tcp(path, size, offset)
        final = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=10)
        if final:
            print(final.decode().strip())
        self._stats("Upload", sent, time.time() - t0)
        return sent == size

    def _send_tcp(self, path: Path, size: int, offset: int) -> int:
        sent = 0
        try:
            with open(path, "rb") as f:
                f.seek(offset)
                while sent < size:
                    chunk = f.read(min(BUFFER_SIZE, size - sent))
                    if not chunk:
                        break
                    if not send_all(self.tcp_socket, chunk):
                        break
                    sent += len(chunk)
                    self._prog(sent, size)
        except IOError as e:
            print(f"Read error: {e}")
        return sent

    # ── download ──────────────────────────────────────────

    def download_file(self, filename: str, use_udp: bool = False) -> bool:
        return self._do_download(filename, 0, use_udp)

    def resume_download(self, filename: str, offset: int, use_udp: bool = False) -> bool:
        return self._do_download(filename, offset, use_udp)

    def _do_download(self, filename: str, offset: int, use_udp: bool) -> bool:
        filename = Path(filename).name
        flag = "--udp" if use_udp else "--tcp"
        cmd = (
            f"RESUME_DOWNLOAD {filename} {offset} {flag}"
            if offset
            else f"DOWNLOAD {filename} {flag}"
        )

        if use_udp:
            print("Downloading via RUDP (UDP)...")
            rudp = self._get_rudp()

            # Отправляем команду DOWNLOAD через RUDP
            resp = rudp.send_command(cmd, timeout=10)
            if not resp or "ERROR" in resp:
                print(resp or "No response")
                return False

            # Парсим размер файла
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
                rudp.recv_stream(writer, fsize, lambda x: self._prog(x, fsize))
                print(f"\nDownload completed: {received} bytes")
            except Exception as e:
                print(f"\nDownload error: {e}")
                return False

            self._stats("Download", received, time.time() - t0)
            return received == fsize

        # TCP download
        if not self.connected:
            return False
        send_all(self.tcp_socket, (cmd + "\n").encode())
        r = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=10)
        if not r:
            print("No response")
            return False
        resp_str = r.decode().strip()

        if "ERROR" in resp_str:
            print(resp_str)
            return False

        fsize = self._parse_resp(resp_str)
        if fsize <= 0:
            print(f"Bad resp: {resp_str}")
            return False

        print("Downloading via TCP...")
        t0 = time.time()
        received = self._recv_tcp(filename, fsize, offset)
        self._stats("Download", received, time.time() - t0)
        if received != fsize:
            print(f"Incomplete: received {received} of {fsize} bytes")
            return False
        return True

    def _recv_tcp(self, filename: str, size: int, offset: int) -> int:
        fp = self.download_dir / filename
        received = 0
        try:
            with open(fp, "ab" if offset else "wb") as f:
                while received < size:
                    data = recv_exact(
                        self.tcp_socket,
                        min(BUFFER_SIZE, size - received),
                        timeout=60,
                    )
                    if data is None:
                        print("\nConnection lost")
                        break
                    f.write(data)
                    received += len(data)
                    self._prog(received, size)
        except IOError as e:
            print(f"Write error: {e}")
        return received

    # ── utils ─────────────────────────────────────────────

    @staticmethod
    def _parse_resp(resp: str) -> int:
        parts = resp.split()
        for i, p in enumerate(parts):
            if p == "FILE" and i + 1 < len(parts):
                try:
                    return int(parts[i + 1])
                except ValueError:
                    return 0
        return 0

    def _prog(self, cur: int, total: int) -> None:
        if total <= 0:
            return
        pct = int(cur / total * 100)
        now = time.time()
        if cur == total or now - self._prog_ts >= 0.2 or pct != self._prog_pct:
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
        self.client = FileTransferClient(host, port)
        self.last_up: Optional[Tuple[str, int, bool]] = None
        self.last_down: Optional[Tuple[str, int, bool]] = None

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
            "ECHO [--udp]",
            "TIME [--udp]",
            "UPLOAD [--udp]",
            "DOWNLOAD [--udp]",
            "RESUME",
            "QUIT",
        ]:
            print(f"  {c}")

    def _process(self, cmd: str) -> bool:
        parts = cmd.split()
        command = parts[0].upper()
        use_udp = "--udp" in [p.lower() for p in parts]
        args = " ".join(p for p in parts[1:] if p.lower() != "--udp")

        if command in ("QUIT", "EXIT", "CLOSE"):
            return False

        if command == "UPLOAD":
            if not args:
                print("Usage: UPLOAD <path> [--udp]")
                return True
            ok = self.client.upload_file(args, use_udp)
            if not ok:
                self.last_up = (args, 0, use_udp)
            else:
                self.last_up = None
            return True

        if command == "DOWNLOAD":
            if not args:
                print("Usage: DOWNLOAD <filename> [--udp]")
                return True
            ok = self.client.download_file(args, use_udp)
            if not ok:
                self.last_down = (args, 0, use_udp)
            else:
                self.last_down = None
            return True

        if command == "RESUME":
            if self.last_up:
                self.client.resume_upload(*self.last_up)
            elif self.last_down:
                self.client.resume_download(*self.last_down)
            else:
                print("Nothing to resume")
            return True

        resp = self.client.send_command(cmd, "UDP" if use_udp else "TCP")
        if resp:
            print(resp)
        return True