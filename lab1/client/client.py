"""TCP/UDP клиент."""

import socket
import time
from pathlib import Path
from typing import Optional, Tuple

from common.protocol import COMMAND_TERMINATOR, BUFFER_SIZE
from common.socket_utils import create_client_socket, recv_until, recv_exact, send_all
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
        self.udp_socket.setblocking(False)
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try:
                self.udp_socket.setsockopt(socket.SOL_SOCKET, opt,
                                            64 * 1024 * 1024)
            except Exception:
                pass

        self._prog_ts  = 0.0
        self._prog_pct = -1

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

    def disconnect(self):
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

    def send_command(self, command: str, proto: str = "TCP") -> Optional[str]:
        if proto == "UDP":
            return RUDPSocket(self.udp_socket,
                              (self.host, self.port)).send_command(command)
        if not self.connected:
            return None
        try:
            send_all(self.tcp_socket, (command + "\n").encode())
            r = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            return r.decode().strip() if r else None
        except socket.error as e:
            print(f"Command failed: {e}")
            return None

    # ── upload ─────────────────────────────────────────────

    def upload_file(self, filepath: str, use_udp: bool = False) -> bool:
        p = Path(filepath)
        if not p.exists():
            print(f"File not found: {filepath}")
            return False
        return self._do_upload(p, p.name, p.stat().st_size, 0, use_udp)

    def resume_upload(self, filepath: str, offset: int,
                      use_udp: bool = False) -> bool:
        p = Path(filepath)
        if not p.exists():
            print(f"File not found: {filepath}")
            return False
        return self._do_upload(p, p.name, p.stat().st_size - offset,
                               offset, use_udp)

    def _do_upload(self, path: Path, filename: str,
                   size: int, offset: int, use_udp: bool) -> bool:
        flag = "--udp" if use_udp else "--tcp"
        cmd  = (f"RESUME_UPLOAD {filename} {offset} {size} {flag}"
                if offset > 0
                else f"UPLOAD {filename} {size} {flag}")

        if use_udp:
            resp = RUDPSocket(self.udp_socket,
                              (self.host, self.port)).send_command(cmd)
            if not resp or "READY" not in resp:
                print(f"Server not ready: {resp}")
                return False
        else:
            if not self.connected:
                return False
            send_all(self.tcp_socket, (cmd + "\n").encode())
            raw  = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            resp = raw.decode().strip() if raw else ""
            if not resp or not resp.startswith("READY"):
                print(f"Server not ready: {resp}")
                return False

        print(f"Uploading via {'UDP' if use_udp else 'TCP'}...")
        t0 = time.time()

        if use_udp:
            # Небольшая пауза, чтобы сервер успел зарегистрировать сессию
            time.sleep(0.05)
            try:
                with open(path, "rb") as f:
                    f.seek(offset)
                    RUDPSocket(self.udp_socket,
                               (self.host, self.port)).send_stream(
                        f, total_size=size,
                        progress_callback=lambda s: self._prog(s, size),
                    )
                sent = size
            except Exception as e:
                print(f"\nUDP upload error: {e}")
                sent = 0
            self._stats("Upload", sent, time.time() - t0)
            return sent == size

        sent  = self._send_tcp(path, size, offset)
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

    # ── download ───────────────────────────────────────────

    def download_file(self, filename: str, use_udp: bool = False) -> bool:
        return self._do_download(filename, 0, use_udp)

    def resume_download(self, filename: str, offset: int,
                        use_udp: bool = False) -> bool:
        return self._do_download(filename, offset, use_udp)

    def _do_download(self, filename: str, offset: int, use_udp: bool) -> bool:
        filename = Path(filename).name
        flag     = "--udp" if use_udp else "--tcp"
        cmd      = (f"RESUME_DOWNLOAD {filename} {offset} {flag}"
                    if offset > 0
                    else f"DOWNLOAD {filename} {flag}")

        if use_udp:
            resp_str = RUDPSocket(self.udp_socket,
                                  (self.host, self.port)).send_command(cmd)
            if not resp_str:
                print("No UDP response")
                return False
        else:
            if not self.connected:
                return False
            send_all(self.tcp_socket, (cmd + "\n").encode())
            r = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            if not r:
                print("No response")
                return False
            resp_str = r.decode().strip()

        if "ERROR" in resp_str:
            print(resp_str)
            return False

        fsize = self._parse_file_resp(resp_str)
        if fsize <= 0:
            print(f"Bad response: {resp_str}")
            return False

        print(f"Downloading via {'UDP' if use_udp else 'TCP'}...")
        t0 = time.time()

        if use_udp:
            filepath = self.download_dir / filename
            mode     = "ab" if offset > 0 else "wb"
            try:
                rudp = RUDPSocket(self.udp_socket, (self.host, self.port))
                with open(filepath, mode) as f:
                    received = rudp.recv_stream(
                        f, total_size=fsize,
                        progress_callback=lambda r: self._prog(r, fsize),
                    )
            except Exception as e:
                print(f"\nUDP download error: {e}")
                received = 0
        else:
            received = self._recv_tcp(filename, fsize, offset)

        self._stats("Download", received, time.time() - t0)
        if received != fsize:
            print("Incomplete transfer")
            return False
        return True

    def _recv_tcp(self, filename: str, size: int, offset: int) -> int:
        fp  = self.download_dir / filename
        rcv = 0
        try:
            with open(fp, "ab" if offset > 0 else "wb") as f:
                while rcv < size:
                    data = recv_exact(self.tcp_socket,
                                      min(BUFFER_SIZE, size - rcv),
                                      timeout=60)
                    if data is None:
                        print("\nConnection lost")
                        break
                    f.write(data)
                    rcv += len(data)
                    self._prog(rcv, size)
        except IOError as e:
            print(f"Write error: {e}")
        return rcv

    def _parse_file_resp(self, r: str) -> int:
        parts = r.split()
        for i, p in enumerate(parts):
            if p == "FILE" and i + 1 < len(parts):
                try:
                    return int(parts[i + 1])
                except ValueError:
                    pass
        return 0

    def _prog(self, cur: int, total: int):
        if total <= 0:
            return
        pct = int(cur / total * 100)
        now = time.time()
        if cur == total or now - self._prog_ts >= 0.1 or pct != self._prog_pct:
            print(f"\rProgress: {pct}% ({cur}/{total})", end="", flush=True)
            self._prog_ts  = now
            self._prog_pct = pct

    def _stats(self, op: str, n: int, t: float):
        print()
        if t > 0:
            bps = n / t
            if bps >= 1 << 20:
                print(f"{op}: {bps/(1<<20):.2f} MB/s")
            elif bps >= 1024:
                print(f"{op}: {bps/1024:.2f} KB/s")
            else:
                print(f"{op}: {bps:.2f} B/s")


# ── InteractiveClient ──────────────────────────────────────

class InteractiveClient:

    def __init__(self, host: str, port: int):
        self.client = FileTransferClient(host, port)
        self.last_up:   Optional[Tuple[str, int, bool]] = None
        self.last_down: Optional[Tuple[str, int, bool]] = None

    def run(self):
        if not self.client.connect():
            print("Cannot connect. Exiting.")
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

    def _help(self):
        print("\nCommands:")
        print("  ECHO <text> [--udp]")
        print("  TIME [--udp]")
        print("  UPLOAD <file> [--udp]")
        print("  DOWNLOAD <file> [--udp]")
        print("  RESUME")
        print("  QUIT")

    def _process(self, cmd: str) -> bool:
        parts   = cmd.split()
        command = parts[0].upper()
        use_udp = "--udp" in [p.lower() for p in parts]
        args    = " ".join(p for p in parts[1:] if p.lower() != "--udp")

        if command in ("QUIT", "EXIT", "CLOSE"):
            return False
        if command == "UPLOAD":
            if not args:
                print("Usage: UPLOAD <file> [--udp]")
                return True
            ok = self.client.upload_file(args, use_udp)
            self.last_up = None if ok else (args, 0, use_udp)
            return True
        if command == "DOWNLOAD":
            if not args:
                print("Usage: DOWNLOAD <file> [--udp]")
                return True
            ok = self.client.download_file(args, use_udp)
            self.last_down = None if ok else (args, 0, use_udp)
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
