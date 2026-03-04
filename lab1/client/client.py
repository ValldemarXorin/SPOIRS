"""TCP/UDP клиент. Кроссплатформенный: Windows + Linux."""

import socket
import time
import select
import struct
from pathlib import Path
from typing import Optional, Tuple

from common.protocol import (
    COMMAND_TERMINATOR, BUFFER_SIZE, PacketType, UDP_HEADER_SIZE,
)
from common.socket_utils import (
    create_client_socket, create_udp_socket,
    recv_until, recv_exact, send_all,
)
from common.rudp import RUDPSocket, ConnectionLostError

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

        self._prog_ts = 0.0
        self._prog_pct = -1

    def connect(self) -> bool:
        try:
            self.tcp_socket = create_client_socket()
            self.tcp_socket.connect((self.host, self.port))
            self.connected = True
            welcome = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=5)
            if welcome:
                print(welcome.decode(errors="ignore").strip())
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
        try:
            self.udp_socket.close()
        except OSError:
            pass

    def send_command(self, command: str, proto: str = "TCP") -> Optional[str]:
        if proto == "UDP":
            rudp = RUDPSocket(self.udp_socket, (self.host, self.port))
            return rudp.send_command(command)
        if not self.connected:
            return None
        try:
            send_all(self.tcp_socket, (command + "\n").encode())
            resp = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=10)
            return resp.decode(errors="ignore").strip() if resp else None
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

    def resume_upload(self, filepath: str, offset: int,
                      use_udp: bool = False) -> bool:
        path = Path(filepath)
        if not path.exists():
            print(f"Not found: {filepath}")
            return False
        return self._do_upload(
            path, path.name, path.stat().st_size - offset, offset, use_udp
        )

    def _do_upload(self, path: Path, filename: str, size: int,
                   offset: int, use_udp: bool) -> bool:
        flag = "--udp" if use_udp else "--tcp"
        cmd = (f"RESUME_UPLOAD {filename} {offset} {size} {flag}" if offset
               else f"UPLOAD {filename} {size} {flag}")

        if use_udp:
            rudp_cmd = RUDPSocket(self.udp_socket, (self.host, self.port))
            resp = rudp_cmd.send_command(cmd)
            if not resp or "READY" not in resp:
                print(f"Not ready: {resp}")
                return False
        else:
            if not self.connected:
                return False
            send_all(self.tcp_socket, (cmd + "\n").encode())
            raw = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=10)
            resp = raw.decode(errors="ignore").strip() if raw else ""
            if not resp or not resp.startswith("READY"):
                print(f"Not ready: {resp}")
                return False

        proto_name = "UDP" if use_udp else "TCP"
        print(f"Uploading {filename} ({size} bytes) via {proto_name}...")
        self._prog_pct = -1
        t0 = time.time()

        if use_udp:
            sent = 0
            try:
                rudp = RUDPSocket(self.udp_socket, (self.host, self.port))
                with open(path, "rb") as f:
                    f.seek(offset)
                    rudp.send_stream(f, total_size=size,
                                     progress_callback=lambda s: self._prog(s, size))
                sent = size
            except ConnectionLostError as e:
                print(f"\nUDP upload error: {e}")
            except Exception as e:
                print(f"\nUDP upload error: {e}")
            self._stats("Upload", sent, time.time() - t0)
            return sent == size
        else:
            sent = self._send_tcp(path, size, offset)
            final = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=30)
            if final:
                print(f"\n{final.decode(errors='ignore').strip()}")
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
                        print("\nConnection lost during upload")
                        break
                    sent += len(chunk)
                    self._prog(sent, size)
        except IOError as e:
            print(f"\nRead error: {e}")
        return sent

    # ── download ──────────────────────────────────────────

    def download_file(self, filename: str, use_udp: bool = False) -> bool:
        return self._do_download(filename, 0, use_udp)

    def resume_download(self, filename: str, offset: int,
                        use_udp: bool = False) -> bool:
        return self._do_download(filename, offset, use_udp)

    def _do_download(self, filename: str, offset: int,
                     use_udp: bool) -> bool:
        filename = Path(filename).name
        flag = "--udp" if use_udp else "--tcp"
        cmd = (f"RESUME_DOWNLOAD {filename} {offset} {flag}" if offset
               else f"DOWNLOAD {filename} {flag}")

        if use_udp:
            resp_str = RUDPSocket(
                self.udp_socket, (self.host, self.port)
            ).send_command(cmd)
            if not resp_str:
                print("No UDP response")
                return False
        else:
            if not self.connected:
                return False
            send_all(self.tcp_socket, (cmd + "\n").encode())
            r = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=10)
            if not r:
                print("No response")
                return False
            resp_str = r.decode(errors="ignore").strip()

        if "ERROR" in resp_str:
            print(resp_str)
            return False

        fsize = self._parse_resp(resp_str)
        if fsize <= 0:
            print(f"Bad response: {resp_str}")
            return False

        proto_name = "UDP" if use_udp else "TCP"
        print(f"Downloading {filename} ({fsize} bytes) via {proto_name}...")
        self._prog_pct = -1
        t0 = time.time()

        if use_udp:
            received = self._recv_udp_download(filename, fsize, offset)
        else:
            received = self._recv_tcp(filename, fsize, offset)

        self._stats("Download", received, time.time() - t0)
        if received != fsize:
            print(f"Incomplete: {received}/{fsize}")
            return False
        return True

    def _recv_udp_download(self, filename: str, fsize: int,
                           offset: int) -> int:
        """Принимает файл по UDP через выделенный сокет."""

        # Ждём DOWNLOAD_PORT от сервера
        download_port = None
        t0 = time.monotonic()
        while time.monotonic() - t0 < 15.0:
            r, _, _ = select.select([self.udp_socket], [], [], 0.1)
            if not r:
                continue
            try:
                pkt, addr = self.udp_socket.recvfrom(65536)
            except OSError:
                continue
            if len(pkt) < UDP_HEADER_SIZE:
                continue
            _, ptype = _HDR.unpack_from(pkt)
            if ptype == PacketType.CMD.value:
                msg = pkt[UDP_HEADER_SIZE:].decode(errors="ignore")
                if "DOWNLOAD_PORT" in msg:
                    for part in msg.split():
                        try:
                            download_port = int(part)
                            break
                        except ValueError:
                            continue
                    if download_port:
                        break

        if download_port is None:
            print("\nTimeout waiting for download port")
            return 0

        print(f"  Server download port: {download_port}")

        # Выделенный сокет для приёма
        dl_sock = create_udp_socket()
        dl_sock.setblocking(False)
        dl_sock.bind(('', 0))

        server_dl_addr = (self.host, download_port)

        # Hello (несколько раз для надёжности)
        hello = _HDR.pack(0, PacketType.ACK.value)
        for _ in range(10):
            try:
                dl_sock.sendto(hello, server_dl_addr)
            except OSError:
                pass
            time.sleep(0.03)

        fp = self.download_dir / filename
        mode = "ab" if offset else "wb"
        received = 0

        try:
            rudp = RUDPSocket(dl_sock, dest_addr=server_dl_addr)
            with open(fp, mode) as f:
                received = rudp.recv_stream(
                    f, total_size=fsize,
                    progress_callback=lambda r: self._prog(r, fsize),
                )
        except ConnectionLostError as e:
            print(f"\nUDP download error: {e}")
        except Exception as e:
            print(f"\nUDP download error: {e}")
        finally:
            dl_sock.close()

        return received

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
                        print("\nConnection lost during download")
                        break
                    f.write(data)
                    received += len(data)
                    self._prog(received, size)
        except IOError as e:
            print(f"\nWrite error: {e}")
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
        if cur == total or now - self._prog_ts >= 0.15 or pct != self._prog_pct:
            print(f"\rProgress: {pct}% ({cur}/{total})", end="", flush=True)
            self._prog_ts = now
            self._prog_pct = pct

    def _stats(self, op: str, n: int, t: float) -> None:
        print()
        if t <= 0 or n <= 0:
            return
        bps = n / t
        if bps >= 1 << 20:
            print(f"{op}: {bps / (1 << 20):.2f} MB/s  ({n} bytes in {t:.3f}s)")
        elif bps >= 1024:
            print(f"{op}: {bps / 1024:.2f} KB/s  ({n} bytes in {t:.3f}s)")
        else:
            print(f"{op}: {bps:.2f} B/s  ({n} bytes in {t:.3f}s)")


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
            "ECHO <text> [--udp]",
            "TIME [--udp]",
            "UPLOAD <filepath> [--udp]",
            "DOWNLOAD <filename> [--udp]",
            "RESUME_UPLOAD <filepath> <offset> [--udp]",
            "RESUME_DOWNLOAD <filename> <offset> [--udp]",
            "RESUME  (retry last failed transfer)",
            "QUIT",
        ]:
            print(f"  {c}")

    def _process(self, cmd: str) -> bool:
        parts = cmd.split()
        command = parts[0].upper()
        use_udp = "--udp" in [p.lower() for p in parts]
        args = [p for p in parts[1:] if p.lower() not in ("--udp", "--tcp")]

        if command in ("QUIT", "EXIT", "CLOSE"):
            return False

        if command == "UPLOAD":
            if not args:
                print("Usage: UPLOAD <path> [--udp]")
                return True
            ok = self.client.upload_file(args[0], use_udp)
            self.last_up = None if ok else (args[0], 0, use_udp)
            return True

        if command == "DOWNLOAD":
            if not args:
                print("Usage: DOWNLOAD <filename> [--udp]")
                return True
            ok = self.client.download_file(args[0], use_udp)
            self.last_down = None if ok else (args[0], 0, use_udp)
            return True

        if command == "RESUME_UPLOAD":
            if len(args) < 2:
                print("Usage: RESUME_UPLOAD <filepath> <offset> [--udp]")
                return True
            try:
                offset = int(args[1])
            except ValueError:
                print("Invalid offset")
                return True
            self.client.resume_upload(args[0], offset, use_udp)
            return True

        if command == "RESUME_DOWNLOAD":
            if len(args) < 2:
                print("Usage: RESUME_DOWNLOAD <filename> <offset> [--udp]")
                return True
            try:
                offset = int(args[1])
            except ValueError:
                print("Invalid offset")
                return True
            self.client.resume_download(args[0], offset, use_udp)
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