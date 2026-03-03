"""TCP/UDP клиент."""

import socket
import time
import select
import struct
from pathlib import Path
from typing import Optional, Tuple

from common.protocol import COMMAND_TERMINATOR, BUFFER_SIZE, PacketType
from common.socket_utils import create_client_socket, recv_until, recv_exact, send_all
from common.rudp import RUDPSocket

_HDR = struct.Struct("!IB")


class FileTransferClient:
    def __init__(self, host: str, port: int):
        self.host = host; self.port = port
        self.tcp_socket: Optional[socket.socket] = None
        self.connected = False
        self.download_dir = Path("./downloads")
        self.download_dir.mkdir(exist_ok=True)
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setblocking(False)
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try: self.udp_socket.setsockopt(socket.SOL_SOCKET, opt, 8*1024*1024)
            except: pass
        self._prog_ts = 0.0; self._prog_pct = -1

    def connect(self) -> bool:
        try:
            self.tcp_socket = create_client_socket()
            self.tcp_socket.connect((self.host, self.port))
            self.connected = True
            w = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            if w: print(w.decode().strip())
            return True
        except socket.error as e:
            print(f"Connection failed: {e}"); return False

    def disconnect(self):
        if self.tcp_socket:
            try: self.send_command("QUIT")
            except: pass
            try: self.tcp_socket.close()
            except: pass
        self.connected = False
        try: self.udp_socket.close()
        except: pass

    def send_command(self, command: str, proto: str = "TCP") -> Optional[str]:
        if proto == "UDP":
            return RUDPSocket(self.udp_socket, (self.host, self.port)).send_command(command)
        if not self.connected: return None
        try:
            send_all(self.tcp_socket, (command + "\n").encode())
            r = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            return r.decode().strip() if r else None
        except socket.error as e:
            print(f"Command failed: {e}"); return None

    def upload_file(self, filepath: str, use_udp=False) -> bool:
        p = Path(filepath)
        if not p.exists(): print(f"Not found: {filepath}"); return False
        return self._do_upload(p, p.name, p.stat().st_size, 0, use_udp)

    def resume_upload(self, filepath: str, offset: int, use_udp=False) -> bool:
        p = Path(filepath)
        if not p.exists(): print(f"Not found: {filepath}"); return False
        return self._do_upload(p, p.name, p.stat().st_size - offset, offset, use_udp)

    def _do_upload(self, path, filename, size, offset, use_udp):
        flag = "--udp" if use_udp else "--tcp"
        cmd = (f"RESUME_UPLOAD {filename} {offset} {size} {flag}"
               if offset else f"UPLOAD {filename} {size} {flag}")
        if use_udp:
            resp = RUDPSocket(self.udp_socket, (self.host, self.port)).send_command(cmd)
            if not resp or "READY" not in resp:
                print(f"Not ready: {resp}"); return False

            upload_port = self._wait_upload_port()
            if not upload_port:
                print("No upload port received"); return False

            proto_name = "UDP"
            print(f"Uploading via {proto_name} (port {upload_port})...")
            t0 = time.time()
            try:
                up_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                up_sock.setblocking(False)
                for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
                    try: up_sock.setsockopt(socket.SOL_SOCKET, opt, 8*1024*1024)
                    except: pass

                rudp = RUDPSocket(up_sock, (self.host, upload_port))
                with open(path, "rb") as f:
                    f.seek(offset)
                    rudp.send_stream(f, total_size=size,
                                     progress_callback=lambda s: self._prog(s, size))
                sent = size
                up_sock.close()
            except Exception as e:
                print(f"\nUDP upload error: {e}"); sent = 0
            self._stats("Upload", sent, time.time() - t0)
            return sent == size
        else:
            if not self.connected: return False
            send_all(self.tcp_socket, (cmd + "\n").encode())
            raw = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            resp = raw.decode().strip() if raw else ""
            if not resp or not resp.startswith("READY"):
                print(f"Not ready: {resp}"); return False
            print("Uploading via TCP...")
            t0 = time.time()
            sent = self._send_tcp(path, size, offset)
            final = recv_until(self.tcp_socket, COMMAND_TERMINATOR, timeout=10)
            if final: print(final.decode().strip())
            self._stats("Upload", sent, time.time() - t0)
            return sent == size

    def _wait_upload_port(self) -> Optional[int]:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            r, _, _ = select.select([self.udp_socket], [], [], 0.1)
            if not r: continue
            try:
                pkt, addr = self.udp_socket.recvfrom(65536)
            except: continue
            if len(pkt) < _HDR.size: continue
            _, pt = _HDR.unpack_from(pkt)
            if pt == PacketType.CMD.value:
                msg = pkt[_HDR.size:].decode(errors="ignore")
                if msg.startswith("UPLOAD_PORT "):
                    try:
                        return int(msg.split()[1])
                    except: pass
                elif msg == "ACK_CMD":
                    continue
        return None

    def _send_tcp(self, path, size, offset):
        sent = 0
        try:
            with open(path, "rb") as f:
                f.seek(offset)
                while sent < size:
                    chunk = f.read(min(BUFFER_SIZE, size - sent))
                    if not chunk: break
                    if not send_all(self.tcp_socket, chunk): break
                    sent += len(chunk); self._prog(sent, size)
        except IOError as e: print(f"Read error: {e}")
        return sent

    def download_file(self, filename: str, use_udp=False) -> bool:
        return self._do_download(filename, 0, use_udp)

    def resume_download(self, filename: str, offset: int, use_udp=False) -> bool:
        return self._do_download(filename, offset, use_udp)

    def _do_download(self, filename, offset, use_udp):
        filename = Path(filename).name
        flag = "--udp" if use_udp else "--tcp"
        cmd = (f"RESUME_DOWNLOAD {filename} {offset} {flag}"
               if offset else f"DOWNLOAD {filename} {flag}")
        if use_udp:
            resp_str = RUDPSocket(self.udp_socket, (self.host, self.port)).send_command(cmd)
            if not resp_str: print("No UDP response"); return False
        else:
            if not self.connected: return False
            send_all(self.tcp_socket, (cmd + "\n").encode())
            r = recv_until(self.tcp_socket, COMMAND_TERMINATOR)
            if not r: print("No response"); return False
            resp_str = r.decode().strip()
        if "ERROR" in resp_str: print(resp_str); return False
        fsize = self._parse_resp(resp_str)
        if fsize <= 0: print(f"Bad resp: {resp_str}"); return False
        proto_name = "UDP" if use_udp else "TCP"
        print(f"Downloading via {proto_name}...")
        t0 = time.time()
        if use_udp:
            fp = self.download_dir / filename
            mode = "ab" if offset else "wb"
            try:
                rudp = RUDPSocket(self.udp_socket, dest_addr=None)
                with open(fp, mode) as f:
                    received = rudp.recv_stream(
                        f, total_size=fsize,
                        progress_callback=lambda r: self._prog(r, fsize))
            except Exception as e:
                print(f"\nUDP download error: {e}"); received = 0
        else:
            received = self._recv_tcp(filename, fsize, offset)
        self._stats("Download", received, time.time() - t0)
        if received != fsize: print("Incomplete"); return False
        return True

    def _recv_tcp(self, filename, size, offset):
        fp = self.download_dir / filename; rcv = 0
        try:
            with open(fp, "ab" if offset else "wb") as f:
                while rcv < size:
                    data = recv_exact(self.tcp_socket, min(BUFFER_SIZE, size - rcv), timeout=60)
                    if data is None: print("\nConnection lost"); break
                    f.write(data); rcv += len(data); self._prog(rcv, size)
        except IOError as e: print(f"Write error: {e}")
        return rcv

    def _parse_resp(self, r):
        for i, p in enumerate(r.split()):
            if p == "FILE" and i+1 < len(r.split()):
                try: return int(r.split()[i+1])
                except: pass
        return 0

    def _prog(self, cur, total):
        if total <= 0: return
        pct = int(cur / total * 100); now = time.time()
        if cur == total or now - self._prog_ts >= 0.1 or pct != self._prog_pct:
            print(f"\rProgress: {pct}% ({cur}/{total})", end="", flush=True)
            self._prog_ts = now; self._prog_pct = pct

    def _stats(self, op, n, t):
        print()
        if t > 0:
            bps = n / t
            if bps >= 1<<20: print(f"{op}: {bps/(1<<20):.2f} MB/s")
            elif bps >= 1024: print(f"{op}: {bps/1024:.2f} KB/s")
            else: print(f"{op}: {bps:.2f} B/s")


class InteractiveClient:
    def __init__(self, host: str, port: int):
        self.client = FileTransferClient(host, port)
        self.last_up: Optional[Tuple[str, int, bool]] = None
        self.last_down: Optional[Tuple[str, int, bool]] = None

    def run(self):
        if not self.client.connect(): print("Cannot connect."); return
        self._help()
        try:
            while True:
                try: cmd = input("\n> ").strip()
                except EOFError: break
                if not cmd: continue
                if not self._process(cmd): break
        except KeyboardInterrupt: print("\nInterrupted")
        finally: self.client.disconnect()

    def _help(self):
        print("\nCommands:")
        for c in ["ECHO [--udp]", "TIME [--udp]", "UPLOAD [--udp]",
                   "DOWNLOAD [--udp]", "RESUME", "QUIT"]:
            print(f"  {c}")

    def _process(self, cmd):
        parts = cmd.split(); command = parts[0].upper()
        use_udp = "--udp" in [p.lower() for p in parts]
        args = " ".join(p for p in parts[1:] if p.lower() != "--udp")
        if command in ("QUIT","EXIT","CLOSE"): return False
        if command == "UPLOAD":
            if not args: print("Usage: UPLOAD [--udp]"); return True
            ok = self.client.upload_file(args, use_udp)
            self.last_up = None if ok else (args, 0, use_udp); return True
        if command == "DOWNLOAD":
            if not args: print("Usage: DOWNLOAD [--udp]"); return True
            ok = self.client.download_file(args, use_udp)
            self.last_down = None if ok else (args, 0, use_udp); return True
        if command == "RESUME":
            if self.last_up: self.client.resume_upload(*self.last_up)
            elif self.last_down: self.client.resume_download(*self.last_down)
            else: print("Nothing to resume")
            return True
        resp = self.client.send_command(cmd, "UDP" if use_udp else "TCP")
        if resp: print(resp)
        return True
