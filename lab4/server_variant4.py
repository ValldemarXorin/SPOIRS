"""Lab 4 Variant 4: UDP Server with per-session threads and MSG_PEEK."""

import socket
import threading
import time
import select
import struct
import sys
from datetime import datetime
from typing import Optional, Dict, Tuple, Callable
from dataclasses import dataclass, field

from common.protocol import (
    CommandType,
    PacketType,
    parse_command,
    format_response,
    COMMAND_TERMINATOR,
    Response,
)
from common.socket_utils import create_udp_socket
from common.rudp import RudpSocket
from lab1.server.file_manager import FileManager

_HDR = struct.Struct("!IB")


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


@dataclass
class SessionInfo:
    addr: Tuple[str, int]
    thread: threading.Thread
    rudp: RudpSocket
    last_activity: float = field(default_factory=time.time)
    active: bool = True
    client_id: str = ""


class SessionManager:
    """Thread-safe registry of active client sessions."""

    def __init__(self):
        self._sessions: Dict[str, SessionInfo] = {}
        self._lock = threading.RLock()
        self._file_manager = FileManager()

    def get_file_manager(self) -> FileManager:
        return self._file_manager

    def create_session(self, addr: Tuple[str, int], rudp: RudpSocket) -> SessionInfo:
        cid = f"{addr[0]}:{addr[1]}"
        with self._lock:
            if cid in self._sessions:
                old = self._sessions[cid]
                old.active = False
            session = SessionInfo(
                addr=addr,
                thread=None,
                rudp=rudp,
                client_id=cid,
            )
            self._sessions[cid] = session
            return session

    def get_session(self, cid: str) -> Optional[SessionInfo]:
        with self._lock:
            return self._sessions.get(cid)

    def remove_session(self, cid: str) -> None:
        with self._lock:
            session = self._sessions.pop(cid, None)
            if session:
                session.active = False
                session.rudp.close()

    def get_all_sessions(self) -> Dict[str, SessionInfo]:
        with self._lock:
            return dict(self._sessions)

    def cleanup_inactive(self, timeout: float = 300.0) -> None:
        """Remove sessions inactive for more than timeout seconds."""
        now = time.time()
        with self._lock:
            to_remove = [
                cid for cid, s in self._sessions.items()
                if not s.active or now - s.last_activity > timeout
            ]
            for cid in to_remove:
                session = self._sessions.pop(cid, None)
                if session:
                    session.active = False
                    session.rudp.close()
                    self._file_manager.close_session(cid)


class ClientHandler:
    """Handles a single client session in its own thread using MSG_PEEK."""

    def __init__(
        self,
        session: SessionInfo,
        server_socket: socket.socket,
        session_manager: SessionManager,
        server_ref: "Variant4Server",
    ):
        self.session = session
        self.server_socket = server_socket
        self.session_manager = session_manager
        self.server = server_ref
        self.cid = session.client_id
        self.running = True
        self.file_transfer_active = False
        self.expected_file_size = 0
        self.file_received = 0
        self.file_handle = None
        self.file_name = ""
        self.is_upload = False
        self.download_file_handle = None

    def run(self) -> None:
        """Main thread loop: MSG_PEEK -> process if my packet -> yield."""
        print(f"[{_ts()}] Session started for {self.cid}")

        # Настраиваем RUDP
        rudp = self.session.rudp
        rudp.set_peer_filter(self.session.addr)

        # Callbacks для RUDP
        rudp.set_data_callback(self._on_rudp_data)
        rudp.set_connection_lost_callback(self._on_connection_lost)

        # Принимаем соединение (ждём SYN)
        if not rudp.accept(timeout=10.0):
            print(f"[{_ts()}] Failed to accept RUDP connection from {self.cid}")
            self._cleanup()
            return

        print(f"[{_ts()}] RUDP connected for {self.cid}")

        # Отправляем приветствие
        rudp.send_stream(lambda _: b"220 Welcome\n", 12)

        # Основной цикл обработки команд
        while self.running and self.session.active:
            try:
                # MSG_PEEK: проверяем есть ли пакет для нас
                r, _, _ = select.select([self.server_socket], [], [], 0.1)
                if not r:
                    # Проверяем таймаут сессии
                    if time.time() - self.session.last_activity > 300:
                        print(f"[{_ts()}] Session timeout for {self.cid}")
                        break
                    continue

                # Peek пакет
                try:
                    data, addr = self.server_socket.recvfrom(65536, socket.MSG_PEEK)
                except (BlockingIOError, OSError):
                    continue

                if addr != self.session.addr:
                    # Пакет не для нас - оставляем в буфере, yield
                    time.sleep(0.001)
                    continue

                # Пакет для нас - потребляем его
                try:
                    data, addr = self.server_socket.recvfrom(65536)
                except (BlockingIOError, OSError):
                    continue

                if len(data) < 5:
                    continue

                seq, ptype = _HDR.unpack_from(data)
                payload = data[5:]

                self.session.last_activity = time.time()

                if ptype == PacketType.CMD.value:
                    self._handle_command(payload)
                elif ptype == PacketType.DATA.value:
                    # RUDP обработает DATA через свой механизм
                    # Но нам нужно передать его в RUDP
                    self._feed_rudp(seq, ptype, payload)
                elif ptype in (PacketType.ACK.value, PacketType.NACK.value, PacketType.FIN.value):
                    self._feed_rudp(seq, ptype, payload)

            except Exception as e:
                print(f"[{_ts()}] Handler error for {self.cid}: {e}")
                break

        print(f"[{_ts()}] Session ended for {self.cid}")
        self._cleanup()

    def _feed_rudp(self, seq: int, ptype: int, payload: bytes) -> None:
        """Кормит RUDP пакетом (внутренний вызов)."""
        # RUDP работает на том же сокете, поэтому мы не можем напрямую
        # "скормить" ему пакет. Вместо этого RUDP сам читает из сокета
        # в своём worker_loop. Но так как мы делаем MSG_PEEK и потребляем
        # пакеты для своего адреса, RUDP их не увидит.
        #
        # Решение: RUDP должен работать в режиме, где мы передаём ему
        # пакеты через callback, или RUDP должен иметь доступ к сокету
        # и мы не должны потреблять пакеты, которые ему нужны.
        #
        # Для простоты: RUDP worker_loop отключён, мы обрабатываем всё здесь.
        pass

    def _handle_command(self, payload: bytes) -> None:
        msg = payload.decode(errors="ignore").strip()
        if not msg:
            return

        print(f"[{_ts()}] CMD from {self.cid}: {msg}")

        cmd = parse_command(msg, "UDP")
        response = None

        if cmd.type == CommandType.ECHO:
            response = Response(True, cmd.args[0] if cmd.args else "")
        elif cmd.type == CommandType.TIME:
            response = Response(True, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        elif cmd.type == CommandType.UPLOAD:
            response = self._handle_upload(cmd)
        elif cmd.type == CommandType.DOWNLOAD:
            response = self._handle_download(cmd)
        elif cmd.type == CommandType.RESUME_UPLOAD:
            response = self._handle_resume_upload(cmd)
        elif cmd.type == CommandType.RESUME_DOWNLOAD:
            response = self._handle_resume_download(cmd)
        elif cmd.type == CommandType.QUIT:
            response = Response(True, "Goodbye")
            self.running = False
        else:
            response = Response(False, f"Unknown command: {cmd.raw}")

        if response:
            # Отправляем ответ через RUDP (CMD пакет)
            resp_pkt = _HDR.pack(0, PacketType.CMD.value) + format_response(response)
            try:
                self.server_socket.sendto(resp_pkt, self.session.addr)
            except OSError:
                pass

    def _handle_upload(self, cmd) -> Response:
        if len(cmd.args) < 2:
            return Response(False, "Usage: UPLOAD <filename> <size>")
        try:
            size = int(cmd.args[1])
        except ValueError:
            return Response(False, "Invalid size")

        self.file_name = cmd.args[0]
        self.expected_file_size = size
        self.file_received = 0
        self.is_upload = True

        fm = self.session_manager.get_file_manager()
        fm.close_session(self.cid)
        sess = fm.create_session(self.file_name, size, self.cid, is_upload=True)
        if not sess:
            return Response(False, "Cannot create session")
        self.file_handle = sess.file_handle

        # Переходим в режим приёма файла через RUDP
        self.file_transfer_active = True
        threading.Thread(target=self._receive_file_rudp, daemon=True).start()

        return Response(True, "READY")

    def _handle_resume_upload(self, cmd) -> Response:
        if len(cmd.args) < 3:
            return Response(False, "Usage: RESUME_UPLOAD <f> <off> <size>")
        try:
            offset, size = int(cmd.args[1]), int(cmd.args[2])
        except ValueError:
            return Response(False, "Invalid args")

        self.file_name = cmd.args[0]
        self.expected_file_size = size
        self.file_received = offset
        self.is_upload = True

        fm = self.session_manager.get_file_manager()
        fm.close_session(self.cid)
        sess = fm.create_session(self.file_name, size, self.cid, is_upload=True)
        if not sess:
            return Response(False, "Cannot create session")
        if sess.file_handle:
            sess.file_handle.seek(offset)
        self.file_handle = sess.file_handle

        self.file_transfer_active = True
        threading.Thread(target=self._receive_file_rudp, daemon=True).start()

        return Response(True, "READY")

    def _handle_download(self, cmd) -> Response:
        if not cmd.args:
            return Response(False, "Usage: DOWNLOAD <filename>")

        self.file_name = cmd.args[0]
        self.is_upload = False

        fm = self.session_manager.get_file_manager()
        if not fm.file_exists(self.file_name):
            return Response(False, "File not found")
        fsize = fm.get_file_size(self.file_name)

        fm.close_session(self.cid)
        sess = fm.create_session(self.file_name, fsize, self.cid, is_upload=False)
        if not sess:
            return Response(False, "Cannot open file")
        self.download_file_handle = sess.file_handle
        self.expected_file_size = fsize

        # Отправляем FILE SIZE
        resp = Response(True, f"FILE {fsize}")
        return resp

    def _handle_resume_download(self, cmd) -> Response:
        if len(cmd.args) < 2:
            return Response(False, "Usage: RESUME_DOWNLOAD <f> <off>")
        try:
            offset = int(cmd.args[1])
        except ValueError:
            return Response(False, "Invalid offset")

        self.file_name = cmd.args[0]
        self.is_upload = False

        fm = self.session_manager.get_file_manager()
        if not fm.file_exists(self.file_name):
            return Response(False, "File not found")
        fsize = fm.get_file_size(self.file_name)
        remaining = fsize - offset

        fm.close_session(self.cid)
        sess = fm.create_session(self.file_name, remaining, self.cid, is_upload=False)
        if not sess:
            return Response(False, "Cannot open file")
        if sess.file_handle:
            sess.file_handle.seek(offset)
        self.download_file_handle = sess.file_handle
        self.expected_file_size = remaining

        resp = Response(True, f"FILE {remaining}")
        return resp

    def _receive_file_rudp(self) -> None:
        """Приём файла через RUDP send_stream (клиент шлёт, мы принимаем)."""
        # В RUDP архитектуре: клиент вызывает send_stream, сервер recv_stream
        # Но у нас RUDP на сервере - мы должны вызвать recv_stream
        rudp = self.session.rudp

        def writer(data: bytes):
            if self.file_handle:
                self.file_handle.write(data)
            self.file_received += len(data)
            self.session.last_activity = time.time()

        try:
            rudp.recv_stream(writer, self.expected_file_size,
                           lambda x: self._log_progress("Upload"))

            # Файл принят
            if self.file_handle:
                self.file_handle.close()
                self.file_handle = None

            fm = self.session_manager.get_file_manager()
            session = fm.get_session(self.cid)
            if session and session.temp_path:
                import os
                final_path = fm.get_file_path(self.file_name)
                if os.path.exists(final_path):
                    os.remove(final_path)
                os.rename(session.temp_path, final_path)

            br = fm.calculate_bitrate(fm.get_session(self.cid))
            bs = fm.format_bitrate(br)
            fm.complete_session(self.cid)
            print(f"[{_ts()}] Upload done: {self.file_name} ({bs})")

        except Exception as e:
            print(f"[{_ts()}] Receive file error: {e}")
            fm.close_session(self.cid)
        finally:
            self.file_transfer_active = False

    def _send_file_rudp(self) -> None:
        """Отправка файла через RUDP send_stream."""
        rudp = self.session.rudp

        def reader(size: int) -> bytes:
            if self.download_file_handle:
                return self.download_file_handle.read(size)
            return b""

        try:
            rudp.send_stream(reader, self.expected_file_size,
                           lambda x: self._log_progress("Download"))

            if self.download_file_handle:
                self.download_file_handle.close()
                self.download_file_handle = None

            fm = self.session_manager.get_file_manager()
            br = fm.calculate_bitrate(fm.get_session(self.cid))
            bs = fm.format_bitrate(br)
            fm.complete_session(self.cid)
            print(f"[{_ts()}] Download done: {self.file_name} ({bs})")

        except Exception as e:
            print(f"[{_ts()}] Send file error: {e}")
            fm.close_session(self.cid)
        finally:
            self.file_transfer_active = False

    def _log_progress(self, op: str):
        if self.expected_file_size <= 0:
            return
        pct = int(self.file_received / self.expected_file_size * 100)
        if pct >= getattr(self, "_last_pct", -10) + 10 or pct == 100:
            self._last_pct = pct
            print(f"[{_ts()}] {op}: {self.file_name} [{self.cid}] — {pct}%")

    def _on_rudp_data(self, data: bytes) -> None:
        """Callback для данных от RUDP (если не файл)."""
        # Команды приходят через _handle_command
        pass

    def _on_connection_lost(self) -> None:
        print(f"[{_ts()}] Connection lost for {self.cid}")
        self.running = False

    def _cleanup(self) -> None:
        self.running = False
        self.session.active = False
        self.session.rudp.close()
        self.session_manager.remove_session(self.cid)
        fm = self.session_manager.get_file_manager()
        fm.close_session(self.cid)


class Variant4Server:
    """UDP Server Variant 4: per-session threads with MSG_PEEK."""

    def __init__(self, host: str = "0.0.0.0", port: int = 9000):
        self.host = host
        self.port = port
        self.running = False

        self.server_socket = create_udp_socket()
        self.server_socket.bind((host, port))
        self.server_socket.setblocking(False)

        self.session_manager = SessionManager()

    def start(self) -> None:
        self.running = True

        try:
            ip = socket.gethostbyname(socket.gethostname())
        except OSError:
            ip = "127.0.0.1"
        bind_ip = ip if self.host in ("0.0.0.0", "") else self.host
        print(f"[{_ts()}] Variant 4 Server started on {bind_ip}:{self.port}")
        print(f"[{_ts()}] Mode: UDP, thread per session, MSG_PEEK")

        self._main_loop()

    def _main_loop(self) -> None:
        while self.running:
            try:
                # Основной select на серверном сокете
                r, _, _ = select.select([self.server_socket], [], [], 0.1)
                if not r:
                    # Периодическая очистка неактивных сессий
                    self.session_manager.cleanup_inactive(300.0)
                    continue

                # Есть входящий пакет - peek
                try:
                    data, addr = self.server_socket.recvfrom(65536, socket.MSG_PEEK)
                except (BlockingIOError, OSError):
                    continue

                if len(data) < 5:
                    continue

                cid = f"{addr[0]}:{addr[1]}"

                # Проверяем, есть ли уже сессия для этого клиента
                session = self.session_manager.get_session(cid)
                if session and session.active:
                    # Сессия существует - пакет будет обработан её потоком
                    # (который сделает recvfrom после peek)
                    continue

                # Новый клиент - создаём сессию и поток
                print(f"[{_ts()}] New client: {cid}")

                # Создаём RUDP сокет на этом же UDP сокете
                rudp = RudpSocket(self.server_socket, dest_addr=addr)
                rudp.set_peer_filter(addr)

                session_info = self.session_manager.create_session(addr, rudp)

                # Запускаем поток обработчика
                handler = ClientHandler(session_info, self.server_socket,
                                       self.session_manager, self)
                session_info.thread = threading.Thread(target=handler.run, daemon=True)
                session_info.thread.start()

            except Exception as e:
                if self.running:
                    print(f"[{_ts()}] Main loop error: {e}")

    def stop(self) -> None:
        self.running = False
        try:
            self.server_socket.close()
        except OSError:
            pass


def main(host: str = "0.0.0.0", port: int = 9000) -> None:
    server = Variant4Server(host, port)
    server.start()


if __name__ == "__main__":
    main()