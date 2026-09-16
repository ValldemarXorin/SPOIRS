"""Обработчики команд сервера."""

import socket
import struct
import threading
import time
from datetime import datetime

from lab1.common.protocol import (
    Command, CommandType, Response, PacketType, format_response
)
from lab1.common.socket_utils import send_all
from lab1.server.file_manager import FileManager
from lab1.common.rudp import RudpSocket, create_rudp_socket

_HDR = struct.Struct("!IB")

# Глобальный реестр RUDP сокетов для UDP файловых передач
# cid -> RudpSocket
_rudp_sessions: dict = {}
_rudp_lock = threading.Lock()


class CommandHandler:
    def __init__(self, file_manager: FileManager, udp_socket: socket.socket):
        self.fm = file_manager
        self.udp_socket = udp_socket
        self.handlers = {
            CommandType.ECHO: self._echo,
            CommandType.TIME: self._time,
            CommandType.UPLOAD: self._upload,
            CommandType.DOWNLOAD: self._download,
            CommandType.RESUME_UPLOAD: self._resume_upload,
            CommandType.RESUME_DOWNLOAD: self._resume_download,
        }

    def execute(self, cmd, tcp_sock, udp_sock, udp_addr):
        h = self.handlers.get(cmd.type)
        return h(cmd, tcp_sock, udp_sock, udp_addr) if h else Response(False, f"Unknown: {cmd.raw.strip()}")

    def _echo(self, cmd, *_):
        return Response(True, cmd.args[0] if cmd.args else "")

    def _time(self, cmd, *_):
        return Response(True, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def _upload(self, cmd, tcp_sock, udp_sock, udp_addr):
        if len(cmd.args) < 2:
            return Response(False, "Usage: UPLOAD <filename> <size>")
        try:
            size = int(cmd.args[1])
        except ValueError:
            return Response(False, "Invalid size")
        return self._init_upload(cmd.protocol, tcp_sock, udp_sock, udp_addr,
                                 cmd.args[0], size, 0)

    def _resume_upload(self, cmd, tcp_sock, udp_sock, udp_addr):
        if len(cmd.args) < 3:
            return Response(False, "Usage: RESUME_UPLOAD <f> <off> <size>")
        try:
            offset, size = int(cmd.args[1]), int(cmd.args[2])
        except ValueError:
            return Response(False, "Invalid args")
        return self._init_upload(cmd.protocol, tcp_sock, udp_sock, udp_addr,
                                 cmd.args[0], size, offset)

    def _init_upload(self, proto, tcp_sock, udp_sock, udp_addr, filename, size, offset):
        if proto == "UDP" and udp_addr is not None:
            cid = f"{udp_addr[0]}:{udp_addr[1]}"
        elif proto == "UDP":
            cid = f"udp_unknown_{time.time()}"
        else:
            cid = str(tcp_sock.fileno()) if tcp_sock else f"tcp_unknown_{time.time()}"

        self.fm.close_session(cid)
        sess = self.fm.create_session(filename, size, cid, is_upload=True, sock=tcp_sock)
        if not sess:
            return Response(False, "Cannot create session")
        if offset > 0 and sess.file_handle:
            sess.transferred = offset
            sess.file_handle.seek(offset)

        if proto == "TCP" and tcp_sock is not None:
            send_all(tcp_sock, b"READY\n")
            return Response(True, "READY")

        # UDP: создаём RUDP сокет для этого клиента
        rudp_sock = self._get_or_create_rudp(cid, udp_addr)
        if rudp_sock is None:
            return Response(False, "Failed to create RUDP session")

        # Запускаем приём файла в фоне
        threading.Thread(
            target=self._rudp_receive_file,
            args=(rudp_sock, cid, sess),
            daemon=True
        ).start()

        return Response(True, "READY")

    def _download(self, cmd, tcp_sock, udp_sock, udp_addr):
        if not cmd.args:
            return Response(False, "Usage: DOWNLOAD <filename>")
        return self._init_download(cmd.protocol, tcp_sock, udp_sock, udp_addr, cmd.args[0], 0)

    def _resume_download(self, cmd, tcp_sock, udp_sock, udp_addr):
        if len(cmd.args) < 2:
            return Response(False, "Usage: RESUME_DOWNLOAD <f> <off>")
        try:
            offset = int(cmd.args[1])
        except ValueError:
            return Response(False, "Invalid offset")
        return self._init_download(cmd.protocol, tcp_sock, udp_sock, udp_addr, cmd.args[0], offset)

    def _init_download(self, proto, tcp_sock, udp_sock, udp_addr, filename, offset):
        if not self.fm.file_exists(filename):
            return Response(False, "File not found")
        fsize = self.fm.get_file_size(filename)
        remaining = fsize - offset

        if proto == "UDP" and udp_addr is not None:
            cid = f"{udp_addr[0]}:{udp_addr[1]}"
        elif proto == "UDP":
            cid = f"udp_unknown_{time.time()}"
        else:
            cid = str(tcp_sock.fileno()) if tcp_sock else f"tcp_unknown_{time.time()}"

        self.fm.close_session(cid)
        sess = self.fm.create_session(filename, remaining, cid, is_upload=False, sock=tcp_sock)
        if not sess:
            return Response(False, "Cannot open file")
        if offset > 0 and sess.file_handle:
            sess.file_handle.seek(offset)
        if proto == "UDP" and udp_addr is not None:
            sess.udp_client_addr = udp_addr

        info = f"FILE {remaining}"
        if proto == "TCP" and tcp_sock is not None:
            send_all(tcp_sock, f"{info}\n".encode())
            return Response(True, info)

        # UDP: создаём RUDP сокет и отправляем файл
        rudp_sock = self._get_or_create_rudp(cid, udp_addr)
        if rudp_sock is None:
            return Response(False, "Failed to create RUDP session")

        threading.Thread(
            target=self._rudp_send_file,
            args=(rudp_sock, cid, sess),
            daemon=True
        ).start()

        return Response(True, info)

    def _get_or_create_rudp(self, cid: str, udp_addr) -> RudpSocket:
        """Получает или создаёт RUDP сокет для данного клиента."""
        with _rudp_lock:
            if cid in _rudp_sessions:
                rudp = _rudp_sessions[cid]
                rudp.set_peer_filter(udp_addr)
                return rudp

            # Создаём новый RUDP на том же UDP сокете
            rudp = RudpSocket(self.udp_socket, dest_addr=udp_addr)
            rudp.set_peer_filter(udp_addr)
            rudp.set_connection_lost_callback(lambda: self._on_rudp_closed(cid))
            _rudp_sessions[cid] = rudp
            return rudp

    def _on_rudp_closed(self, cid: str):
        with _rudp_lock:
            _rudp_sessions.pop(cid, None)
        self.fm.close_session(cid)

    def _rudp_receive_file(self, rudp: RudpSocket, cid: str, session):
        """Приём файла через RUDP (upload от клиента)."""
        try:
            def writer(data: bytes):
                if session.file_handle:
                    session.file_handle.write(data)
                session.transferred += len(data)
                session.last_activity = time.time()

            rudp.recv_stream(writer, session.total_size, 
                           lambda x: self._log_progress(cid, session, "UDP Upload"))

            # Файл принят полностью
            if session.file_handle:
                session.file_handle.close()
                session.file_handle = None
            temp_path = session.temp_path
            if temp_path:
                import os
                final_path = self.fm.get_file_path(session.filename)
                if os.path.exists(final_path):
                    os.remove(final_path)
                os.rename(temp_path, final_path)

            br = self.fm.calculate_bitrate(session)
            bs = self.fm.format_bitrate(br)
            self.fm.complete_session(cid)
            print(f"[RUDP] Upload done: {session.filename} ({bs})")

        except Exception as e:
            print(f"[RUDP] Upload error [{cid}]: {e}")
            self.fm.close_session(cid)
        finally:
            self._cleanup_rudp(cid)

    def _rudp_send_file(self, rudp: RudpSocket, cid: str, session):
        """Отправка файла через RUDP (download к клиенту)."""
        try:
            def reader(size: int):
                if session.file_handle:
                    return session.file_handle.read(size)
                return b""

            rudp.send_stream(reader, session.total_size,
                           lambda x: self._log_progress(cid, session, "UDP Download"))

            if session.file_handle:
                session.file_handle.close()
                session.file_handle = None

            br = self.fm.calculate_bitrate(session)
            bs = self.fm.format_bitrate(br)
            self.fm.complete_session(cid)
            print(f"[RUDP] Download done: {session.filename} ({bs})")

        except Exception as e:
            print(f"[RUDP] Download error [{cid}]: {e}")
            self.fm.close_session(cid)
        finally:
            self._cleanup_rudp(cid)

    def _cleanup_rudp(self, cid: str):
        with _rudp_lock:
            rudp = _rudp_sessions.pop(cid, None)
            if rudp:
                rudp.close()

    def _log_progress(self, cid, session, op):
        if session.total_size <= 0:
            return
        pct = int(session.transferred / session.total_size * 100)
        if pct >= getattr(session, "_last_pct", -10) + 10 or pct == 100:
            session._last_pct = pct
            from datetime import datetime
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] {op}: {session.filename} [{cid}] — {pct}%")