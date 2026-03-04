"""Обработчики команд сервера."""

import struct
from datetime import datetime
import time  # добавлено для time.time() в случае неизвестного адреса

from common.protocol import (
    Command, CommandType, Response, PacketType, format_response
)
from common.socket_utils import send_all
from server.file_manager import FileManager

_HDR = struct.Struct("!IB")


class CommandHandler:
    def __init__(self, file_manager: FileManager):
        self.fm = file_manager
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
        # Безопасное создание client_id для UDP
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
        # Для TCP сразу отправляем READY, для UDP ответ придёт позже с портом
        if proto == "TCP" and tcp_sock is not None:
            send_all(tcp_sock, b"READY\n")
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