"""
Сервер на ЧИСТОМ мультиплексировании (select).
Ни одного потока, ни одного процесса.
Вся работа — в одном цикле select.
"""

import os
import socket
import select
import signal
import struct
import sys
import time
from datetime import datetime
from typing import Optional, Dict, List, Tuple

from common.protocol import (
    CommandType,
    PacketType,
    parse_command,
    format_response,
    COMMAND_TERMINATOR,
    UDP_WINDOW_SIZE,
    Response,
    UDP_ACK_INTERVAL,
)
from common.socket_utils import create_server_socket, send_all, create_udp_socket
from server.command_handler import CommandHandler
from server.file_manager import FileManager, TransferSession

TCP_CHUNK = 64 * 1024
_HDR = struct.Struct("!IB")


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


class SelectServer:
    """Однопоточный сервер на select — чистое мультиплексирование."""

    def __init__(self, host: str = "0.0.0.0", port: int = 9000) -> None:
        self.host = host
        self.port = port
        self.running = False

        self.server_socket_tcp = create_server_socket(host, port)
        self.server_socket_tcp.setblocking(False)

        self.server_socket_udp = create_udp_socket()
        self.server_socket_udp.bind((host, port))
        self.server_socket_udp.setblocking(False)

        self.file_manager = FileManager()
        self.command_handler = CommandHandler(self.file_manager, self.server_socket_udp)

        self.inputs: List[socket.socket] = [self.server_socket_tcp, self.server_socket_udp]
        self.outputs: List[socket.socket] = []
        self.tcp_buffers: Dict[int, bytes] = {}

        # Для UDP download через select (без потоков!)
        # Ключ: cid, значение: (listen_sock, client_sock, session)
        self.udp_transfer_listeners: Dict[str, socket.socket] = {}
        self.udp_transfer_connections: Dict[str, Tuple[socket.socket, TransferSession]] = {}

    def start(self) -> None:
        self._setup_signals()
        self.running = True

        try:
            ip = socket.gethostbyname(socket.gethostname())
        except OSError:
            ip = "127.0.0.1"
        bind_ip = ip if self.host in ("0.0.0.0", "") else self.host
        print(f"[{_ts()}] SELECT Server started on {bind_ip}:{self.port}")
        print(f"[{_ts()}] Mode: pure select multiplexing (single thread, single process)")
        self._loop()

    def _setup_signals(self) -> None:
        signal.signal(signal.SIGINT, lambda *_: self._shutdown())
        signal.signal(signal.SIGTERM, lambda *_: self._shutdown())

    def _shutdown(self) -> None:
        self.running = False
        self.stop()
        sys.exit(0)

    def stop(self) -> None:
        self.running = False
        for s in list(self.inputs):
            try:
                s.close()
            except OSError:
                pass
        for cid, sock in list(self.udp_transfer_listeners.items()):
            try:
                sock.close()
            except OSError:
                pass
        for cid, (sock, _) in list(self.udp_transfer_connections.items()):
            try:
                sock.close()
            except OSError:
                pass

    # ── main loop ─────────────────────────────────────────

    def _loop(self) -> None:
        while self.running:
            try:
                # Собираем все сокеты для select
                all_inputs = list(self.inputs)
                # Добавляем listener-ы для UDP transfer
                for sock in self.udp_transfer_listeners.values():
                    if sock not in all_inputs:
                        all_inputs.append(sock)

                # outputs — сокеты, в которые нужно писать (TCP download + UDP download через TCP)
                self.outputs = []
                for s in self.file_manager.sessions.values():
                    if not s.is_upload and s.sock is not None and s.sock in self.inputs:
                        self.outputs.append(s.sock)

                # UDP download connections — тоже в outputs
                for cid, (sock, sess) in list(self.udp_transfer_connections.items()):
                    if sock.fileno() != -1:
                        self.outputs.append(sock)

                readable, writable, exceptional = select.select(
                    all_inputs, self.outputs, all_inputs, 0.005
                )

                for s in readable:
                    if s is self.server_socket_tcp:
                        self._tcp_accept()
                    elif s is self.server_socket_udp:
                        self._udp_read()
                    elif s in self.udp_transfer_listeners.values():
                        self._udp_transfer_accept(s)
                    else:
                        self._tcp_read(s)

                for s in writable:
                    # Проверяем, это UDP transfer connection?
                    handled = False
                    for cid, (sock, sess) in list(self.udp_transfer_connections.items()):
                        if s is sock:
                            self._udp_transfer_write(cid, sock, sess)
                            handled = True
                            break
                    if not handled:
                        self._tcp_write(s)

                for s in exceptional:
                    self._remove(s)

                self._udp_send_acks()
                self._send_port_notifications()

            except Exception as e:
                if self.running:
                    print(f"[{_ts()}] Loop error: {e}")

    # ── TCP ───────────────────────────────────────────────

    def _tcp_accept(self) -> None:
        try:
            cs, addr = self.server_socket_tcp.accept()
        except OSError:
            return
        print(f"[{_ts()}] TCP Connect: {addr[0]}:{addr[1]}")
        cs.setblocking(False)
        self.inputs.append(cs)
        self.tcp_buffers[cs.fileno()] = b""
        send_all(cs, b"220 Welcome\n")

    def _tcp_read(self, sock: socket.socket) -> None:
        cid = str(sock.fileno())
        session = self.file_manager.get_session(cid)

        if session and session.is_upload:
            try:
                chunk = sock.recv(TCP_CHUNK)
            except OSError:
                self._remove(sock)
                return
            if not chunk:
                self._remove(sock)
                return
            try:
                session.file_handle.write(chunk)
            except (IOError, OSError):
                self._remove(sock)
                return
            session.transferred += len(chunk)
            session.last_activity = time.time()
            self._log(cid, session, "TCP Upload")
            if session.transferred >= session.total_size:
                br = self.file_manager.calculate_bitrate(session)
                bs = self.file_manager.format_bitrate(br)
                self.file_manager.complete_session(cid)
                send_all(
                    sock,
                    format_response(
                        Response(True, f"Received {session.transferred} bytes. {bs}")
                    ),
                )
                print(f"[{_ts()}] TCP Upload done ({bs})")
            return

        try:
            data = sock.recv(4096)
        except OSError:
            self._remove(sock)
            return
        if not data:
            self._remove(sock)
            return

        fd = sock.fileno()
        buf = self.tcp_buffers.get(fd, b"") + data
        while COMMAND_TERMINATOR in buf:
            line, buf = buf.split(COMMAND_TERMINATOR, 1)
            raw = line.decode("utf-8", errors="ignore")
            cmd = parse_command(raw, "TCP")
            print(f"[{_ts()}] TCP CMD [{cid}]: {raw.strip()}")
            resp = self.command_handler.execute(cmd, sock, None, None)
            transfer_cmds = {
                CommandType.UPLOAD,
                CommandType.DOWNLOAD,
                CommandType.RESUME_UPLOAD,
                CommandType.RESUME_DOWNLOAD,
            }
            if cmd.type not in transfer_cmds or not resp.success:
                send_all(sock, format_response(resp))
        self.tcp_buffers[fd] = buf

    def _tcp_write(self, sock: socket.socket) -> None:
        cid = str(sock.fileno())
        session = self.file_manager.get_session(cid)
        if not (session and not session.is_upload and session.file_handle):
            return
        try:
            chunk = session.file_handle.read(TCP_CHUNK)
        except OSError:
            self._remove(sock)
            return
        if chunk:
            try:
                sent = sock.send(chunk)
                if sent < len(chunk):
                    session.file_handle.seek(-(len(chunk) - sent), 1)
                session.transferred += sent
            except OSError:
                self._remove(sock)
                return
            session.last_activity = time.time()
            self._log(cid, session, "TCP Download")
        else:
            br = self.file_manager.calculate_bitrate(session)
            bs = self.file_manager.format_bitrate(br)
            self.file_manager.complete_session(cid)
            print(f"[{_ts()}] TCP Download done ({bs})")

    # ── UDP ───────────────────────────────────────────────

    def _udp_read(self) -> None:
        for _ in range(16384):
            try:
                pkt, addr = self.server_socket_udp.recvfrom(65536)
            except (BlockingIOError, OSError):
                break
            if len(pkt) < 5:
                continue
            seq, ptype = _HDR.unpack_from(pkt)
            data = pkt[5:]
            cid = f"{addr[0]}:{addr[1]}"

            if ptype == PacketType.CMD.value:
                self._udp_cmd(cid, seq, data, addr)
            elif ptype == PacketType.DATA.value:
                self._udp_data(cid, seq, data, addr)
            elif ptype == PacketType.FIN.value:
                self._udp_fin(cid, seq, addr)

    def _udp_cmd(self, cid, seq, data, addr) -> None:
        ack = _HDR.pack(seq, PacketType.CMD.value) + b"ACK_CMD"
        try:
            self.server_socket_udp.sendto(ack, addr)
        except OSError:
            pass

        msg = data.decode(errors="ignore")
        if msg.startswith("OK") or msg.startswith("ERROR") or msg == "ACK_CMD":
            return

        cmd = parse_command(msg, "UDP")
        print(f"[{_ts()}] UDP CMD [{cid}]: {msg.strip()}")
        resp = self.command_handler.execute(cmd, None, self.server_socket_udp, addr)

        transfer_cmds = {
            CommandType.UPLOAD,
            CommandType.DOWNLOAD,
            CommandType.RESUME_UPLOAD,
            CommandType.RESUME_DOWNLOAD,
        }

        if cmd.type in transfer_cmds:
            if not resp.success:
                ep = _HDR.pack(0, PacketType.CMD.value) + format_response(resp)
                try:
                    self.server_socket_udp.sendto(ep, addr)
                except OSError:
                    pass
            else:
                if cmd.type in (CommandType.DOWNLOAD, CommandType.RESUME_DOWNLOAD):
                    self._start_udp_download_select(cid, addr)
                elif cmd.type in (CommandType.UPLOAD, CommandType.RESUME_UPLOAD):
                    if cmd.type == CommandType.UPLOAD:
                        filename = cmd.args[0]
                        size = int(cmd.args[1])
                        offset = 0
                    else:
                        filename = cmd.args[0]
                        offset = int(cmd.args[1])
                        size = int(cmd.args[2])
                    self._start_udp_upload_select(cid, addr, filename, size, offset)
        else:
            rp = _HDR.pack(0, PacketType.CMD.value) + format_response(resp)
            try:
                self.server_socket_udp.sendto(rp, addr)
            except OSError:
                pass

    def _start_udp_upload_select(self, cid, addr, filename, total_size, offset=0):
        """Создаём listener для приёма upload через TCP — без потоков."""
        session = self.file_manager.get_session(cid)
        if not session or not session.is_upload:
            return

        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_sock.bind(("", 0))
        tcp_port = tcp_sock.getsockname()[1]
        tcp_sock.listen(1)
        tcp_sock.setblocking(False)

        print(f"[{_ts()}] UDP Upload (select): {filename} ← {addr[0]}:{addr[1]} port {tcp_port}")

        # Сохраняем listener — select будет следить за ним
        self.udp_transfer_listeners[cid] = tcp_sock
        # Помечаем сессию
        session._upload_listener_port = tcp_port
        session._upload_addr = addr
        session._port_notify_count = 0
        session._port_notify_time = 0

    def _start_udp_download_select(self, cid, addr):
        """Создаём listener для отдачи download через TCP — без потоков."""
        session = self.file_manager.get_session(cid)
        if not session or session.is_upload:
            return

        session.udp_download_active = True
        session.udp_client_addr = addr

        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_sock.bind(("", 0))
        tcp_port = tcp_sock.getsockname()[1]
        tcp_sock.listen(1)
        tcp_sock.setblocking(False)

        print(f"[{_ts()}] UDP Download (select): {session.filename} → {addr[0]}:{addr[1]} port {tcp_port}")

        self.udp_transfer_listeners[cid] = tcp_sock
        session._download_listener_port = tcp_port
        session._download_addr = addr
        session._port_notify_count = 0
        session._port_notify_time = 0

    def _send_port_notifications(self):
        """Периодически шлём UDP-уведомления о порте (вместо цикла в потоке)."""
        now = time.time()
        for cid in list(self.udp_transfer_listeners.keys()):
            session = self.file_manager.get_session(cid)
            if not session:
                # Сессия закрылась — убираем listener
                sock = self.udp_transfer_listeners.pop(cid, None)
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass
                continue

            # Определяем тип: upload или download
            port = getattr(session, '_upload_listener_port', None) or \
                   getattr(session, '_download_listener_port', None)
            addr = getattr(session, '_upload_addr', None) or \
                   getattr(session, '_download_addr', None)
            count = getattr(session, '_port_notify_count', 0)
            last_time = getattr(session, '_port_notify_time', 0)

            if port is None or addr is None:
                continue
            if count >= 300:  # 30 секунд * 10/сек
                continue
            if now - last_time < 0.1:
                continue

            is_upload = hasattr(session, '_upload_listener_port') and session._upload_listener_port
            if is_upload:
                msg = f"UPLOAD_PORT {port}"
            else:
                msg = f"DOWNLOAD_PORT {port}"

            pkt = _HDR.pack(0, PacketType.CMD.value) + msg.encode()
            try:
                self.server_socket_udp.sendto(pkt, addr)
            except OSError:
                pass
            session._port_notify_count = count + 1
            session._port_notify_time = now

    def _udp_transfer_accept(self, listener_sock):
        """Accept на listener для UDP transfer."""
        # Находим cid по listener
        cid = None
        for c, s in self.udp_transfer_listeners.items():
            if s is listener_sock:
                cid = c
                break
        if cid is None:
            return

        try:
            client_conn, client_addr = listener_sock.accept()
        except OSError:
            return

        client_conn.setblocking(False)
        print(f"[{_ts()}] UDP transfer: got TCP connection from {client_addr} for {cid}")

        # Убираем listener
        del self.udp_transfer_listeners[cid]
        try:
            listener_sock.close()
        except OSError:
            pass

        session = self.file_manager.get_session(cid)
        if not session:
            client_conn.close()
            return

        if session.is_upload:
            # Upload: читаем из client_conn, пишем в файл
            # Добавляем в inputs для чтения
            self.inputs.append(client_conn)
            # Переназначаем сессию на новый сокет
            new_cid = f"udp_upload_{cid}"
            session.client_id = new_cid
            session.sock = client_conn
            session._udp_upload_conn = client_conn
            session._udp_upload_total = session.total_size
            # Перемещаем сессию
            self.file_manager.sessions.pop(cid, None)
            self.file_manager.sessions[new_cid] = session
            self.tcp_buffers[client_conn.fileno()] = b""
            # Помечаем как "UDP upload via TCP" — _tcp_read будет обрабатывать
        else:
            # Download: пишем в client_conn
            self.udp_transfer_connections[cid] = (client_conn, session)

    def _udp_transfer_write(self, cid, sock, session):
        """Пишем данные в TCP сокет для UDP download — без потоков."""
        if not session.file_handle:
            self._cleanup_udp_transfer(cid, sock)
            return
        try:
            chunk = session.file_handle.read(TCP_CHUNK)
        except OSError:
            self._cleanup_udp_transfer(cid, sock)
            return

        if chunk:
            try:
                sent = sock.send(chunk)
                if sent < len(chunk):
                    session.file_handle.seek(-(len(chunk) - sent), 1)
                session.transferred += sent
            except (OSError, BrokenPipeError):
                self._cleanup_udp_transfer(cid, sock)
                return
            session.last_activity = time.time()
            self._log(cid, session, "UDP Download (select)")
        else:
            br = self.file_manager.calculate_bitrate(session)
            bs = self.file_manager.format_bitrate(br)
            print(f"[{_ts()}] UDP Download done (select): {session.filename} ({bs})")
            self._cleanup_udp_transfer(cid, sock)

    def _cleanup_udp_transfer(self, cid, sock):
        self.udp_transfer_connections.pop(cid, None)
        self.file_manager.complete_session(cid)
        try:
            sock.close()
        except OSError:
            pass

    def _udp_data(self, cid, seq, data, addr) -> None:
        session = self.file_manager.get_session(cid)
        if not session or not session.is_upload:
            return
        session.last_activity = time.time()

        if seq == session.expected_seq:
            try:
                session.file_handle.write(data)
            except (IOError, OSError):
                return
            session.transferred += len(data)
            session.expected_seq += 1

            while session.expected_seq in session.udp_recv_buffer:
                d = session.udp_recv_buffer.pop(session.expected_seq)
                try:
                    session.file_handle.write(d)
                except (IOError, OSError):
                    return
                session.transferred += len(d)
                session.expected_seq += 1

            self._log(cid, session, "UDP Upload")

        elif seq > session.expected_seq:
            if seq < session.expected_seq + UDP_WINDOW_SIZE * 2:
                session.udp_recv_buffer[seq] = data
            nack = _HDR.pack(session.expected_seq, PacketType.NACK.value)
            try:
                self.server_socket_udp.sendto(nack, addr)
            except OSError:
                pass

    def _udp_fin(self, cid, seq, addr) -> None:
        fin_ack = _HDR.pack(seq + 1, PacketType.ACK.value)
        for _ in range(10):
            try:
                self.server_socket_udp.sendto(fin_ack, addr)
            except OSError:
                pass
            time.sleep(0.01)
        session = self.file_manager.get_session(cid)
        if session and session.is_upload:
            br = self.file_manager.calculate_bitrate(session)
            bs = self.file_manager.format_bitrate(br)
            self.file_manager.complete_session(cid)
            print(f"[{_ts()}] UDP Upload done: {session.filename} ({bs})")

    def _udp_send_acks(self) -> None:
        now = time.time()
        for cid, sess in list(self.file_manager.sessions.items()):
            if not sess.is_upload or ":" not in cid or cid.isdigit():
                continue
            if now - sess.udp_last_ack_time < UDP_ACK_INTERVAL * 0.1:
                continue
            parts = cid.rsplit(":", 1)
            if len(parts) != 2:
                continue
            try:
                addr = (parts[0], int(parts[1]))
            except ValueError:
                continue
            ack = _HDR.pack(sess.expected_seq, PacketType.ACK.value)
            try:
                self.server_socket_udp.sendto(ack, addr)
            except OSError:
                pass
            sess.udp_last_ack_time = now

    # ── helpers ───────────────────────────────────────────

    def _log(self, cid, session, op) -> None:
        if session.total_size <= 0:
            return
        pct = int(session.transferred / session.total_size * 100)
        if pct >= getattr(session, "_last_pct", -10) + 10 or pct == 100:
            session._last_pct = pct
            print(f"[{_ts()}] {op}: {session.filename} [{cid}] — {pct}%")

    def _remove(self, sock: socket.socket) -> None:
        try:
            addr = sock.getpeername()
            print(f"[{_ts()}] Disconnected: {addr[0]}:{addr[1]}")
        except OSError:
            pass
        fd = sock.fileno()
        if sock in self.inputs:
            self.inputs.remove(sock)
        if sock in self.outputs:
            self.outputs.remove(sock)
        self.tcp_buffers.pop(fd, None)
        self.file_manager.close_session(str(fd))
        # Также проверяем udp_upload сессии
        for cid in list(self.file_manager.sessions.keys()):
            sess = self.file_manager.sessions[cid]
            if getattr(sess, '_udp_upload_conn', None) is sock:
                if sess.transferred >= sess.total_size:
                    br = self.file_manager.calculate_bitrate(sess)
                    bs = self.file_manager.format_bitrate(br)
                    self.file_manager.complete_session(cid)
                    print(f"[{_ts()}] UDP Upload done (select): {sess.filename} ({bs})")
                else:
                    self.file_manager.close_session(cid)
                break
        try:
            sock.close()
        except OSError:
            pass


def main(host: str = "0.0.0.0", port: int = 9000) -> None:
    server = SelectServer(host, port)
    server.start()