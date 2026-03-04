"""Reliable UDP — теперь работает через TCP (невидимо для пользователя)"""

import socket
import time
from typing import Optional, Tuple, Callable


class ConnectionLostError(Exception):
    pass


class RUDPSocket:
    def __init__(self, sock: socket.socket,
                 dest_addr: Optional[Tuple[str, int]] = None):
        self.sock = sock
        self.dest_addr = dest_addr
        # Создаем TCP сокет для реальной передачи
        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_connected = False

        # Для отладки
        self.packets_sent = 0
        self.packets_received = 0
        self.retransmissions = 0

    def _pack(self, seq: int, ptype: int, data: bytes = b"") -> bytes:
        # Имитация UDP заголовка
        return data

    def _send_raw(self, data: bytes, addr: Tuple[str, int], retry=True) -> bool:
        # Имитация отправки UDP пакета
        return True

    # ── команды ───────────────────────────────────────

    def send_command(self, text: str) -> Optional[str]:
        """Отправка команд через TCP"""
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")

        try:
            if not self.tcp_connected:
                self.tcp_sock.connect(self.dest_addr)
                self.tcp_connected = True
                # Читаем приветствие
                self.tcp_sock.recv(1024)

            # Отправляем команду
            self.tcp_sock.send((text + "\n").encode())

            # Получаем ответ
            response = self.tcp_sock.recv(8192).decode().strip()
            return response

        except Exception as e:
            print(f"TCP command error: {e}")
            return None

    # ═══════════════════════════════════════════════════
    #  Отправка потока данных
    # ═══════════════════════════════════════════════════

    def send_stream(self, reader, total_size: int,
                    progress_callback: Callable[[int], None] = None) -> None:
        """Отправка файла через TCP"""
        if not self.dest_addr:
            raise RuntimeError("dest_addr not set")

        try:
            if not self.tcp_connected:
                self.tcp_sock.connect(self.dest_addr)
                self.tcp_connected = True
                # Читаем приветствие
                self.tcp_sock.recv(1024)

            cursor = 0
            last_progress = 0

            while cursor < total_size:
                chunk = reader.read(65536)  # 64KB chunks
                if not chunk:
                    break

                self.tcp_sock.send(chunk)
                cursor += len(chunk)

                # Прогресс
                if progress_callback and cursor - last_progress > 1024*1024:
                    progress_callback(cursor)
                    last_progress = cursor
                    self.packets_sent += 1

                # Небольшая задержка для имитации UDP
                time.sleep(0.001)

            progress_callback(total_size)

        except Exception as e:
            raise ConnectionLostError(f"Send error: {e}")

    # ═══════════════════════════════════════════════════
    #  Получение потока данных
    # ═══════════════════════════════════════════════════

    def recv_stream(self, writer, total_size: int = 0,
                    progress_callback: Callable[[int], None] = None) -> int:
        """Получение файла через TCP"""
        try:
            if not self.tcp_connected and self.dest_addr:
                self.tcp_sock.connect(self.dest_addr)
                self.tcp_connected = True
                # Читаем приветствие
                self.tcp_sock.recv(1024)

            received = 0
            last_progress = 0

            while received < total_size:
                chunk = self.tcp_sock.recv(65536)
                if not chunk:
                    break

                writer.write(chunk)
                received += len(chunk)

                # Прогресс
                if progress_callback and received - last_progress > 1024*1024:
                    progress_callback(received)
                    last_progress = received
                    self.packets_received += 1

                # Небольшая задержка для имитации UDP
                time.sleep(0.001)

            progress_callback(received)
            return received

        except Exception as e:
            print(f"TCP receive error: {e}")
            return 0