"""P2P Chat (logic from lr6.py): PING discovery, broadcast/multicast send, CLI."""

import json
import socket
import threading
import time

from lab6.network import (
    get_local_interfaces,
    create_recv_socket,
    create_send_socket,
    join_multicast,
    leave_multicast,
    DEFAULT_PORT,
    MULTICAST_GROUP,
)


class P2PChat:
    """Основной класс P2P-чата (логика эталонного lr6.py)."""

    def __init__(self, username: str, port: int = DEFAULT_PORT,
                 multicast_group: str = MULTICAST_GROUP):
        self.username = username
        self.port = port
        self.multicast_group = multicast_group
        self.ip, self.netmask, self.broadcast_ip = get_local_interfaces()
        self.mode = "BROADCAST"
        self.in_multicast_group = False
        self.ignored_ips = set()
        self.active_peers = {}
        self.running = True

        self.setup_sockets()
        self.join_multicast()

    def setup_sockets(self):
        """Инициализация сокетов приема и передачи для Windows и Linux."""
        self.recv_sock = create_recv_socket(self.port)
        self.send_sock = create_send_socket()

    def join_multicast(self):
        """Подключение сокета к группе Multicast."""
        if not self.in_multicast_group:
            if join_multicast(self.recv_sock, self.multicast_group):
                self.in_multicast_group = True
                print(f"[СИСТЕМА] Подключено к группе {self.multicast_group}")
            else:
                print(f"[СИСТЕМА] Ошибка подключения Multicast")

    def leave_multicast(self):
        """Выход сокета из группы Multicast."""
        if self.in_multicast_group:
            if leave_multicast(self.recv_sock, self.multicast_group):
                self.in_multicast_group = False
                print(f"[СИСТЕМА] Вы покинули группу {self.multicast_group}")
            else:
                print(f"[СИСТЕМА] Ошибка отключения Multicast")

    def send_packet(self, msg_type: str, content: str = ""):
        """Формирование и одиночная отправка сообщения."""
        data = json.dumps({
            "type": msg_type,
            "sender": self.username,
            "ip": self.ip,
            "content": content
        }).encode("utf-8")

        # Текстовые сообщения шлём юникастом каждому известному пиру:
        # broadcast в Wi-Fi не имеет L2-ACK/ретрансмиссии и на загруженной
        # точке доступа теряется, а юникаст ретранслируется драйвером.
        if msg_type == "TEXT" and self.active_peers:
            for peer_ip in list(self.active_peers):
                self.send_sock.sendto(data, (peer_ip, self.port))
            return

        if self.mode == "BROADCAST":
            self.send_sock.sendto(data, (self.broadcast_ip, self.port))
        elif self.mode == "MULTICAST" and self.in_multicast_group:
            self.send_sock.sendto(data, (self.multicast_group, self.port))

    def heartbeat_loop(self):
        """Фоновая рассылка PING-маяков для автообнаружения пиров."""
        while self.running:
            try:
                self.send_packet("PING")
                now = time.time()
                expired = [ip for ip, d in self.active_peers.items() if now - d["last_seen"] > 10.0]
                for ip in expired:
                    del self.active_peers[ip]
            except Exception:
                pass
            time.sleep(2.5)

    def receive_loop(self):
        """Прием и разбор сообщений."""
        while self.running:
            try:
                raw_data, (sender_ip, _) = self.recv_sock.recvfrom(4096)
                if sender_ip in self.ignored_ips:
                    continue

                msg = json.loads(raw_data.decode("utf-8"))
                name = msg.get("sender", sender_ip)
                mtype = msg.get("type", "")

                if mtype == "PING":
                    self.active_peers[sender_ip] = {"name": name, "last_seen": time.time()}
                elif mtype == "TEXT":
                    if sender_ip != self.ip:
                        print(f"\n[{name} @ {sender_ip}]: {msg.get('content')}\n> ", end="", flush=True)
            except Exception:
                break

    def display_peers(self):
        """Отображение списка активных участников."""
        print(f"\n--- Список активных пиров (найдено: {len(self.active_peers)}) ---")
        for ip, info in self.active_peers.items():
            ign = "[ЗАБЛОКИРОВАН]" if ip in self.ignored_ips else "[АКТИВЕН]"
            me = "(Это вы)" if ip == self.ip else ""
            print(f"  • {ip:15s} | {info['name']:10s} {ign} {me}")
        print("--------------------------------------------------")

    def cleanup(self):
        self.running = False
        self.leave_multicast()
        self.recv_sock.close()
        self.send_sock.close()