"""Лабораторная работа №6: P2P-чат через UDP Broadcast/Multicast."""

import ipaddress
import json
import socket
import struct
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

try:
    import psutil
except ImportError:
    psutil = None


DEFAULT_PORT = 50000
MULTICAST_GROUP = "239.255.0.1"
HELLO_INTERVAL = 3.0
PEER_TIMEOUT = 12.0


@dataclass
class InterfaceInfo:
    name: str
    ip: str
    netmask: str
    broadcast: str
    is_up: bool = True
    is_loopback: bool = False


class NetworkUtils:
    @staticmethod
    def calculate_broadcast(ip: str, netmask: str) -> str:
        try:
            network = ipaddress.IPv4Network(f"{ip}/{netmask}", strict=False)
            return str(network.broadcast_address)
        except (ipaddress.AddressValueError, ipaddress.NetmaskValueError):
            return "255.255.255.255"

    @staticmethod
    def get_route_ip() -> str:
        """Определяет IP интерфейса, через который ОС строит обычный маршрут."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # UDP connect не отправляет пакет, а только просит ОС выбрать маршрут.
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
        except OSError:
            return "127.0.0.1"
        finally:
            sock.close()

    @classmethod
    def get_interfaces(cls) -> List[InterfaceInfo]:
        interfaces: List[InterfaceInfo] = []

        if psutil is not None:
            stats = psutil.net_if_stats()
            for name, addresses in psutil.net_if_addrs().items():
                is_up = stats.get(name).isup if name in stats else True
                for address in addresses:
                    if address.family != socket.AF_INET:
                        continue
                    ip = address.address
                    netmask = address.netmask or "255.255.255.0"
                    broadcast = address.broadcast or cls.calculate_broadcast(ip, netmask)
                    interfaces.append(
                        InterfaceInfo(
                            name=name,
                            ip=ip,
                            netmask=netmask,
                            broadcast=broadcast,
                            is_up=is_up,
                            is_loopback=ip.startswith("127."),
                        )
                    )

        if interfaces:
            return interfaces

        # Резервный вариант, если psutil не установлен.
        route_ip = cls.get_route_ip()
        if route_ip.startswith("172.20.10."):
            netmask = "255.255.255.240"
        elif route_ip.startswith("127."):
            netmask = "255.0.0.0"
        else:
            netmask = "255.255.255.0"

        interface_name = "Network interface"
        try:
            for _, name in socket.if_nameindex():
                if name:
                    interface_name = name
                    break
        except (AttributeError, OSError):
            pass

        return [
            InterfaceInfo(
                name=interface_name,
                ip=route_ip,
                netmask=netmask,
                broadcast=cls.calculate_broadcast(route_ip, netmask),
                is_up=True,
                is_loopback=route_ip.startswith("127."),
            )
        ]

    @classmethod
    def select_interface(cls, preferred_ip: Optional[str] = None) -> InterfaceInfo:
        interfaces = cls.get_interfaces()
        if not interfaces:
            raise RuntimeError("No IPv4 network interfaces found")

        if preferred_ip:
            for interface in interfaces:
                if interface.ip == preferred_ip:
                    return interface
            raise RuntimeError(f"Interface with IP {preferred_ip} not found")

        route_ip = cls.get_route_ip()
        for interface in interfaces:
            if interface.ip == route_ip and interface.is_up:
                return interface

        for interface in interfaces:
            if interface.is_up and not interface.is_loopback:
                return interface

        return interfaces[0]


class P2PChat:
    def __init__(
        self,
        username: str = "Anonymous",
        port: int = DEFAULT_PORT,
        interface_ip: Optional[str] = None,
    ):
        self.username = username
        self.port = port
        self.interface = NetworkUtils.select_interface(interface_ip)
        self.ip = self.interface.ip
        self.netmask = self.interface.netmask
        self.broadcast_ip = self.interface.broadcast

        self.mode = "BROADCAST"
        self.instance_id = uuid.uuid4().hex
        self.in_multicast_group = False
        self.ignored_ips: Set[str] = set()
        self.active_peers: Dict[str, Dict[str, object]] = {}
        self.running = False
        self.lock = threading.RLock()

        self.recv_sock: Optional[socket.socket] = None
        self.send_sock: Optional[socket.socket] = None
        self.setup_sockets()
        self.join_multicast(silent=True)

    def setup_sockets(self) -> None:
        """Создаёт совместимые с Windows и Linux UDP-сокеты."""
        self.recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if hasattr(socket, "SO_REUSEPORT"):
            try:
                self.recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass

        self.recv_sock.bind(("0.0.0.0", self.port))
        self.recv_sock.settimeout(1.0)

        self.send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        self.send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

        try:
            self.send_sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_MULTICAST_IF,
                socket.inet_aton(self.ip),
            )
        except OSError:
            pass

    def join_multicast(self, silent: bool = False) -> bool:
        if self.in_multicast_group or self.recv_sock is None:
            return self.in_multicast_group

        try:
            membership = struct.pack(
                "4s4s",
                socket.inet_aton(MULTICAST_GROUP),
                socket.inet_aton(self.ip),
            )
            self.recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
            self.in_multicast_group = True
            if not silent:
                print(f"Joined multicast group {MULTICAST_GROUP}")
            return True
        except OSError as error:
            if not silent:
                print(f"Unable to join multicast group: {error}")
            return False

    def leave_multicast(self, silent: bool = False) -> None:
        if not self.in_multicast_group or self.recv_sock is None:
            return

        try:
            membership = struct.pack(
                "4s4s",
                socket.inet_aton(MULTICAST_GROUP),
                socket.inet_aton(self.ip),
            )
            self.recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, membership)
        except OSError:
            pass
        finally:
            self.in_multicast_group = False
            if not silent:
                print(f"Left multicast group {MULTICAST_GROUP}")

    def build_packet(
        self,
        message_type: str,
        text: str = "",
        target_ip: Optional[str] = None,
    ) -> bytes:
        packet = {
            "type": message_type,
            "sender": self.username,
            "ip": self.ip,
            "text": text,
            "timestamp": time.time(),
            "instance_id": self.instance_id,
        }
        if target_ip is not None:
            packet["target_ip"] = target_ip
        return json.dumps(packet, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    def send_packet(
        self,
        message_type: str,
        text: str = "",
        target_ip: Optional[str] = None,
        force_broadcast: bool = False,
    ) -> bool:
        if self.send_sock is None:
            return False

        data = self.build_packet(message_type, text, target_ip)
        try:
            # TEXT шлём юникастом каждому известному пиру: broadcast в Wi-Fi
            # не имеет L2-ACK/ретрансмиссии и на загруженной точке доступа
            # теряется, а юникаст ретранслируется драйвером.
            if message_type == "TEXT" and self.active_peers:
                for peer_ip in list(self.active_peers):
                    self.send_sock.sendto(data, (peer_ip, self.port))
                return True
            if force_broadcast or self.mode == "BROADCAST":
                destination = (self.broadcast_ip, self.port)
            else:
                if not self.in_multicast_group:
                    print("Multicast mode is unavailable: the group has not been joined")
                    return False
                destination = (MULTICAST_GROUP, self.port)

            self.send_sock.sendto(data, destination)
            return True
        except OSError as error:
            print(f"Send error: {error}")
            return False

    def heartbeat_loop(self) -> None:
        while self.running:
            self.send_packet("HELLO")
            self.remove_expired_peers()
            time.sleep(HELLO_INTERVAL)

    def remove_expired_peers(self) -> None:
        now = time.time()
        with self.lock:
            expired = [
                ip
                for ip, peer in self.active_peers.items()
                if now - float(peer["last_seen"]) > PEER_TIMEOUT
            ]
            for ip in expired:
                del self.active_peers[ip]

    def update_peer(self, sender_ip: str, sender_name: str) -> None:
        with self.lock:
            self.active_peers[sender_ip] = {
                "name": sender_name,
                "last_seen": time.time(),
            }

    def receive_loop(self) -> None:
        if self.recv_sock is None:
            return

        while self.running:
            try:
                raw_data, (sender_ip, _) = self.recv_sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                if self.running:
                    print("Receive socket was closed unexpectedly")
                break

            try:
                message = json.loads(raw_data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue

            if message.get("instance_id") == self.instance_id:
                continue

            message_type = str(message.get("type", ""))
            sender_name = str(message.get("sender", sender_ip))

            if message_type in {"HELLO", "TEXT", "IGNORE", "UNIGNORE"}:
                self.update_peer(sender_ip, sender_name)

            if message_type == "TEXT":
                with self.lock:
                    ignored = sender_ip in self.ignored_ips
                if not ignored:
                    timestamp = float(message.get("timestamp", time.time()))
                    clock = time.strftime("%H:%M:%S", time.localtime(timestamp))
                    print(
                        f"\n[{clock}] {sender_name} ({sender_ip}): "
                        f"{message.get('text', '')}\n> ",
                        end="",
                        flush=True,
                    )
            elif message_type == "BYE":
                with self.lock:
                    self.active_peers.pop(sender_ip, None)

    def display_peers(self) -> None:
        self.remove_expired_peers()
        with self.lock:
            peers = list(self.active_peers.items())
            ignored = set(self.ignored_ips)

        if not peers:
            print("No connected peers")
            return

        print(f"Connected peers ({len(peers)}):")
        now = time.time()
        for ip, info in sorted(peers):
            status = "ignored" if ip in ignored else "active"
            age = now - float(info["last_seen"])
            print(f"  {ip:<15} {str(info['name']):<15} {age:>5.0f}s ago [{status}]")

    @staticmethod
    def show_help() -> None:
        print("=== P2P Chat Commands ===")
        print("  /help           - Show this help")
        print("  /name <name>    - Set your display name")
        print("  /list           - List connected peers")
        print("  /ignore <ip>    - Ignore a peer (broadcasts IGNORE)")
        print("  /unignore <ip>  - Stop ignoring a peer")
        print("  /bcast          - Switch to broadcast mode")
        print("  /mcast          - Switch to multicast mode")
        print("  /mode           - Show current mode")
        print("  /interfaces     - Show available network interfaces")
        print("  /quit           - Exit chat")
        print("============================")

    @staticmethod
    def show_interfaces() -> None:
        interfaces = NetworkUtils.get_interfaces()
        print("Available network interfaces:")
        for interface in interfaces:
            state = "UP" if interface.is_up else "DOWN"
            print(
                f"  {interface.name}: {interface.ip}/{interface.netmask}, "
                f"broadcast {interface.broadcast} [{state}]"
            )

    @staticmethod
    def command_argument(command: str) -> str:
        parts = command.split(maxsplit=1)
        return parts[1].strip() if len(parts) == 2 else ""

    def handle_command(self, command: str) -> bool:
        lowered = command.lower()

        if lowered == "/help":
            self.show_help()
        elif lowered == "/list":
            self.display_peers()
        elif lowered == "/bcast":
            self.mode = "BROADCAST"
            print("Mode: BROADCAST")
        elif lowered == "/mcast":
            if self.in_multicast_group or self.join_multicast(silent=True):
                self.mode = "MULTICAST"
                print("Mode: MULTICAST")
            else:
                print("Cannot switch mode: multicast group is unavailable")
        elif lowered == "/mode":
            print(f"Mode: {self.mode}")
        elif lowered == "/interfaces":
            self.show_interfaces()
        elif lowered == "/quit":
            return False
        elif lowered.startswith("/name"):
            new_name = self.command_argument(command)
            if not new_name:
                print("Usage: /name <name>")
            else:
                old_name = self.username
                self.username = new_name
                print(f"Name changed: {old_name} -> {self.username}")
                self.send_packet("HELLO")
        elif lowered.startswith("/ignore"):
            target_ip = self.command_argument(command)
            if not self.is_valid_ip(target_ip):
                print("Usage: /ignore <ip>")
            else:
                with self.lock:
                    self.ignored_ips.add(target_ip)
                self.send_packet("IGNORE", target_ip=target_ip, force_broadcast=True)
                print(f"Peer {target_ip} is now ignored")
        elif lowered.startswith("/unignore"):
            target_ip = self.command_argument(command)
            if not self.is_valid_ip(target_ip):
                print("Usage: /unignore <ip>")
            else:
                with self.lock:
                    self.ignored_ips.discard(target_ip)
                self.send_packet("UNIGNORE", target_ip=target_ip, force_broadcast=True)
                print(f"Peer {target_ip} is no longer ignored")
        else:
            print("Unknown command. Type /help")

        return True

    @staticmethod
    def is_valid_ip(value: str) -> bool:
        try:
            ipaddress.IPv4Address(value)
            return True
        except ipaddress.AddressValueError:
            return False

    def run_cli(self) -> None:
        print(
            f"Using interface: {self.interface.name} "
            f"({self.ip}/{self.netmask})"
        )
        print(f"Broadcast address: {self.broadcast_ip}")
        print(f"Multicast group: {MULTICAST_GROUP}")

        self.running = True
        receive_thread = threading.Thread(target=self.receive_loop, daemon=True)
        heartbeat_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        receive_thread.start()
        heartbeat_thread.start()

        print(f"Chat started as '{self.username}' on {self.ip}:{self.port}")
        print(f"Mode: {self.mode}")
        print("Type /help for commands")
        self.show_help()

        try:
            while self.running:
                try:
                    line = input("> ").strip()
                except (KeyboardInterrupt, EOFError):
                    break

                if not line:
                    continue

                if line.startswith("/"):
                    if not self.handle_command(line):
                        break
                else:
                    self.send_packet("TEXT", line)
        finally:
            self.cleanup()
            print("Chat stopped")

    def cleanup(self) -> None:
        if not self.running and self.recv_sock is None and self.send_sock is None:
            return

        if self.running:
            self.send_packet("BYE")
        self.running = False
        self.leave_multicast(silent=True)

        if self.recv_sock is not None:
            try:
                self.recv_sock.close()
            except OSError:
                pass
            self.recv_sock = None

        if self.send_sock is not None:
            try:
                self.send_sock.close()
            except OSError:
                pass
            self.send_sock = None


if __name__ == "__main__":
    chat = P2PChat(username="Anonymous", port=DEFAULT_PORT)
    chat.run_cli()
