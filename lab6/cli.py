"""CLI interface (logic from lr6.py run_cli) с argparse."""

import argparse
import threading

from lab6.chat import P2PChat
from lab6.network import DEFAULT_PORT, MULTICAST_GROUP


class ChatCLI:
    """Командный интерфейс чата (команды как в lr6.py)."""

    def __init__(self, chat: P2PChat):
        self.chat = chat
        self.running = False

    def run(self):
        self.running = True
        chat = self.chat

        print(f"Локальный IP: {chat.ip} | Маска: {chat.netmask} | Broadcast: {chat.broadcast_ip}")
        print("Команды: /mode [b|m], /peers, /ignore <ip>, /unignore <ip>, /leave, /join, /exit")

        t_recv = threading.Thread(target=chat.receive_loop, daemon=True)
        t_beat = threading.Thread(target=chat.heartbeat_loop, daemon=True)
        t_recv.start()
        t_beat.start()

        try:
            while chat.running:
                try:
                    cmd = input("> ").strip()
                except (EOFError, KeyboardInterrupt):
                    break
                if not cmd:
                    continue
                if cmd == "/exit":
                    chat.running = False
                elif cmd == "/peers":
                    chat.display_peers()
                elif cmd == "/mode b":
                    chat.mode = "BROADCAST"
                    print("[СИСТЕМА] Режим отправки переключен на BROADCAST")
                elif cmd == "/mode m":
                    chat.mode = "MULTICAST"
                    print("[СИСТЕМА] Режим отправки переключен на MULTICAST")
                elif cmd == "/leave":
                    chat.leave_multicast()
                elif cmd == "/join":
                    chat.join_multicast()
                elif cmd.startswith("/ignore "):
                    target = cmd.split(" ")[1].strip()
                    chat.ignored_ips.add(target)
                    print(f"[СИСТЕМА] Хост {target} добавлен в черный список.")
                elif cmd.startswith("/unignore "):
                    target = cmd.split(" ")[1].strip()
                    chat.ignored_ips.discard(target)
                    print(f"[СИСТЕМА] Хост {target} удален из черного списка.")
                else:
                    chat.send_packet("TEXT", cmd)
        finally:
            chat.cleanup()


def main():
    parser = argparse.ArgumentParser(description="P2P Chat (Broadcast + Multicast)")
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT, help="Порт")
    parser.add_argument("-g", "--group", default=MULTICAST_GROUP, help="Multicast группа")
    parser.add_argument("-n", "--name", default=None, help="Никнейм (спросит, если не указан)")
    args = parser.parse_args()

    name = args.name
    if not name:
        try:
            name = input("Введите ваш никнейм: ").strip()
        except (EOFError, KeyboardInterrupt):
            name = ""
        if not name:
            name = "User"

    chat = P2PChat(username=name, port=args.port, multicast_group=args.group)
    ChatCLI(chat).run()


if __name__ == "__main__":
    main()