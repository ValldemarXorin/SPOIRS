"""CLI interface for P2P chat."""

import sys
import threading
from typing import Optional

try:
    import colorama
    colorama.init()
    HAS_COLORAMA = True
except ImportError:
    HAS_COLORAMA = False

from lab6.chat import P2PChat, SendMode
from lab6.network import DEFAULT_PORT, MULTICAST_GROUP


class ChatCLI:
    """Command-line interface for P2P chat."""

    def __init__(self, chat: P2PChat):
        self.chat = chat
        self.running = False

    def run(self) -> None:
        """Main CLI loop."""
        self.running = True
        chat = self.chat

        chat.set_output_callback(self._output)

        if not chat.start():
            print("Failed to start chat")
            return

        self._print_help()

        try:
            while self.running:
                try:
                    line = input("").strip()
                except (EOFError, KeyboardInterrupt):
                    break

                if not line:
                    continue

                if not self._process_command(line):
                    break

        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            chat.stop()

    def _output(self, msg: str) -> None:
        """Output message with prompt preservation."""
        sys.stdout.write("\r\033[K" if HAS_COLORAMA else "\r")
        sys.stdout.write(msg + "\n")
        sys.stdout.write("> ")
        sys.stdout.flush()

    def _print_help(self) -> None:
        self._output("=== P2P Chat Commands ===")
        self._output("  /help           - Show this help")
        self._output("  /name <name>    - Set your display name")
        self._output("  /peers          - List active peers")
        self._output("  /mode b|m       - Switch send mode: broadcast | multicast")
        self._output("  /join           - Join multicast group")
        self._output("  /leave          - Leave multicast group")
        self._output("  /ignore <ip>    - Ignore a peer (black list)")
        self._output("  /unignore <ip>  - Remove a peer from the black list")
        self._output("  /slow           - Toggle low-throughput fallback (patient RTO)")
        self._output("  /buffer         - Show messages awaiting delivery ACKs")
        self._output("  /rto            - Show RTO / reliability statistics")
        self._output("  /net            - Show local IP / broadcast / multicast")
        self._output("  /quit           - Exit chat")
        self._output("============================")

    def _process_command(self, line: str) -> bool:
        """Process command. Return False to quit."""
        parts = line.split()
        if not parts:
            return True

        cmd = parts[0].lower()

        if cmd == "/help":
            self._print_help()

        elif cmd == "/name":
            if len(parts) < 2:
                self._output("Usage: /name <name>")
            else:
                self.chat.set_name(parts[1])

        elif cmd in ("/peers", "/list"):
            for line in self.chat.list_peers():
                self._output(line)

        elif cmd == "/mode":
            if len(parts) < 2:
                self._output("Usage: /mode b|m")
            elif parts[1] in ("b", "broadcast"):
                self.chat.set_mode(SendMode.BROADCAST)
            elif parts[1] in ("m", "multicast"):
                self.chat.set_mode(SendMode.MULTICAST)
            else:
                self._output("Usage: /mode b|m")

        elif cmd == "/join":
            self.chat.join_multicast()

        elif cmd == "/leave":
            self.chat.leave_multicast()

        elif cmd == "/ignore":
            if len(parts) < 2:
                self._output("Usage: /ignore <ip>")
            else:
                self.chat.ignore_peer(parts[1])

        elif cmd == "/unignore":
            if len(parts) < 2:
                self._output("Usage: /unignore <ip>")
            else:
                self.chat.unignore_peer(parts[1])

        elif cmd == "/slow":
            if len(parts) > 1:
                self.chat.set_low_throughput(parts[1].lower() in ("on", "1", "true", "yes"))
            else:
                self.chat.set_low_throughput(not self.chat._low_throughput)

        elif cmd == "/buffer":
            for line in self.chat.buffer_info():
                self._output(line)

        elif cmd == "/rto":
            for line in self.chat.rto_info():
                self._output(line)

        elif cmd in ("/net", "/interfaces"):
            self._show_network()

        elif cmd in ("/quit", "/exit", "/q"):
            self._output("Quitting...")
            return False

        else:
            # Regular chat message
            self.chat.send_chat(line)

        return True

    def _show_network(self) -> None:
        net = self.chat.network
        self._output(f"Local IP: {net.ip}")
        self._output(f"Broadcast: {net.broadcast_ip}")
        self._output(f"Multicast group: {net.multicast_group} "
                     f"({'joined' if net.in_multicast_group else 'not joined'})")
        self._output(f"Mode: {self.chat._send_mode.value}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="P2P Chat (Broadcast + Multicast)")
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT, help="Port number")
    parser.add_argument("-g", "--group", default=MULTICAST_GROUP, help="Multicast group")
    parser.add_argument("-n", "--name", default=None, help="Display name (prompted if omitted)")
    parser.add_argument("--slow", dest="low_throughput", action="store_true",
                        help="Low-throughput fallback: patient RTOs, more retries")
    parser.add_argument("--rto", type=float, default=None,
                        help="Initial retransmission timeout in seconds")
    parser.add_argument("--min-rto", type=float, default=None,
                        help="Minimum retransmission timeout in seconds")
    parser.add_argument("--max-rto", type=float, default=None,
                        help="Maximum retransmission timeout in seconds")
    parser.add_argument("--max-retries", type=int, default=None,
                        help="Max resends before a message is dropped")
    parser.add_argument("--backoff", type=float, default=None,
                        help="RTO backoff multiplier per retry (default 2.0)")
    args = parser.parse_args()

    name = args.name
    if not name:
        try:
            name = input("Enter your nickname: ").strip()
        except (EOFError, KeyboardInterrupt):
            name = ""
        if not name:
            name = "User"

    chat = P2PChat(
        port=args.port,
        multicast_group=args.group,
        name=name,
        low_throughput=args.low_throughput,
        initial_rto=args.rto,
        min_rto=args.min_rto,
        max_rto=args.max_rto,
        max_retries=args.max_retries,
        backoff_factor=args.backoff,
    )

    cli = ChatCLI(chat)
    cli.run()


if __name__ == "__main__":
    main()