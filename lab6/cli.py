"""CLI interface for P2P chat."""

import sys
import threading
import time
from typing import Optional

try:
    import colorama
    colorama.init()
    HAS_COLORAMA = True
except ImportError:
    HAS_COLORAMA = False

from lab6.chat import P2PChat, SendMode
from lab6.network import get_interfaces


class ChatCLI:
    """Command-line interface for P2P chat."""

    def __init__(self, chat: P2PChat):
        self.chat = chat
        self.running = False
        self._input_thread: Optional[threading.Thread] = None

    def run(self) -> None:
        """Main CLI loop."""
        self.running = True
        chat = self.chat

        # Set output callback for chat to use our formatted output
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
        # Clear current line, print message, restore prompt
        sys.stdout.write("\r\033[K" if HAS_COLORAMA else "\r")
        sys.stdout.write(msg + "\n")
        sys.stdout.write("> ")
        sys.stdout.flush()

    def _print_help(self) -> None:
        self._output("=== P2P Chat Commands ===")
        self._output("  /help           - Show this help")
        self._output("  /name <name>    - Set your display name")
        self._output("  /list           - List connected peers")
        self._output("  /ignore <ip>    - Ignore a peer (broadcasts IGNORE)")
        self._output("  /unignore <ip>  - Stop ignoring a peer")
        self._output("  /bcast          - Switch to broadcast mode")
        self._output("  /mcast          - Switch to multicast mode")
        self._output("  /mode           - Show current mode")
        self._output("  /reliable       - Toggle ACK-based resend buffer")
        self._output("  /slow           - Toggle low-throughput fallback (patient RTO)")
        self._output("  /buffer         - Show messages awaiting delivery ACKs")
        self._output("  /rto            - Show RTO / reliability statistics")
        self._output("  /interfaces     - Show available network interfaces")
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

        elif cmd == "/list":
            for line in self.chat.list_peers():
                self._output(line)

        elif cmd == "/ignore":
            if len(parts) < 2:
                self._output("Usage: /ignore <ip>")
            else:
                if self.chat.ignore_peer(parts[1]):
                    self._output(f"Ignoring {parts[1]}")
                else:
                    self._output(f"Peer {parts[1]} not found")

        elif cmd == "/unignore":
            if len(parts) < 2:
                self._output("Usage: /unignore <ip>")
            else:
                if self.chat.unignore_peer(parts[1]):
                    self._output(f"Unignored {parts[1]}")
                else:
                    self._output(f"Peer {parts[1]} not found")

        elif cmd == "/bcast":
            self.chat.set_mode(SendMode.BROADCAST)

        elif cmd == "/mcast":
            self.chat.set_mode(SendMode.MULTICAST)

        elif cmd == "/mode":
            self._output(f"Current mode: {self.chat._send_mode.value.upper()}")

        elif cmd == "/reliable":
            # /reliable        → toggle
            # /reliable on|off → explicit
            if len(parts) > 1:
                self.chat.set_reliable(parts[1].lower() in ("on", "1", "true", "yes"))
            else:
                self.chat.set_reliable(not self.chat._reliable_enabled)

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

        elif cmd == "/interfaces":
            self._show_interfaces()

        elif cmd in ("/quit", "/exit", "/q"):
            self._output("Quitting...")
            return False

        else:
            # Regular chat message
            self.chat.send_chat(line)

        return True

    def _show_interfaces(self) -> None:
        interfaces = get_interfaces()
        if not interfaces:
            self._output("No interfaces found")
            return
        self._output("Available interfaces:")
        for iface in interfaces:
            marker = " *" if iface.name == (self.chat.network.interface.name if self.chat.network.interface else "") else ""
            loop = " (loopback)" if iface.is_loopback else ""
            self._output(f"  {iface.name}: {iface.ip}/{iface.netmask} -> bcast={iface.broadcast}{loop}{marker}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="P2P Chat (Broadcast + Multicast)")
    parser.add_argument("-p", "--port", type=int, default=50000, help="Port number")
    parser.add_argument("-g", "--group", default="239.255.0.1", help="Multicast group")
    parser.add_argument("-i", "--interface", help="Interface name")
    parser.add_argument("--ip", help="Interface IP")
    parser.add_argument("-n", "--name", default="Anonymous", help="Display name")
    parser.add_argument("--reliable", dest="reliable", action="store_true", default=True,
                        help="Enable ACK-based reliable delivery (default)")
    parser.add_argument("--no-reliable", dest="reliable", action="store_false",
                        help="Disable reliable delivery (best-effort)")
    parser.add_argument("--slow", dest="low_throughput", action="store_true",
                        help="Low-throughput network fallback: patient RTOs, more retries")
    parser.add_argument("--rto", type=float, default=None,
                        help="Initial retransmission timeout in seconds")
    parser.add_argument("--max-retries", type=int, default=None,
                        help="Max resends before a message is dropped")
    parser.add_argument("--backoff", type=float, default=None,
                        help="RTO backoff multiplier per retry (default 2.0)")
    args = parser.parse_args()

    chat = P2PChat(
        port=args.port,
        multicast_group=args.group,
        interface_name=args.interface,
        interface_ip=args.ip,
        name=args.name,
        reliable=args.reliable,
        low_throughput=args.low_throughput,
        initial_rto=args.rto,
        max_retries=args.max_retries,
        backoff_factor=args.backoff,
    )

    cli = ChatCLI(chat)
    cli.run()


if __name__ == "__main__":
    main()