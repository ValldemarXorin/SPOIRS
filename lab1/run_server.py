#!/usr/bin/env python3
"""Точка входа сервера."""

import argparse
from server.server import TCPServer


def main():
    parser = argparse.ArgumentParser(description='File Transfer Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind')
    parser.add_argument('--port', type=int, default=9000, help='Port to bind')
    args = parser.parse_args()

    server = TCPServer(args.host, args.port)
    server.start()


if __name__ == '__main__':
    main()