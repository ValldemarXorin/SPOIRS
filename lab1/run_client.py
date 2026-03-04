#!/usr/bin/env python3
"""Точка входа клиента."""

import argparse
from client.client import InteractiveClient


def main():
    parser = argparse.ArgumentParser(description='File Transfer Client')
    parser.add_argument('host', help='Server host')
    parser.add_argument('--port', type=int, default=9000, help='Server port')
    args = parser.parse_args()

    client = InteractiveClient(args.host, args.port)
    client.run()


if __name__ == '__main__':
    main()