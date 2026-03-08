#!/usr/bin/env python3
"""Клиент для подключения к threaded-серверу."""

import argparse
from client.client import InteractiveClient

def main():
    p = argparse.ArgumentParser(description="FTP Client (for threaded server)")
    p.add_argument('host')
    p.add_argument('--port', type=int, default=9000)
    a = p.parse_args()
    print("=== Client for THREADED server ===")
    InteractiveClient(a.host, a.port).run()

if __name__ == '__main__':
    main()