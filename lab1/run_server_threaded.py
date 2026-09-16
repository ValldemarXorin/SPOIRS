#!/usr/bin/env python3
"""Сервер на многопоточности — по потоку на каждого клиента."""

import argparse
from lab1.server.server_threaded import main

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="FTP Server (threaded)")
    p.add_argument('--host', default='0.0.0.0')
    p.add_argument('--port', type=int, default=9000)
    a = p.parse_args()
    main(a.host, a.port)