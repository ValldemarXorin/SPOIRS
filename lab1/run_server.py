#!/usr/bin/env python3
# run_server.py
import argparse
from server.server import TCPServer

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', default='0.0.0.0')
    p.add_argument('--port', type=int, default=9000)
    a = p.parse_args()
    TCPServer(a.host, a.port).start()

if __name__ == '__main__':
    main()