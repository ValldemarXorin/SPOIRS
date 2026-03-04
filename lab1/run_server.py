#!/usr/bin/env python3
import argparse
from server.server import TCPServer

def main():
    parser = argparse.ArgumentParser(description='File Transfer Server')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=9000)
    args = parser.parse_args()
    TCPServer(args.host, args.port).start()

if __name__ == '__main__':
    main()