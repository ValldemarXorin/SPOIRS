#!/usr/bin/env python3
import argparse
from client.client import InteractiveClient

def main():
    parser = argparse.ArgumentParser(description='File Transfer Client')
    parser.add_argument('host', help='Server host')
    parser.add_argument('--port', type=int, default=9000)
    args = parser.parse_args()
    InteractiveClient(args.host, args.port).run()

if __name__ == '__main__':
    main()