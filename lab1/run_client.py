#!/usr/bin/env python3
# run_client.py
import argparse
from client.client import InteractiveClient

def main():
    p = argparse.ArgumentParser()
    p.add_argument('host')
    p.add_argument('--port', type=int, default=9000)
    a = p.parse_args()
    InteractiveClient(a.host, a.port).run()

if __name__ == '__main__':
    main()