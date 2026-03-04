#!/usr/bin/env python3
"""Точка входа для запуска сервера с пулом процессов (вариант 12)."""

from server.server import main


def run():
    # main() сам создаёт сокеты и поднимает пул процессов
    main()


if __name__ == "__main__":
    run()
