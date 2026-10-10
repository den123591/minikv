"""Точки входа: ``minikv-server`` и ``minikv-cli``."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import shlex
import signal
import socket
import sys
from pathlib import Path

from minikv.resp import NULL_ARRAY, ErrorReply, Parser, SimpleString, encode
from minikv.server import Config, MiniKVServer


def server_main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="minikv-server", description="Redis-совместимый сервер")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6380)
    p.add_argument("--aof", type=Path, help="путь к append-only файлу (без него — только память)")
    p.add_argument("--appendfsync", choices=["always", "everysec", "no"], default="everysec")
    p.add_argument("--max-keys", type=int, help="лимит ключей")
    p.add_argument("--eviction", choices=["noeviction", "allkeys-lru"], default="noeviction")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config = Config(
        host=args.host,
        port=args.port,
        aof_path=args.aof,
        appendfsync=args.appendfsync,
        max_keys=args.max_keys,
        eviction=args.eviction,
    )

    async def run() -> None:
        server = MiniKVServer(config)
        await server.start()
        print(f"minikv listening on {config.host}:{server.port}", file=sys.stderr)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)
        await stop.wait()
        await server.stop()  # корректно закрываем AOF с fsync

    asyncio.run(run())


def format_reply(value: object, indent: str = "") -> str:
    """Форматирование в стиле redis-cli."""
    if value is None or value is NULL_ARRAY:
        return "(nil)"
    if isinstance(value, ErrorReply):
        return f"(error) {value}"
    if isinstance(value, SimpleString):
        return str(value)
    if isinstance(value, bool):
        return f"(boolean) {str(value).lower()}"
    if isinstance(value, int):
        return f"(integer) {value}"
    if isinstance(value, bytes):
        return '"' + value.decode("utf-8", "backslashreplace") + '"'
    if isinstance(value, (list, set)):
        items = sorted(value) if isinstance(value, set) else value
        if not items:
            return "(empty array)"
        width = len(str(len(items)))
        lines = []
        for i, item in enumerate(items, start=1):
            prefix = f"{indent}{i:>{width}}) "
            lines.append(prefix + format_reply(item, indent + " " * len(prefix)).lstrip())
        return "\n".join(lines)
    if isinstance(value, dict):
        return format_reply([x for kv in value.items() for x in kv], indent)
    return str(value)


def cli_main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="minikv-cli")
    p.add_argument("-H", "--host", default="127.0.0.1")
    p.add_argument("-p", "--port", type=int, default=6380)
    p.add_argument("command", nargs="*", help="выполнить одну команду и выйти")
    args = p.parse_args(argv)

    sock = socket.create_connection((args.host, args.port))
    parser = Parser(server_mode=False)

    def call(parts: list[str]) -> object:
        sock.sendall(encode([x.encode() for x in parts]))
        while True:
            for value in parser:
                return value
            chunk = sock.recv(64 * 1024)
            if not chunk:
                raise ConnectionError("server closed the connection")
            parser.feed(chunk)

    if args.command:
        print(format_reply(call(args.command)))
        return
    prompt = f"{args.host}:{args.port}> "
    while True:
        try:
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not line.strip():
            continue
        try:
            parts = shlex.split(line)
        except ValueError as exc:
            print(f"(error) {exc}")
            continue
        print(format_reply(call(parts)))
        if parts[0].upper() == "QUIT":
            return
