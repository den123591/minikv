"""TCP-сервер на asyncio.

Модель как у Redis: один поток и цикл событий. Пока выполняется команда, другие
не выполняются, поэтому каждая команда атомарна, а блокировки не нужны. asyncio даёт
обслуживать тысячи соединений одновременно — ожидание сети не блокирует остальных.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from minikv import __version__
from minikv.aof import AppendOnlyFile, FsyncPolicy
from minikv.client_state import ClientState
from minikv.commands import Executor, MultiReply
from minikv.keyspace import EvictionPolicy, Keyspace
from minikv.resp import OK, ErrorReply, Parser, ProtocolError, encode

log = logging.getLogger(__name__)


@dataclass
class Config:
    host: str = "127.0.0.1"
    port: int = 6380
    aof_path: Path | None = None
    appendfsync: FsyncPolicy = "everysec"
    max_keys: int | None = None
    eviction: EvictionPolicy = "noeviction"
    hz: int = 10  # сколько раз в секунду запускаются фоновые задачи
    max_clients: int = 10_000


class MiniKVServer:
    def __init__(self, config: Config | None = None, clock: Callable[[], float] = time.time):
        self.config = config or Config()
        self.keyspace = Keyspace(
            clock=clock, max_keys=self.config.max_keys, eviction=self.config.eviction
        )
        self.executor = Executor(self.keyspace, info_provider=self._info)
        self.aof: AppendOnlyFile | None = None
        self.clients: dict[int, ClientState] = {}
        self._writers: dict[int, asyncio.StreamWriter] = {}
        self.started_at = time.time()
        self.total_connections = 0
        self._server: asyncio.Server | None = None
        self._cron_task: asyncio.Task[None] | None = None

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.sockets[0].getsockname()[1]

    async def start(self) -> int:
        if self.config.aof_path is not None:
            self.aof = AppendOnlyFile(self.config.aof_path, self.config.appendfsync)
            loaded = self.aof.load(self.executor)  # проигрываем журнал до приёма клиентов
            log.info("AOF: replayed %d commands", loaded)
            self.aof.open()
            self.executor.aof = self.aof
            self.executor.rewrite_aof = lambda: self.aof.rewrite(self.keyspace)  # type: ignore[union-attr]
        self._server = await asyncio.start_server(self._handle, self.config.host, self.config.port)
        self._cron_task = asyncio.create_task(self._cron())
        log.info("minikv %s listening on %s:%d", __version__, self.config.host, self.port)
        return self.port

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._cron_task is not None:
            self._cron_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cron_task
        if self._server is not None:
            self._server.close()
            # С Python 3.12 wait_closed() ждёт закрытия всех соединений, а клиенты
            # (например, пул redis-py) держат их открытыми. Закрываем сами.
            for writer in list(self._writers.values()):
                writer.close()
            await self._server.wait_closed()
        if self.aof is not None:
            self.aof.close()

    async def _cron(self) -> None:
        interval = 1 / self.config.hz
        while True:
            await asyncio.sleep(interval)
            self.keyspace.active_expire_cycle()
            if self.aof is not None:
                self.aof.tick()

    # ------------------------------------------------------------------ соединения

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        client = ClientState(addr=f"{peer[0]}:{peer[1]}" if peer else "?")
        if len(self.clients) >= self.config.max_clients:
            writer.write(encode(ErrorReply("ERR max number of clients reached")))
            await writer.drain()
            writer.close()
            return

        def push(message: object) -> None:
            if not writer.is_closing():
                writer.write(encode(message, client.proto))

        client.push = push
        self.clients[client.id] = client
        self._writers[client.id] = writer
        self.total_connections += 1
        parser = Parser(server_mode=True)
        try:
            while data := await reader.read(64 * 1024):
                parser.feed(data)
                try:
                    for argv in parser:
                        if not argv:
                            continue
                        if argv[0].upper() == b"QUIT":
                            writer.write(encode(OK))
                            return
                        self._write_reply(writer, self.executor.execute(client, argv), client)
                except ProtocolError as exc:
                    writer.write(encode(ErrorReply(f"ERR Protocol error: {exc}")))
                    return
                # Все ответы на пачку команд (pipelining) уходят одним drain — это и даёт скорость.
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            self.executor.drop_client(client)
            self.clients.pop(client.id, None)
            self._writers.pop(client.id, None)
            with contextlib.suppress(Exception):
                await writer.drain()
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    @staticmethod
    def _write_reply(writer: asyncio.StreamWriter, reply: object, client: ClientState) -> None:
        if isinstance(reply, MultiReply):
            for item in reply:
                writer.write(encode(item, client.proto))
        else:
            writer.write(encode(reply, client.proto))

    def _info(self) -> dict[str, dict[str, object]]:
        return {
            "Server": {
                "tcp_port": self.port if self._server else self.config.port,
                "uptime_in_seconds": int(time.time() - self.started_at),
                "process_model": "single-threaded asyncio",
            },
            "Clients": {"connected_clients": len(self.clients)},
            "Stats": {"total_connections_received": self.total_connections},
            "Persistence": {
                "aof_enabled": int(self.aof is not None),
                "aof_current_size": self.aof.size if self.aof else 0,
                "aof_rewrites": self.aof.rewrites if self.aof else 0,
            },
        }
