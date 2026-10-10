"""Минимальный асинхронный клиент — для тестов, бенчмарка и CLI."""

from __future__ import annotations

import asyncio
import contextlib
from types import TracebackType

from minikv.resp import ErrorReply, Parser, Push, encode


class ReplyError(Exception):
    """Сервер ответил ошибкой ``-ERR ...``."""


def _arg(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()


class AsyncClient:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._parser = Parser(server_mode=False)
        self._replies: list[object] = []
        self._pushes: list[Push] = []

    @classmethod
    async def connect(cls, host: str = "127.0.0.1", port: int = 6380) -> AsyncClient:
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer)

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        self._writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await self._writer.wait_closed()

    async def _read_one(self) -> object:
        while not self._replies:
            data = await self._reader.read(64 * 1024)
            if not data:
                raise ConnectionError("server closed the connection")
            self._parser.feed(data)
            for value in self._parser:
                # Push — внеочередные сообщения pub/sub в RESP3, их не путаем с ответами.
                (self._pushes if isinstance(value, Push) else self._replies).append(value)
        return self._replies.pop(0)

    async def send(self, *args: object) -> None:
        """Отправить команду, не дожидаясь ответа (например, подписку в RESP3,
        подтверждение которой приходит push-сообщением)."""
        self._writer.write(encode([_arg(a) for a in args]))
        await self._writer.drain()

    async def raw(self, *args: object) -> object:
        """Отправить команду и вернуть ответ как есть (ошибка — тоже значение)."""
        self._writer.write(encode([_arg(a) for a in args]))
        await self._writer.drain()
        return await self._read_one()

    async def execute(self, *args: object) -> object:
        reply = await self.raw(*args)
        if isinstance(reply, ErrorReply):
            raise ReplyError(str(reply))
        return reply

    async def pipeline(self, commands: list[tuple[object, ...]]) -> list[object]:
        """Отправить пачку команд одним пакетом и прочитать все ответы."""
        self._writer.write(b"".join(encode([_arg(a) for a in cmd]) for cmd in commands))
        await self._writer.drain()
        return [await self._read_one() for _ in commands]

    async def read_message(self, timeout: float = 1.0) -> object:
        """Следующее pub/sub-сообщение (для RESP2 это обычный массив в потоке ответов)."""

        async def wait() -> object:
            while not self._pushes and not self._replies:
                data = await self._reader.read(64 * 1024)
                if not data:
                    raise ConnectionError("server closed the connection")
                self._parser.feed(data)
                for value in self._parser:
                    (self._pushes if isinstance(value, Push) else self._replies).append(value)
            return self._pushes.pop(0) if self._pushes else self._replies.pop(0)

        return await asyncio.wait_for(wait(), timeout)
