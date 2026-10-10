"""Интеграционные тесты: настоящий TCP-сервер и клиенты."""

import asyncio

import pytest

from minikv.client import AsyncClient, ReplyError
from minikv.resp import NULL_ARRAY, OK, Push
from minikv.server import Config, MiniKVServer


def serve(coro_fn, **config):
    async def main():
        server = MiniKVServer(Config(port=0, **config))
        port = await server.start()
        try:
            await coro_fn(server, port)
        finally:
            await server.stop()

    asyncio.run(main())


def test_basic_roundtrip():
    async def scenario(server, port):
        async with await AsyncClient.connect(port=port) as c:
            assert await c.execute("PING") == "PONG"
            assert await c.execute("SET", "k", "v") == OK
            assert await c.execute("GET", "k") == b"v"
            with pytest.raises(ReplyError, match="unknown command"):
                await c.execute("NOPE")

    serve(scenario)


def test_pipelining_thousand_commands():
    async def scenario(server, port):
        async with await AsyncClient.connect(port=port) as c:
            replies = await c.pipeline([("INCR", "n")] * 1000)
            assert replies == list(range(1, 1001))

    serve(scenario)


def test_concurrent_increments_are_atomic():
    async def scenario(server, port):
        async def worker():
            async with await AsyncClient.connect(port=port) as c:
                for _ in range(100):
                    await c.execute("INCR", "counter")

        await asyncio.gather(*(worker() for _ in range(20)))
        async with await AsyncClient.connect(port=port) as c:
            assert await c.execute("GET", "counter") == b"2000"

    serve(scenario)


def test_watch_conflict_between_clients():
    async def scenario(server, port):
        async with (
            await AsyncClient.connect(port=port) as a,
            await AsyncClient.connect(port=port) as b,
        ):
            await a.execute("SET", "stock", "10")
            await a.execute("WATCH", "stock")
            await b.execute("DECR", "stock")
            assert await a.pipeline([("MULTI",), ("DECR", "stock"), ("EXEC",)]) == [
                OK,
                "QUEUED",
                NULL_ARRAY,
            ]
            assert await a.execute("GET", "stock") == b"9"

    serve(scenario)


def test_pubsub_resp2_and_resp3():
    async def scenario(server, port):
        async with (
            await AsyncClient.connect(port=port) as sub2,
            await AsyncClient.connect(port=port) as sub3,
            await AsyncClient.connect(port=port) as pub,
        ):
            assert await sub2.execute("SUBSCRIBE", "news") == [b"subscribe", b"news", 1]
            await sub3.execute("HELLO", "3")
            await sub3.send("PSUBSCRIBE", "ne*")
            assert await sub3.read_message() == Push([b"psubscribe", b"ne*", 1])

            assert await pub.execute("PUBLISH", "news", "hi") == 2
            assert await sub2.read_message() == [b"message", b"news", b"hi"]
            msg = await sub3.read_message()
            assert isinstance(msg, Push) and msg == [b"pmessage", b"ne*", b"news", b"hi"]

            # В RESP2 в режиме подписки обычные команды запрещены
            with pytest.raises(ReplyError, match="only"):
                await sub2.execute("GET", "x")
            # В RESP3 можно и подписываться, и выполнять команды
            assert await sub3.execute("SET", "x", "1") == OK

    serve(scenario)


def test_disconnect_cleans_subscriptions():
    async def scenario(server, port):
        c = await AsyncClient.connect(port=port)
        await c.execute("SUBSCRIBE", "ch")
        await c.close()
        await asyncio.sleep(0.05)
        async with await AsyncClient.connect(port=port) as pub:
            assert await pub.execute("PUBLISH", "ch", "x") == 0
        assert not server.executor.pubsub.channels

    serve(scenario)


def test_active_expiry_runs_in_background():
    async def scenario(server, port):
        async with await AsyncClient.connect(port=port) as c:
            await c.pipeline([("SET", f"k{i}", "v", "PX", "50") for i in range(100)])
            await asyncio.sleep(0.4)
            # никто не обращался к ключам, но фоновой цикл их удалил
            assert len(server.keyspace) == 0

    serve(scenario, hz=50)


def test_inline_protocol_and_quit():
    async def scenario(server, port):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"SET a 1\r\nGET a\r\nQUIT\r\n")
        await writer.drain()
        data = await reader.read()  # сервер закрывает соединение после QUIT
        assert data == b"+OK\r\n$1\r\n1\r\n+OK\r\n"
        writer.close()

    serve(scenario)


def test_protocol_error_closes_connection():
    async def scenario(server, port):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"*1\r\n$x\r\n")
        await writer.drain()
        assert (await reader.read()).startswith(b"-ERR Protocol error")
        writer.close()

    serve(scenario)


def test_persistence_across_restarts(tmp_path):
    aof = tmp_path / "data.aof"

    async def write(server, port):
        async with await AsyncClient.connect(port=port) as c:
            await c.execute("SET", "name", "minikv")
            await c.execute("RPUSH", "queue", "a", "b")

    async def read(server, port):
        async with await AsyncClient.connect(port=port) as c:
            assert await c.execute("GET", "name") == b"minikv"
            assert await c.execute("LRANGE", "queue", "0", "-1") == [b"a", b"b"]
            assert "aof_enabled:1" in (await c.execute("INFO")).decode()

    serve(write, aof_path=aof)
    serve(read, aof_path=aof)


def test_info_reports_clients():
    async def scenario(server, port):
        async with await AsyncClient.connect(port=port) as c:
            info = (await c.execute("INFO")).decode()
            assert "connected_clients:1" in info

    serve(scenario)
