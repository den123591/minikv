"""Совместимость с официальным клиентом redis-py.

Если он работает, значит протокол реализован верно.
"""

import asyncio
import threading
import time

import pytest

from minikv.server import Config, MiniKVServer

redis = pytest.importorskip("redis")


@pytest.fixture(scope="module")
def port():
    """Сервер в отдельном потоке со своим event loop: redis-py — синхронный клиент."""
    ready = threading.Event()
    state: dict = {}

    def run():
        async def main():
            server = MiniKVServer(Config(port=0))
            state["port"] = await server.start()
            state["stop"] = asyncio.Event()
            ready.set()
            await state["stop"].wait()
            await server.stop()

        state["loop"] = asyncio.new_event_loop()
        state["loop"].run_until_complete(main())

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    ready.wait(5)
    yield state["port"]
    state["loop"].call_soon_threadsafe(state["stop"].set)
    thread.join(5)


@pytest.fixture(params=[2, 3], ids=["resp2", "resp3"])
def r(request, port):
    client = redis.Redis(port=port, protocol=request.param, decode_responses=True)
    client.flushdb()
    yield client
    client.close()


def test_strings_and_ttl(r):
    assert r.set("k", "v", ex=30)
    assert r.get("k") == "v"
    assert 29 <= r.ttl("k") <= 30
    assert r.set("k", "v2", nx=True) is None
    assert r.incr("n") == 1 and r.incrby("n", 9) == 10
    assert r.mget("k", "n", "zzz") == ["v", "10", None]


def test_collections(r):
    r.rpush("l", "a", "b", "c")
    assert r.lrange("l", 0, -1) == ["a", "b", "c"]
    assert r.lpop("l") == "a"
    r.hset("h", mapping={"x": "1", "y": "2"})
    assert r.hgetall("h") == {"x": "1", "y": "2"}
    assert r.hincrby("h", "x", 5) == 6
    r.sadd("s", "a", "b")
    assert r.smembers("s") == {"a", "b"}


def test_errors_are_raised_as_exceptions(r):
    r.set("k", "text")
    with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
        r.lpush("k", "x")


def test_pipeline_and_transaction(r):
    with r.pipeline(transaction=True) as pipe:
        pipe.set("a", 1).incr("a").get("a")
        assert pipe.execute() == [True, 2, "2"]


def test_optimistic_locking_with_watch(r, port):
    r.set("balance", 100)
    other = redis.Redis(port=port, decode_responses=True)
    attempts = 0

    def withdraw(pipe):
        nonlocal attempts
        attempts += 1
        balance = int(pipe.get("balance"))
        if attempts == 1:
            other.incrby("balance", 50)  # конкурент вмешался между GET и EXEC
        pipe.multi()
        pipe.set("balance", balance - 30)

    r.transaction(withdraw, "balance")
    assert attempts == 2  # первая попытка отменена WATCH, вторая прошла
    assert r.get("balance") == "120"
    other.close()


def test_pubsub(r, port):
    p = r.pubsub()
    p.subscribe("events")
    assert p.get_message(timeout=1)["type"] == "subscribe"
    publisher = redis.Redis(port=port)
    assert publisher.publish("events", "hello") == 1
    deadline = time.time() + 2
    message = None
    while message is None and time.time() < deadline:
        message = p.get_message(timeout=0.5)
    assert message["type"] == "message" and message["data"] == "hello"
    p.close()
    publisher.close()


def test_info_and_dbsize(r):
    r.set("a", 1)
    assert r.dbsize() == 1
    info = r.info()
    assert info["redis_version"] == "7.4.0"
    assert info["db0"]["keys"] == 1
