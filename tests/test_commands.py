"""Команды через Executor, без сети."""

import pytest

from minikv.client_state import ClientState
from minikv.commands import REGISTRY, Executor
from minikv.keyspace import Keyspace
from minikv.resp import NULL_ARRAY, OK, QUEUED, ErrorReply, Map


class FakeAOF:
    def __init__(self) -> None:
        self.entries: list[list[bytes]] = []

    def append(self, argv):
        self.entries.append(argv)


class Clock:
    t = 1_000.0

    def __call__(self):
        return self.t


@pytest.fixture
def env():
    clock = Clock()
    ex = Executor(Keyspace(clock=clock), aof=FakeAOF())
    client = ClientState()

    def run(*args, who=client):
        return ex.execute(who, [a if isinstance(a, bytes) else str(a).encode() for a in args])

    return ex, run, clock


def test_strings(env):
    _, run, _ = env
    assert run("SET", "k", "v") == OK
    assert run("GET", "k") == b"v"
    assert run("APPEND", "k", "123") == 4
    assert run("STRLEN", "k") == 4
    assert run("GET", "missing") is None
    assert run("MSET", "a", "1", "b", "2") == OK
    assert run("MGET", "a", "b", "nope") == [b"1", b"2", None]
    assert run("GETDEL", "a") == b"1" and run("EXISTS", "a") == 0


def test_set_options(env):
    _, run, _ = env
    assert run("SET", "k", "1", "NX") == OK
    assert run("SET", "k", "2", "NX") is None
    assert run("SET", "k", "3", "XX", "GET") == b"1"
    assert run("SET", "new", "x", "XX") is None
    assert run("SET", "k", "v", "EX", "10") == OK and run("TTL", "k") == 10
    assert run("SET", "k", "v2", "KEEPTTL") == OK and run("TTL", "k") == 10
    assert run("SET", "k", "v", "NX", "XX").startswith("ERR syntax")
    assert run("SET", "k", "v", "EX", "0").startswith("ERR invalid expire")
    assert run("SET", "k", "v", "BOGUS").startswith("ERR syntax")


def test_incr_family(env):
    _, run, _ = env
    assert run("INCR", "n") == 1
    assert run("INCRBY", "n", "41") == 42
    assert run("DECR", "n") == 41
    assert run("DECRBY", "n", "40") == 1
    run("SET", "s", "abc")
    assert run("INCR", "s").startswith("ERR value is not an integer")
    run("SET", "big", str(2**63 - 1))
    assert "overflow" in run("INCR", "big")


def test_incr_keeps_ttl(env):
    _, run, _ = env
    run("SET", "n", "1", "EX", "100")
    run("INCR", "n")
    assert run("TTL", "n") == 100


def test_expire_ttl_persist(env):
    _, run, clock = env
    run("SET", "k", "v")
    assert run("TTL", "k") == -1
    assert run("EXPIRE", "k", "5") == 1
    assert run("PTTL", "k") == 5000
    clock.t += 2
    assert run("TTL", "k") == 3
    assert run("PERSIST", "k") == 1 and run("TTL", "k") == -1
    assert run("EXPIRE", "missing", "5") == 0
    assert run("TTL", "missing") == -2
    run("EXPIRE", "k", "-1")
    assert run("EXISTS", "k") == 0


def test_lists(env):
    _, run, _ = env
    assert run("RPUSH", "l", "a", "b", "c") == 3
    assert run("LPUSH", "l", "x", "y") == 5
    assert run("LRANGE", "l", "0", "-1") == [b"y", b"x", b"a", b"b", b"c"]
    assert run("LRANGE", "l", "-2", "100") == [b"b", b"c"]
    assert run("LRANGE", "l", "3", "1") == []
    assert run("LINDEX", "l", "-1") == b"c" and run("LINDEX", "l", "99") is None
    assert run("LPOP", "l") == b"y"
    assert run("RPOP", "l", "2") == [b"c", b"b"]
    assert run("LLEN", "l") == 2
    run("LPOP", "l", "10")
    assert run("EXISTS", "l") == 0  # пустой список удаляется
    assert run("LPOP", "l") is None and run("LPOP", "l", "2") is NULL_ARRAY


def test_hashes(env):
    _, run, _ = env
    assert run("HSET", "h", "a", "1", "b", "2") == 2
    assert run("HSET", "h", "a", "10") == 0
    assert run("HGET", "h", "a") == b"10"
    assert run("HGETALL", "h") == Map({b"a": b"10", b"b": b"2"})
    assert run("HINCRBY", "h", "b", "5") == 7
    assert run("HKEYS", "h") == [b"a", b"b"] and run("HLEN", "h") == 2
    assert run("HEXISTS", "h", "a") == 1
    assert run("HDEL", "h", "a", "b", "zzz") == 2
    assert run("EXISTS", "h") == 0
    assert run("HSET", "h", "odd").startswith("ERR wrong number")


def test_sets(env):
    _, run, _ = env
    assert run("SADD", "s", "a", "b", "a") == 2
    assert run("SMEMBERS", "s") == {b"a", b"b"}
    assert run("SISMEMBER", "s", "a") == 1 and run("SCARD", "s") == 2
    assert run("SREM", "s", "a", "b") == 2 and run("EXISTS", "s") == 0


def test_wrongtype(env):
    _, run, _ = env
    run("SET", "k", "v")
    assert run("LPUSH", "k", "x").startswith("WRONGTYPE")
    assert run("HGET", "k", "f").startswith("WRONGTYPE")
    run("RPUSH", "l", "x")
    assert run("GET", "l").startswith("WRONGTYPE")


def test_keys_type_rename_del(env):
    _, run, _ = env
    run("SET", "user:1", "a")
    run("RPUSH", "user:2", "b")
    assert sorted(run("KEYS", "user:*")) == [b"user:1", b"user:2"]
    assert run("TYPE", "user:2") == "list" and run("TYPE", "nope") == "none"
    assert run("RENAME", "user:1", "u") == OK
    assert run("RENAME", "nope", "x").startswith("ERR no such key")
    assert run("DEL", "u", "user:2", "nope") == 2
    assert run("DBSIZE") == 0


def test_unknown_and_arity_errors(env):
    _, run, _ = env
    err = run("FOO", "a", "b")
    assert isinstance(err, ErrorReply) and err.startswith("ERR unknown command 'FOO'")
    assert run("GET") == "ERR wrong number of arguments for 'get' command"
    assert run("get", "k") is None  # имена команд регистронезависимы


def test_hello_switches_protocol(env):
    ex, _, _ = env
    client = ClientState()
    reply = ex.execute(client, [b"HELLO", b"3"])
    assert reply["proto"] == 3 and client.proto == 3
    assert ex.execute(client, [b"HELLO", b"4"]).startswith("NOPROTO")


def test_multi_exec(env):
    _, run, _ = env
    assert run("MULTI") == OK
    assert run("SET", "a", "1") == QUEUED
    assert run("INCR", "a") == QUEUED
    assert run("EXEC") == [OK, 2]
    assert run("EXEC").startswith("ERR EXEC without MULTI")


def test_exec_abort_on_queue_error(env):
    _, run, _ = env
    run("MULTI")
    run("SET", "a", "1")
    run("GET")  # ошибка арности при постановке в очередь
    assert run("EXEC").startswith("EXECABORT")
    assert run("GET", "a") is None


def test_runtime_error_inside_exec_does_not_rollback(env):
    _, run, _ = env
    run("SET", "s", "text")
    run("MULTI")
    run("SET", "a", "1")
    run("INCR", "s")
    replies = run("EXEC")
    assert replies[0] == OK and replies[1].startswith("ERR")
    assert run("GET", "a") == b"1"  # как в Redis: нет отката, остальные команды выполнены


def test_watch_aborts_exec_when_key_changed(env):
    ex, run, _ = env
    other = ClientState()
    run("SET", "balance", "100")
    assert run("WATCH", "balance") == OK
    ex.execute(other, [b"INCRBY", b"balance", b"5"])  # конкурент меняет ключ
    run("MULTI")
    run("DECRBY", "balance", "30")
    assert run("EXEC") is NULL_ARRAY
    assert run("GET", "balance") == b"105"


def test_watch_allows_exec_when_untouched(env):
    _, run, _ = env
    run("SET", "balance", "100")
    run("WATCH", "balance")
    run("MULTI")
    run("DECRBY", "balance", "30")
    assert run("EXEC") == [70]


def test_watch_inside_multi_and_discard(env):
    _, run, _ = env
    run("MULTI")
    assert run("WATCH", "x").startswith("ERR WATCH inside MULTI")
    assert run("DISCARD") == OK
    assert run("DISCARD").startswith("ERR DISCARD without MULTI")


def test_aof_propagation_uses_absolute_time(env):
    ex, run, _ = env
    run("SET", "k", "v", "EX", "10")
    run("EXPIRE", "k", "20")
    run("GET", "k")  # чтение не пишется
    run("SET", "n", "1", "NX")
    run("SET", "n", "2", "NX")  # не изменилось — не пишется
    run("EXPIRE", "missing", "5")
    assert ex.aof.entries == [
        [b"SET", b"k", b"v", b"PXAT", b"1010000"],
        [b"PEXPIREAT", b"k", b"1020000"],
        [b"SET", b"n", b"1"],
    ]


def test_aof_wraps_transactions(env):
    ex, run, _ = env
    run("MULTI")
    run("SET", "a", "1")
    run("EXEC")
    assert ex.aof.entries == [[b"MULTI"], [b"SET", b"a", b"1"], [b"EXEC"]]


def test_info_and_command_count(env):
    _, run, _ = env
    run("SET", "a", "1")
    info = run("INFO")
    assert "redis_version:7.4.0" in info and "db0:keys=1" in info
    assert run("COMMAND", "COUNT") == len(REGISTRY)
