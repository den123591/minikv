import pytest

from minikv.aof import AppendOnlyFile
from minikv.client_state import ClientState
from minikv.commands import Executor
from minikv.keyspace import Keyspace
from minikv.resp import encode


class Clock:
    t = 1_000.0

    def __call__(self):
        return self.t


def make(path, clock=None):
    clock = clock or Clock()
    ks = Keyspace(clock=clock)
    ex = Executor(ks)
    aof = AppendOnlyFile(path, fsync="always")
    loaded = aof.load(ex)
    aof.open()
    ex.aof = aof
    return ks, ex, aof, loaded


def run(ex, *args):
    return ex.execute(ClientState(), [str(a).encode() for a in args])


def populate(ex):
    run(ex, "SET", "s", "hello")
    run(ex, "RPUSH", "l", "a", "b", "c")
    run(ex, "HSET", "h", "f", "1")
    run(ex, "SADD", "set", "x", "y")
    run(ex, "SET", "ttl", "v", "EX", "100")
    run(ex, "INCR", "n")
    run(ex, "INCR", "n")
    run(ex, "LPOP", "l")
    run(ex, "DEL", "s")


def state(ks):
    return {k: (v, ks.ttl_ms(k)) for k, v, _ in ks.snapshot()}


def test_restart_restores_state(tmp_path):
    path = tmp_path / "db.aof"
    ks1, ex1, aof1, _ = make(path)
    populate(ex1)
    aof1.close()

    ks2, _, aof2, loaded = make(path)
    assert loaded == 9
    assert state(ks2) == state(ks1)
    aof2.close()


def test_ttl_survives_restart_without_resetting(tmp_path):
    path = tmp_path / "db.aof"
    clock = Clock()
    _, ex1, aof1, _ = make(path, clock)
    run(ex1, "SET", "k", "v", "EX", "10")
    aof1.close()

    clock.t += 7  # сервер был выключен 7 секунд
    ks2, _, aof2, _ = make(path, clock)
    assert ks2.ttl_ms(b"k") == 3000  # не 10 000: в журнале абсолютное время
    aof2.close()

    clock.t += 5
    ks3, _, aof3, _ = make(path, clock)
    assert not ks3.exists(b"k")
    aof3.close()


def test_truncated_tail_is_dropped(tmp_path):
    path = tmp_path / "db.aof"
    good = encode([b"SET", b"a", b"1"]) + encode([b"SET", b"b", b"2"])
    path.write_bytes(good + b"*3\r\n$3\r\nSET\r\n$1\r\nc")  # сбой посреди записи
    ks, _, aof, loaded = make(path)
    assert loaded == 2
    assert sorted(ks.keys()) == [b"a", b"b"]
    assert path.read_bytes() == good
    aof.close()


def test_unfinished_transaction_is_ignored(tmp_path):
    path = tmp_path / "db.aof"
    path.write_bytes(
        encode([b"SET", b"a", b"1"]) + encode([b"MULTI"]) + encode([b"SET", b"b", b"2"])
    )
    ks, _, aof, _ = make(path)
    assert ks.keys() == [b"a"]
    aof.close()


def test_rewrite_compacts_and_preserves_state(tmp_path):
    path = tmp_path / "db.aof"
    ks, ex, aof, _ = make(path)
    for i in range(500):
        run(ex, "SET", "counter", str(i))
    populate(ex)
    before_size, before_state = aof.size, state(ks)

    new_size = aof.rewrite(ks)
    assert new_size < before_size / 10
    run(ex, "SET", "after", "rewrite")  # журнал продолжает писаться в новый файл
    aof.close()

    ks2, _, aof2, _ = make(path)
    expected = before_state | {b"after": (b"rewrite", -1)}
    assert state(ks2) == expected
    aof2.close()


def test_append_requires_open(tmp_path):
    with pytest.raises(RuntimeError):
        AppendOnlyFile(tmp_path / "x.aof").append([b"PING"])


def test_context_manager_closes(tmp_path):
    with AppendOnlyFile(tmp_path / "x.aof") as aof:
        aof.append([b"SET", b"a", b"1"])
    assert (tmp_path / "x.aof").read_bytes() == encode([b"SET", b"a", b"1"])
