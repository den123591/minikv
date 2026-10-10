import random

import pytest

from minikv.keyspace import Keyspace, OutOfMemoryError, WrongTypeError


class Clock:
    def __init__(self) -> None:
        self.t = 1_000.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def ks(clock):
    return Keyspace(clock=clock, rng=random.Random(0))


def test_lazy_expiry(ks, clock):
    ks.set(b"k", b"v", expire_at_ms=ks.now_ms() + 1500)
    assert ks.ttl_ms(b"k") == 1500
    clock.t += 1.0
    assert ks.lookup(b"k") == b"v"
    clock.t += 0.6
    assert ks.lookup(b"k") is None
    assert ks.ttl_ms(b"k") == -2
    assert ks.stats.expired_keys == 1


def test_ttl_states(ks):
    assert ks.ttl_ms(b"missing") == -2
    ks.set(b"k", b"v")
    assert ks.ttl_ms(b"k") == -1
    assert ks.expire_at(b"k", ks.now_ms() + 10_000)
    assert ks.persist(b"k") and ks.ttl_ms(b"k") == -1
    assert not ks.persist(b"k")


def test_set_without_keepttl_resets_ttl(ks):
    ks.set(b"k", b"v", expire_at_ms=ks.now_ms() + 1000)
    ks.set(b"k", b"v2", keep_ttl=True)
    assert ks.ttl_ms(b"k") > 0
    ks.set(b"k", b"v3")
    assert ks.ttl_ms(b"k") == -1


def test_expire_in_past_deletes(ks):
    ks.set(b"k", b"v")
    assert ks.expire_at(b"k", ks.now_ms() - 1)
    assert not ks.exists(b"k")


def test_active_expire_cycle_removes_without_access(ks, clock):
    for i in range(200):
        ks.set(b"tmp%d" % i, b"x", expire_at_ms=ks.now_ms() + 100)
    for i in range(50):
        ks.set(b"keep%d" % i, b"x")
    clock.t += 1
    removed = ks.active_expire_cycle(sample_size=20, max_rounds=100)
    assert removed == 200  # все просроченные — цикл продолжается, пока их доля > 25%
    assert len(ks) == 50


def test_wrong_type(ks):
    ks.set(b"k", b"str")
    with pytest.raises(WrongTypeError):
        ks.get_typed(b"k", dict)


def test_noeviction_raises_when_full(clock):
    ks = Keyspace(clock=clock, max_keys=2)
    ks.set(b"a", b"1")
    ks.set(b"b", b"2")
    ks.set(b"a", b"updated")  # перезапись существующего ключа — можно
    with pytest.raises(OutOfMemoryError):
        ks.set(b"c", b"3")


def test_lru_eviction_keeps_recently_used(clock):
    ks = Keyspace(clock=clock, max_keys=3, eviction="allkeys-lru")
    for key in (b"a", b"b", b"c"):
        ks.set(key, b"x")
    ks.lookup(b"a")  # a стал свежим, самый старый — b
    ks.set(b"d", b"x")
    assert sorted(ks.keys()) == [b"a", b"c", b"d"]
    assert ks.stats.evicted_keys == 1


def test_keys_pattern(ks):
    for key in (b"user:1", b"user:2", b"order:1"):
        ks.set(key, b"x")
    assert sorted(ks.keys(b"user:*")) == [b"user:1", b"user:2"]
    assert ks.keys(b"order:?") == [b"order:1"]


def test_rename_moves_value_and_ttl(ks):
    ks.set(b"a", b"1", expire_at_ms=ks.now_ms() + 5000)
    ks.set(b"b", b"old")
    assert ks.rename(b"a", b"b")
    assert ks.lookup(b"b") == b"1" and ks.ttl_ms(b"b") == 5000
    assert not ks.exists(b"a")
    assert not ks.rename(b"nope", b"x")


class Watcher:
    dirty_cas = False


def test_watchers_marked_dirty_on_any_change(ks, clock):
    watcher = Watcher()
    ks.set(b"k", b"v", expire_at_ms=ks.now_ms() + 100)
    ks.watch(b"k", watcher)
    ks.lookup(b"k")
    assert not watcher.dirty_cas  # чтение не считается изменением
    clock.t += 1
    ks.lookup(b"k")  # истечение TTL — изменение
    assert watcher.dirty_cas
    ks.unwatch(b"k", watcher)
