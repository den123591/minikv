"""Пространство ключей: значения, TTL, вытеснение LRU и отслеживание изменений для WATCH."""

from __future__ import annotations

import fnmatch
import math
import random
import time
from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Literal, Protocol, TypeVar

from minikv.structures import LRUIndex, RandomSet

Value = bytes | deque | dict | set
T = TypeVar("T")

EvictionPolicy = Literal["noeviction", "allkeys-lru"]


class WrongTypeError(Exception):
    """Команда для одного типа применена к ключу другого типа (например, LPUSH к строке)."""


class OutOfMemoryError(Exception):
    """Достигнут лимит ключей, а политика вытеснения — noeviction."""


class Watcher(Protocol):
    """Клиент, который сделал WATCH: при изменении ключа его транзакция будет отменена."""

    dirty_cas: bool


@dataclass
class KeyspaceStats:
    expired_keys: int = 0
    evicted_keys: int = 0
    keyspace_hits: int = 0
    keyspace_misses: int = 0


TYPE_NAMES: dict[type, str] = {bytes: "string", deque: "list", dict: "hash", set: "set"}


class Keyspace:
    """Однопоточное хранилище — как в Redis: все команды выполняются по очереди,
    поэтому каждая отдельная команда атомарна без всяких блокировок.

    Просроченные ключи удаляются двумя способами (как в Redis):

    * лениво — при обращении к ключу;
    * активно — ``active_expire_cycle()`` по таймеру проверяет случайную выборку
      ключей с TTL и повторяет, пока доля просроченных в выборке больше 25%.
    """

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.time,
        max_keys: int | None = None,
        eviction: EvictionPolicy = "noeviction",
        rng: random.Random | None = None,
    ) -> None:
        self._clock = clock
        self.max_keys = max_keys
        self.eviction = eviction
        self._data: dict[bytes, Value] = {}
        self._expires: dict[bytes, float] = {}  # абсолютное время в мс
        self._ttl_keys: RandomSet[bytes] = RandomSet(rng)
        self._lru: LRUIndex[bytes] = LRUIndex()
        self._watchers: dict[bytes, set[Watcher]] = {}
        self.stats = KeyspaceStats()

    # ------------------------------------------------------------------ время

    def now_ms(self) -> float:
        return self._clock() * 1000

    def _expire_if_needed(self, key: bytes) -> bool:
        when = self._expires.get(key)
        if when is not None and when <= self.now_ms():
            self._remove(key)
            self.stats.expired_keys += 1
            return True
        return False

    # ------------------------------------------------------------------ чтение

    def lookup(self, key: bytes) -> Value | None:
        if self._expire_if_needed(key):
            self.stats.keyspace_misses += 1
            return None
        value = self._data.get(key)
        if value is None:
            self.stats.keyspace_misses += 1
        else:
            self.stats.keyspace_hits += 1
            self._lru.touch(key)
        return value

    def get_typed(self, key: bytes, cls: type[T]) -> T | None:
        value = self.lookup(key)
        if value is not None and not isinstance(value, cls):
            raise WrongTypeError
        return value  # type: ignore[return-value]

    def exists(self, key: bytes) -> bool:
        return not self._expire_if_needed(key) and key in self._data

    def type_of(self, key: bytes) -> str:
        value = None if self._expire_if_needed(key) else self._data.get(key)
        return "none" if value is None else TYPE_NAMES[type(value)]

    def keys(self, pattern: bytes = b"*") -> list[bytes]:
        pat = pattern.decode("latin-1")
        return [
            key
            for key in list(self._data)
            if not self._expire_if_needed(key) and fnmatch.fnmatchcase(key.decode("latin-1"), pat)
        ]

    def __len__(self) -> int:
        return len(self._data)

    @property
    def expires_count(self) -> int:
        return len(self._expires)

    def snapshot(self) -> Iterator[tuple[bytes, Value, float | None]]:
        """Живые ключи со значениями и временем истечения — для перезаписи AOF."""
        for key in list(self._data):
            if not self._expire_if_needed(key):
                yield key, self._data[key], self._expires.get(key)

    # ------------------------------------------------------------------ запись

    def set(
        self,
        key: bytes,
        value: Value,
        *,
        expire_at_ms: float | None = None,
        keep_ttl: bool = False,
    ) -> None:
        self._expire_if_needed(key)
        if key not in self._data:
            self._ensure_capacity()
        self._data[key] = value
        self._lru.touch(key)
        if expire_at_ms is not None:
            self._expires[key] = expire_at_ms
            self._ttl_keys.add(key)
        elif not keep_ttl:
            self._drop_ttl(key)
        self.touch_modified(key)

    def touch_modified(self, key: bytes) -> None:
        """Сообщить, что значение ключа изменилось на месте (например, LPUSH в список)."""
        self._lru.touch(key)
        for watcher in self._watchers.get(key, ()):
            watcher.dirty_cas = True

    def delete(self, key: bytes) -> bool:
        if self._expire_if_needed(key) or key not in self._data:
            return False
        self._remove(key)
        return True

    def _remove(self, key: bytes) -> None:
        self._data.pop(key, None)
        self._drop_ttl(key)
        self._lru.remove(key)
        for watcher in self._watchers.get(key, ()):
            watcher.dirty_cas = True

    def _drop_ttl(self, key: bytes) -> None:
        if self._expires.pop(key, None) is not None:
            self._ttl_keys.discard(key)

    def _ensure_capacity(self) -> None:
        if self.max_keys is None or len(self._data) < self.max_keys:
            return
        if self.eviction == "noeviction":
            raise OutOfMemoryError
        victim = self._lru.oldest()
        if victim is not None:
            self._remove(victim)
            self.stats.evicted_keys += 1

    def expire_at(self, key: bytes, when_ms: float) -> bool:
        if not self.exists(key):
            return False
        if when_ms <= self.now_ms():
            self._remove(key)
        else:
            self._expires[key] = when_ms
            self._ttl_keys.add(key)
            self.touch_modified(key)
        return True

    def persist(self, key: bytes) -> bool:
        if not self.exists(key) or key not in self._expires:
            return False
        self._drop_ttl(key)
        self.touch_modified(key)
        return True

    def ttl_ms(self, key: bytes) -> int:
        """-2 — ключа нет, -1 — ключ без TTL, иначе оставшиеся миллисекунды."""
        if not self.exists(key):
            return -2
        when = self._expires.get(key)
        if when is None:
            return -1
        return max(0, math.ceil(when - self.now_ms()))

    def expire_at_of(self, key: bytes) -> float | None:
        return self._expires.get(key) if self.exists(key) else None

    def rename(self, src: bytes, dst: bytes) -> bool:
        value = self.lookup(src)
        if value is None:
            return False
        when = self._expires.get(src)
        self._remove(src)
        self._expire_if_needed(dst)
        if dst in self._data:
            self._remove(dst)
        self.set(dst, value, expire_at_ms=when)
        return True

    def flush(self) -> None:
        for key in list(self._data):
            self._remove(key)

    # ------------------------------------------------------------------ фоновые задачи

    def active_expire_cycle(self, sample_size: int = 20, max_rounds: int = 16) -> int:
        """Удаляет просроченные ключи из случайных выборок. Возвращает число удалённых."""
        removed = 0
        for _ in range(max_rounds):
            sample = self._ttl_keys.sample(sample_size)
            if not sample:
                break
            expired = sum(self._expire_if_needed(key) for key in sample)
            removed += expired
            if expired * 4 <= len(sample):  # просроченных ≤ 25% — дальше искать невыгодно
                break
        return removed

    # ------------------------------------------------------------------ WATCH

    def watch(self, key: bytes, watcher: Watcher) -> None:
        self._watchers.setdefault(key, set()).add(watcher)

    def unwatch(self, key: bytes, watcher: Watcher) -> None:
        watchers = self._watchers.get(key)
        if watchers is not None:
            watchers.discard(watcher)
            if not watchers:
                del self._watchers[key]
