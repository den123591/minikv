"""Структуры данных, на которых держится хранилище.

``collections.OrderedDict`` умеет почти то же, что ``LRUIndex``, — здесь двусвязный
список написан вручную, чтобы было видно, откуда берётся O(1). Тесты сравнивают
его поведение с ``OrderedDict`` на случайных последовательностях операций.
"""

from __future__ import annotations

import random
from collections.abc import Hashable, Iterator
from typing import Generic, TypeVar

K = TypeVar("K", bound=Hashable)


class _Node(Generic[K]):
    __slots__ = ("key", "next", "prev")

    def __init__(self, key: K | None) -> None:
        self.key = key
        self.prev: _Node[K] = self
        self.next: _Node[K] = self


class LRUIndex(Generic[K]):
    """Ключи в порядке последнего использования.

    Словарь key → узел даёт поиск за O(1), двусвязный список — перестановку
    и удаление за O(1). Голова списка — самый свежий ключ, хвост — самый старый.
    Используется ``sentinel``-узел, чтобы не проверять края списка на None.
    """

    def __init__(self) -> None:
        self._map: dict[K, _Node[K]] = {}
        self._head: _Node[K] = _Node(None)

    def __len__(self) -> int:
        return len(self._map)

    def __contains__(self, key: object) -> bool:
        return key in self._map

    def __iter__(self) -> Iterator[K]:
        """От самого свежего к самому старому."""
        node = self._head.next
        while node is not self._head:
            yield node.key  # type: ignore[misc]
            node = node.next

    @staticmethod
    def _unlink(node: _Node[K]) -> None:
        node.prev.next = node.next
        node.next.prev = node.prev

    def _push_front(self, node: _Node[K]) -> None:
        node.prev = self._head
        node.next = self._head.next
        self._head.next.prev = node
        self._head.next = node

    def touch(self, key: K) -> None:
        node = self._map.get(key)
        if node is None:
            node = _Node(key)
            self._map[key] = node
        else:
            self._unlink(node)
        self._push_front(node)

    def remove(self, key: K) -> bool:
        node = self._map.pop(key, None)
        if node is None:
            return False
        self._unlink(node)
        return True

    def oldest(self) -> K | None:
        node = self._head.prev
        return None if node is self._head else node.key

    def pop_oldest(self) -> K | None:
        key = self.oldest()
        if key is not None:
            self.remove(key)
        return key

    def clear(self) -> None:
        self._map.clear()
        self._head.next = self._head.prev = self._head


class RandomSet(Generic[K]):
    """Множество с add/discard за O(1) и случайной выборкой за O(k).

    Нужно для активного удаления просроченных ключей: Redis не обходит все ключи
    с TTL, а проверяет случайную выборку. Хранение в списке + словарь индексов,
    удаление — заменой на последний элемент (swap-remove).
    """

    def __init__(self, rng: random.Random | None = None) -> None:
        self._items: list[K] = []
        self._index: dict[K, int] = {}
        self._rng = rng or random.Random()

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, item: object) -> bool:
        return item in self._index

    def __iter__(self) -> Iterator[K]:
        return iter(list(self._items))

    def add(self, item: K) -> None:
        if item not in self._index:
            self._index[item] = len(self._items)
            self._items.append(item)

    def discard(self, item: K) -> None:
        i = self._index.pop(item, None)
        if i is None:
            return
        last = self._items.pop()
        if i < len(self._items):
            self._items[i] = last
            self._index[last] = i

    def sample(self, k: int) -> list[K]:
        return self._rng.sample(self._items, min(k, len(self._items)))

    def clear(self) -> None:
        self._items.clear()
        self._index.clear()
