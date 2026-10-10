import random
from collections import OrderedDict

import pytest

from minikv.structures import LRUIndex, RandomSet


def test_lru_basic_order():
    lru = LRUIndex()
    for key in "abc":
        lru.touch(key)
    lru.touch("a")
    assert list(lru) == ["a", "c", "b"]
    assert lru.oldest() == "b"
    assert lru.pop_oldest() == "b"
    assert lru.remove("c") and not lru.remove("zzz")
    assert list(lru) == ["a"] and len(lru) == 1


def test_lru_empty():
    lru = LRUIndex()
    assert lru.oldest() is None and lru.pop_oldest() is None
    lru.touch(1)
    lru.clear()
    assert list(lru) == []


@pytest.mark.parametrize("seed", range(5))
def test_lru_matches_ordereddict_reference(seed):
    """Дифференциальный тест: на случайных операциях ведём себя как OrderedDict."""
    rng = random.Random(seed)
    lru, ref = LRUIndex(), OrderedDict()
    for _ in range(2000):
        op, key = rng.random(), rng.randrange(30)
        if op < 0.6:
            lru.touch(key)
            ref[key] = None
            ref.move_to_end(key, last=False)
        elif op < 0.8:
            assert lru.remove(key) == (ref.pop(key, 0) is None)
        else:
            expected = next(reversed(ref)) if ref else None
            assert lru.pop_oldest() == expected
            if expected is not None:
                del ref[expected]
        assert list(lru) == list(ref)


def test_random_set_add_discard_sample():
    s = RandomSet(random.Random(0))
    for i in range(100):
        s.add(i)
    s.add(5)  # повтор не дублирует
    assert len(s) == 100
    for i in range(0, 100, 2):
        s.discard(i)
    s.discard(1000)
    assert len(s) == 50 and set(s) == set(range(1, 100, 2))
    sample = s.sample(10)
    assert len(sample) == len(set(sample)) == 10
    assert all(x % 2 == 1 for x in sample)
    assert s.sample(500) and len(s.sample(500)) == 50
