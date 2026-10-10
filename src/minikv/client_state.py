from __future__ import annotations

import itertools
import time
from collections.abc import Callable
from dataclasses import dataclass, field

_ids = itertools.count(1)


@dataclass(eq=False)  # сравнение по identity: клиент кладётся в множества подписчиков
class ClientState:
    id: int = field(default_factory=lambda: next(_ids))
    addr: str = ""
    proto: int = 2
    name: bytes | None = None
    created_at: float = field(default_factory=time.time)

    # MULTI / EXEC / WATCH
    in_multi: bool = False
    multi_error: bool = False
    queued: list[list[bytes]] = field(default_factory=list)
    watched: set[bytes] = field(default_factory=set)
    dirty_cas: bool = False

    # Pub/Sub
    channels: set[bytes] = field(default_factory=set)
    patterns: set[bytes] = field(default_factory=set)
    push: Callable[[object], None] | None = None  # доставка сообщений вне очереди ответов

    @property
    def subscriptions(self) -> int:
        return len(self.channels) + len(self.patterns)
