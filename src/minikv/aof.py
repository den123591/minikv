"""Append-only file: журнал команд записи для восстановления данных после перезапуска.

Каждая успешная команда записи дописывается в конец файла в формате RESP. При старте
файл проигрывается заново. Чтобы журнал не рос бесконечно, ``rewrite()`` записывает
текущее состояние минимальным набором команд во временный файл и атомарно подменяет
им старый (``os.replace``).

Политики fsync (как ``appendfsync`` в Redis):

* ``always``   — fsync после каждой команды: надёжно, но медленно;
* ``everysec`` — fsync раз в секунду: при сбое теряется не больше секунды;
* ``no``       — сбрасывать решает ОС.
"""

from __future__ import annotations

import logging
import os
import time
from collections import deque
from collections.abc import Iterator
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Literal

from minikv.client_state import ClientState
from minikv.commands import Executor
from minikv.keyspace import Keyspace
from minikv.resp import Parser, ProtocolError, encode

log = logging.getLogger(__name__)

FsyncPolicy = Literal["always", "everysec", "no"]
CHUNK = 512  # максимум элементов в одной команде при перезаписи


class AppendOnlyFile:
    def __init__(self, path: Path | str, fsync: FsyncPolicy = "everysec") -> None:
        self.path = Path(path)
        self.fsync_policy = fsync
        self._file: BinaryIO | None = None
        self._last_fsync = time.monotonic()
        self.rewrites = 0

    # ------------------------------------------------------------------ жизненный цикл

    def open(self) -> AppendOnlyFile:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "ab")  # noqa: SIM115 — закрывается в close()
        return self

    def close(self) -> None:
        if self._file is not None:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            self._file = None

    def __enter__(self) -> AppendOnlyFile:
        return self.open()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    @property
    def size(self) -> int:
        return self.path.stat().st_size if self.path.exists() else 0

    # ------------------------------------------------------------------ запись

    def append(self, argv: list[bytes]) -> None:
        if self._file is None:
            raise RuntimeError("AOF is not open")
        self._file.write(encode(argv))
        if self.fsync_policy == "always":
            self._file.flush()
            os.fsync(self._file.fileno())

    def tick(self, now: float | None = None) -> None:
        """Вызывается по таймеру сервера: сбрасывает буфер на диск по политике fsync."""
        if self._file is None:
            return
        now = time.monotonic() if now is None else now
        if self.fsync_policy == "no":
            self._file.flush()
        elif self.fsync_policy == "everysec" and now - self._last_fsync >= 1.0:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._last_fsync = now

    # ------------------------------------------------------------------ загрузка

    def load(self, executor: Executor) -> int:
        """Проигрывает журнал. Если хвост файла оборван (сбой посреди записи),
        обрезает его до последней целой команды — как ``aof-load-truncated yes``."""
        if not self.path.exists():
            return 0
        data = self.path.read_bytes()
        parser = Parser(server_mode=True)
        parser.feed(data)
        loader = ClientState(addr="aof-loader")
        count = 0
        try:
            for argv in parser:
                if argv:
                    executor.execute(loader, argv)
                    count += 1
        except ProtocolError as exc:
            log.warning("AOF corrupted at byte %d: %s", parser.consumed, exc)
        good = parser.consumed
        if loader.in_multi:
            # Оборванная транзакция: MULTI есть, EXEC нет. Её команды не выполнялись.
            log.warning("AOF ends inside MULTI, discarding unfinished transaction")
        if good < len(data):
            log.warning("AOF truncated: dropping %d trailing bytes", len(data) - good)
            with open(self.path, "r+b") as f:
                f.truncate(good)
        return count

    # ------------------------------------------------------------------ перезапись

    def rewrite(self, keyspace: Keyspace) -> int:
        reopen = self._file is not None
        if reopen:
            self.close()
        tmp = self.path.with_name(self.path.name + ".rewrite.tmp")
        with open(tmp, "wb") as f:
            for argv in rewrite_commands(keyspace):
                f.write(encode(argv))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)  # атомарно: читатель видит либо старый, либо новый файл
        self.rewrites += 1
        if reopen:
            self.open()
        return self.size


def rewrite_commands(keyspace: Keyspace) -> Iterator[list[bytes]]:
    """Минимальный набор команд, воссоздающий текущее состояние."""
    for key, value, expire_at in keyspace.snapshot():
        if isinstance(value, bytes):
            yield [b"SET", key, value]
        elif isinstance(value, deque):
            items = list(value)
            for i in range(0, len(items), CHUNK):
                yield [b"RPUSH", key, *items[i : i + CHUNK]]
        elif isinstance(value, dict):
            pairs = [x for kv in value.items() for x in kv]
            for i in range(0, len(pairs), 2 * CHUNK):
                yield [b"HSET", key, *pairs[i : i + 2 * CHUNK]]
        elif isinstance(value, set):
            members = sorted(value)
            for i in range(0, len(members), CHUNK):
                yield [b"SADD", key, *members[i : i + CHUNK]]
        if expire_at is not None:
            yield [b"PEXPIREAT", key, str(int(expire_at)).encode()]
