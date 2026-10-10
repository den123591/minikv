"""RESP — протокол Redis: кодирование ответов и потоковый разбор входящих данных.

Поддерживаются обе версии протокола:

* RESP2 — классический: ``+OK``, ``-ERR``, ``:1``, ``$3\\r\\nfoo``, ``*2 ...``;
* RESP3 — добавляет типы null ``_``, bool ``#``, double ``,``, map ``%``, set ``~``, push ``>``.

Клиент выбирает версию командой ``HELLO 3``. Значения кодируются по-разному в зависимости
от версии: например, ``Map`` в RESP3 — это ``%``, а в RESP2 — плоский массив ключ/значение.
"""

from __future__ import annotations

from collections.abc import Iterator

CRLF = b"\r\n"


class ProtocolError(Exception):
    """Клиент прислал данные, которые не являются корректным RESP."""


class SimpleString(str):
    """Ответ ``+OK``: короткая строка без переводов строки."""


class ErrorReply(str):
    """Ответ-ошибка ``-ERR ...``. Это значение, а не исключение: оно уходит клиенту."""

    @property
    def code(self) -> str:
        return self.split(" ", 1)[0]


class Map(dict):
    """Словарь: RESP3 ``%``, RESP2 — плоский массив ``[k1, v1, k2, v2]``."""


class Push(list):
    """Внеочередное сообщение (pub/sub): RESP3 ``>``, RESP2 — обычный массив."""


class _NullArray:
    """RESP2 ``*-1`` — «нет массива» (например, EXEC после конфликта WATCH)."""

    _instance: _NullArray | None = None

    def __new__(cls) -> _NullArray:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "NULL_ARRAY"


NULL_ARRAY = _NullArray()
OK = SimpleString("OK")
PONG = SimpleString("PONG")
QUEUED = SimpleString("QUEUED")


# ----------------------------------------------------------------------------- encode


def encode(value: object, proto: int = 2) -> bytes:
    out = bytearray()
    _encode(value, proto, out)
    return bytes(out)


def _encode(v: object, proto: int, out: bytearray) -> None:
    # Порядок проверок важен: bool — подкласс int, SimpleString/ErrorReply — подклассы str.
    if v is None:
        out += b"_\r\n" if proto == 3 else b"$-1\r\n"
    elif v is NULL_ARRAY:
        out += b"_\r\n" if proto == 3 else b"*-1\r\n"
    elif isinstance(v, bool):
        if proto == 3:
            out += b"#t\r\n" if v else b"#f\r\n"
        else:
            out += b":1\r\n" if v else b":0\r\n"
    elif isinstance(v, ErrorReply):
        out += b"-" + _one_line(v) + CRLF
    elif isinstance(v, SimpleString):
        out += b"+" + _one_line(v) + CRLF
    elif isinstance(v, int):
        out += b":%d\r\n" % v
    elif isinstance(v, float):
        text = repr(v).encode()
        if proto == 3:
            out += b"," + text + CRLF
        else:
            out += b"$%d\r\n%s\r\n" % (len(text), text)
    elif isinstance(v, (bytes, bytearray, memoryview)):
        data = bytes(v)
        out += b"$%d\r\n" % len(data)
        out += data + CRLF
    elif isinstance(v, str):
        data = v.encode()
        out += b"$%d\r\n" % len(data)
        out += data + CRLF
    elif isinstance(v, Map):
        out += (b"%%%d\r\n" % len(v)) if proto == 3 else (b"*%d\r\n" % (2 * len(v)))
        for key, item in v.items():
            _encode(key, proto, out)
            _encode(item, proto, out)
    elif isinstance(v, Push):
        out += (b">%d\r\n" if proto == 3 else b"*%d\r\n") % len(v)
        for item in v:
            _encode(item, proto, out)
    elif isinstance(v, (set, frozenset)):
        out += (b"~%d\r\n" if proto == 3 else b"*%d\r\n") % len(v)
        for item in sorted(v):  # детерминированный порядок удобнее для тестов и клиентов
            _encode(item, proto, out)
    elif isinstance(v, (list, tuple)):
        out += b"*%d\r\n" % len(v)
        for item in v:
            _encode(item, proto, out)
    else:
        raise TypeError(f"cannot encode {type(v).__name__} as RESP")


def _one_line(text: str) -> bytes:
    return text.replace("\r", " ").replace("\n", " ").encode()


# ----------------------------------------------------------------------------- parse


class _Incomplete(Exception):
    """Во внутреннем буфере пока нет целого сообщения — ждём следующих байтов."""


class Parser:
    """Потоковый разбор RESP.

    TCP не сохраняет границы сообщений: одна команда может прийти по кусочкам,
    а десять команд — одним пакетом (pipelining). Поэтому парсер копит байты через
    ``feed()`` и отдаёт целые сообщения итерацией, оставляя неполный хвост в буфере.

    ``server_mode=True`` — разбираем команды клиента: массивы bulk-строк или
    inline-команды (``PING\\r\\n``, как из telnet). Иначе разбираем ответы сервера
    любого типа — это нужно клиенту и загрузчику AOF.
    """

    def __init__(
        self,
        *,
        server_mode: bool = True,
        max_bulk: int = 512 * 1024 * 1024,
        max_inline: int = 64 * 1024,
        max_depth: int = 32,
    ) -> None:
        self.server_mode = server_mode
        self.max_bulk = max_bulk
        self.max_inline = max_inline
        self.max_depth = max_depth
        self._buf = bytearray()
        self._pos = 0
        self.consumed = 0  # сколько байтов разобрано за всё время (для AOF)

    def feed(self, data: bytes) -> None:
        self._buf += data

    @property
    def pending(self) -> int:
        """Сколько байтов лежит в буфере без завершённого сообщения."""
        return len(self._buf) - self._pos

    def __iter__(self) -> Iterator[object]:
        while True:
            try:
                value, end = self._parse(self._pos, depth=0)
            except _Incomplete:
                self._compact()
                return
            self.consumed += end - self._pos
            self._pos = end
            if self.server_mode:
                value = self._as_command(value)
            yield value

    def _compact(self) -> None:
        if self._pos == len(self._buf):
            self._buf.clear()
            self._pos = 0
        elif self._pos > 64 * 1024:
            del self._buf[: self._pos]
            self._pos = 0

    def _line(self, pos: int) -> tuple[bytes, int]:
        end = self._buf.find(CRLF, pos)
        if end == -1:
            if len(self._buf) - pos > self.max_inline:
                raise ProtocolError("line too long")
            raise _Incomplete
        return bytes(self._buf[pos:end]), end + 2

    def _int(self, raw: bytes) -> int:
        try:
            return int(raw)
        except ValueError:
            raise ProtocolError(f"invalid integer {raw[:20]!r}") from None

    def _parse(self, pos: int, depth: int) -> tuple[object, int]:
        if pos >= len(self._buf):
            raise _Incomplete
        if depth > self.max_depth:
            raise ProtocolError("nesting too deep")
        kind = self._buf[pos : pos + 1]
        if kind == b"*" or kind == b">" or kind == b"~":
            raw, pos = self._line(pos + 1)
            count = self._int(raw)
            if count < 0:
                return (NULL_ARRAY if not self.server_mode else None), pos
            items = []
            for _ in range(count):
                item, pos = self._parse(pos, depth + 1)
                items.append(item)
            if kind == b">":
                return Push(items), pos
            if kind == b"~":
                return set(items), pos
            return items, pos
        if kind == b"$":
            raw, pos = self._line(pos + 1)
            length = self._int(raw)
            if length < 0:
                return None, pos
            if length > self.max_bulk:
                raise ProtocolError("bulk string too large")
            end = pos + length
            if end + 2 > len(self._buf):
                raise _Incomplete
            if self._buf[end : end + 2] != CRLF:
                raise ProtocolError("bulk string not terminated by CRLF")
            return bytes(self._buf[pos:end]), end + 2
        if self.server_mode:
            # Всё, что не массив, — inline-команда: «SET key value\r\n».
            raw, end = self._line(pos)
            return raw.split(), end
        raw, pos = self._line(pos + 1)
        if kind == b"+":
            return SimpleString(raw.decode()), pos
        if kind == b"-":
            return ErrorReply(raw.decode()), pos
        if kind == b":":
            return self._int(raw), pos
        if kind == b"_":
            return None, pos
        if kind == b"#":
            return raw == b"t", pos
        if kind == b",":
            return float(raw), pos
        if kind == b"%":
            result = {}
            for _ in range(self._int(raw)):
                key, pos = self._parse(pos, depth + 1)
                val, pos = self._parse(pos, depth + 1)
                result[key] = val
            return result, pos
        raise ProtocolError(f"unknown type byte {kind!r}")

    @staticmethod
    def _as_command(value: object) -> list[bytes]:
        if value is None:
            return []
        if not isinstance(value, list) or not all(isinstance(a, bytes) for a in value):
            raise ProtocolError("expected array of bulk strings")
        return value
