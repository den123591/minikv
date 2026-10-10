"""Команды и их исполнитель.

Каждая команда — обычная функция, зарегистрированная декоратором ``@command``.
Декоратор хранит метаданные (арность, пишет ли команда данные), а ``Executor``
по ним проверяет аргументы, ведёт транзакции и решает, что записать в AOF.
"""

from __future__ import annotations

import itertools
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from minikv import __version__
from minikv.client_state import ClientState
from minikv.keyspace import Keyspace, OutOfMemoryError, WrongTypeError
from minikv.pubsub import PubSub
from minikv.resp import NULL_ARRAY, OK, PONG, QUEUED, ErrorReply, Map, Push, SimpleString

INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1
WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"


class CommandError(Exception):
    """Ошибка, которая уходит клиенту как ``-ERR ...``."""


class MultiReply(list):
    """Несколько ответов на одну команду (SUBSCRIBE a b → два подтверждения)."""


class AppendLog(Protocol):
    def append(self, argv: list[bytes]) -> None: ...


@dataclass
class Context:
    keyspace: Keyspace
    client: ClientState
    executor: Executor
    # Что записать в AOF. None — исходную команду как есть; [] — ничего.
    propagate: list[list[bytes]] | None = None

    def rewrite_as(self, *commands: list[bytes]) -> None:
        self.propagate = list(commands)


Handler = Callable[[Context, list[bytes]], object]


@dataclass(frozen=True)
class CommandSpec:
    name: str
    handler: Handler
    arity: int  # как в Redis: >0 — ровно столько аргументов (с именем), <0 — не меньше |arity|
    write: bool
    flags: frozenset[str] = frozenset()

    def arity_ok(self, argc: int) -> bool:
        return argc == self.arity if self.arity > 0 else argc >= -self.arity


REGISTRY: dict[str, CommandSpec] = {}


def command(
    name: str, arity: int, *, write: bool = False, flags: tuple[str, ...] = ()
) -> Callable[[Handler], Handler]:
    def decorator(fn: Handler) -> Handler:
        REGISTRY[name.upper()] = CommandSpec(name.upper(), fn, arity, write, frozenset(flags))
        return fn

    return decorator


# ============================================================================ helpers


def to_int(raw: bytes, what: str = "value is not an integer or out of range") -> int:
    try:
        value = int(raw)
    except ValueError:
        raise CommandError(f"ERR {what}") from None
    if not INT64_MIN <= value <= INT64_MAX:
        raise CommandError(f"ERR {what}")
    return value


def b(value: int | float | str) -> bytes:
    return str(value).encode()


def _get_list(ctx: Context, key: bytes) -> deque | None:
    return ctx.keyspace.get_typed(key, deque)


def _get_hash(ctx: Context, key: bytes) -> dict | None:
    return ctx.keyspace.get_typed(key, dict)


def _get_set(ctx: Context, key: bytes) -> set | None:
    return ctx.keyspace.get_typed(key, set)


def _get_str(ctx: Context, key: bytes) -> bytes | None:
    return ctx.keyspace.get_typed(key, bytes)


# ============================================================================ connection


@command("PING", -1, flags=("pubsub-ok",))
def cmd_ping(ctx: Context, args: list[bytes]) -> object:
    if len(args) > 1:
        raise CommandError("ERR wrong number of arguments for 'ping' command")
    if ctx.client.subscriptions and ctx.client.proto == 2:
        return [b"pong", args[0] if args else b""]
    return args[0] if args else PONG


@command("ECHO", 2)
def cmd_echo(ctx: Context, args: list[bytes]) -> object:
    return args[0]


@command("HELLO", -1, flags=("pubsub-ok",))
def cmd_hello(ctx: Context, args: list[bytes]) -> object:
    if args:
        proto = to_int(args[0], "Protocol version is not an integer or out of range")
        if proto not in (2, 3):
            return ErrorReply("NOPROTO unsupported protocol version")
        ctx.client.proto = proto
        rest = args[1:]
        while rest:
            opt = rest[0].upper()
            if opt == b"SETNAME" and len(rest) >= 2:
                ctx.client.name = rest[1]
                rest = rest[2:]
            elif opt == b"AUTH" and len(rest) >= 3:
                rest = rest[3:]  # аутентификации нет — принимаем любые данные
            else:
                raise CommandError(f"ERR Syntax error in HELLO option '{opt.decode()}'")
    return Map(
        {
            "server": "minikv",
            "version": __version__,
            "proto": ctx.client.proto,
            "id": ctx.client.id,
            "mode": "standalone",
            "role": "master",
            "modules": [],
        }
    )


@command("SELECT", 2)
def cmd_select(ctx: Context, args: list[bytes]) -> object:
    if to_int(args[0]) != 0:
        raise CommandError("ERR DB index is out of range")
    return OK


@command("CLIENT", -2)
def cmd_client(ctx: Context, args: list[bytes]) -> object:
    sub = args[0].upper()
    if sub == b"SETNAME" and len(args) == 2:
        ctx.client.name = args[1]
        return OK
    if sub == b"GETNAME":
        return ctx.client.name
    if sub == b"ID":
        return ctx.client.id
    if sub == b"SETINFO":
        return OK
    raise CommandError(f"ERR unknown subcommand '{args[0].decode()}'")


@command("COMMAND", -1)
def cmd_command(ctx: Context, args: list[bytes]) -> object:
    if args and args[0].upper() == b"COUNT":
        return len(REGISTRY)
    return []  # redis-cli вызывает COMMAND DOCS при старте; пустой ответ его устраивает


# ============================================================================ server


@command("DBSIZE", 1)
def cmd_dbsize(ctx: Context, args: list[bytes]) -> object:
    return len(ctx.keyspace)


@command("FLUSHDB", -1, write=True)
def cmd_flushdb(ctx: Context, args: list[bytes]) -> object:
    ctx.keyspace.flush()
    return OK


REGISTRY["FLUSHALL"] = CommandSpec("FLUSHALL", cmd_flushdb, -1, True)


@command("TIME", 1)
def cmd_time(ctx: Context, args: list[bytes]) -> object:
    now = time.time()
    return [b(int(now)), b(int((now % 1) * 1_000_000))]


@command("INFO", -1)
def cmd_info(ctx: Context, args: list[bytes]) -> object:
    sections = ctx.executor.info()
    lines: list[str] = []
    for title, values in sections.items():
        lines.append(f"# {title}")
        lines.extend(f"{k}:{v}" for k, v in values.items())
        lines.append("")
    return "\r\n".join(lines)


@command("BGREWRITEAOF", 1)
def cmd_bgrewriteaof(ctx: Context, args: list[bytes]) -> object:
    rewrite = ctx.executor.rewrite_aof
    if rewrite is None:
        raise CommandError("ERR AOF is disabled")
    rewrite()
    return SimpleString("Background append only file rewriting started")


# ============================================================================ keys


@command("DEL", -2, write=True)
def cmd_del(ctx: Context, args: list[bytes]) -> object:
    return sum(ctx.keyspace.delete(key) for key in args)


REGISTRY["UNLINK"] = CommandSpec("UNLINK", cmd_del, -2, True)


@command("EXISTS", -2)
def cmd_exists(ctx: Context, args: list[bytes]) -> object:
    return sum(ctx.keyspace.exists(key) for key in args)


@command("TYPE", 2)
def cmd_type(ctx: Context, args: list[bytes]) -> object:
    return SimpleString(ctx.keyspace.type_of(args[0]))


@command("KEYS", 2)
def cmd_keys(ctx: Context, args: list[bytes]) -> object:
    return ctx.keyspace.keys(args[0])


@command("RENAME", 3, write=True)
def cmd_rename(ctx: Context, args: list[bytes]) -> object:
    if not ctx.keyspace.rename(args[0], args[1]):
        raise CommandError("ERR no such key")
    return OK


def _expire_generic(ctx: Context, key: bytes, when_ms: float) -> int:
    if not ctx.keyspace.exists(key):
        ctx.rewrite_as()
        return 0
    if when_ms <= ctx.keyspace.now_ms():
        ctx.keyspace.delete(key)
        ctx.rewrite_as([b"DEL", key])
    else:
        ctx.keyspace.expire_at(key, when_ms)
        # В AOF пишем абсолютное время: иначе после перезапуска TTL отсчитался бы заново.
        ctx.rewrite_as([b"PEXPIREAT", key, b(int(when_ms))])
    return 1


@command("EXPIRE", 3, write=True)
def cmd_expire(ctx: Context, args: list[bytes]) -> object:
    return _expire_generic(ctx, args[0], ctx.keyspace.now_ms() + to_int(args[1]) * 1000)


@command("PEXPIRE", 3, write=True)
def cmd_pexpire(ctx: Context, args: list[bytes]) -> object:
    return _expire_generic(ctx, args[0], ctx.keyspace.now_ms() + to_int(args[1]))


@command("EXPIREAT", 3, write=True)
def cmd_expireat(ctx: Context, args: list[bytes]) -> object:
    return _expire_generic(ctx, args[0], to_int(args[1]) * 1000)


@command("PEXPIREAT", 3, write=True)
def cmd_pexpireat(ctx: Context, args: list[bytes]) -> object:
    return _expire_generic(ctx, args[0], to_int(args[1]))


@command("TTL", 2)
def cmd_ttl(ctx: Context, args: list[bytes]) -> object:
    ms = ctx.keyspace.ttl_ms(args[0])
    return ms if ms < 0 else (ms + 500) // 1000


@command("PTTL", 2)
def cmd_pttl(ctx: Context, args: list[bytes]) -> object:
    return ctx.keyspace.ttl_ms(args[0])


@command("PERSIST", 2, write=True)
def cmd_persist(ctx: Context, args: list[bytes]) -> object:
    return int(ctx.keyspace.persist(args[0]))


# ============================================================================ strings


@command("GET", 2)
def cmd_get(ctx: Context, args: list[bytes]) -> object:
    return _get_str(ctx, args[0])


@command("SET", -3, write=True)
def cmd_set(ctx: Context, args: list[bytes]) -> object:
    key, value, opts = args[0], args[1], args[2:]
    nx = xx = get = keep_ttl = False
    expire_at: float | None = None
    i = 0
    while i < len(opts):
        opt = opts[i].upper()
        if opt == b"NX":
            nx = True
        elif opt == b"XX":
            xx = True
        elif opt == b"GET":
            get = True
        elif opt == b"KEEPTTL":
            keep_ttl = True
        elif opt in (b"EX", b"PX", b"EXAT", b"PXAT") and i + 1 < len(opts):
            if expire_at is not None:
                raise CommandError("ERR syntax error")
            amount = to_int(opts[i + 1])
            if amount <= 0:
                raise CommandError("ERR invalid expire time in 'set' command")
            now = ctx.keyspace.now_ms()
            expire_at = {
                b"EX": now + amount * 1000,
                b"PX": now + amount,
                b"EXAT": amount * 1000,
                b"PXAT": amount,
            }[opt]
            i += 1
        else:
            raise CommandError("ERR syntax error")
        i += 1
    if (nx and xx) or (keep_ttl and expire_at is not None):
        raise CommandError("ERR syntax error")

    old = _get_str(ctx, key) if get else None
    exists = ctx.keyspace.exists(key)
    if (nx and exists) or (xx and not exists):
        ctx.rewrite_as()
        return old if get else None

    ctx.keyspace.set(key, value, expire_at_ms=expire_at, keep_ttl=keep_ttl)
    propagated = [b"SET", key, value]
    if expire_at is not None:
        propagated += [b"PXAT", b(int(expire_at))]
    elif keep_ttl:
        propagated.append(b"KEEPTTL")
    ctx.rewrite_as(propagated)
    return old if get else OK


@command("SETNX", 3, write=True)
def cmd_setnx(ctx: Context, args: list[bytes]) -> object:
    if ctx.keyspace.exists(args[0]):
        ctx.rewrite_as()
        return 0
    ctx.keyspace.set(args[0], args[1])
    return 1


@command("GETDEL", 2, write=True)
def cmd_getdel(ctx: Context, args: list[bytes]) -> object:
    value = _get_str(ctx, args[0])
    if value is not None:
        ctx.keyspace.delete(args[0])
    return value


@command("MGET", -2)
def cmd_mget(ctx: Context, args: list[bytes]) -> object:
    result = []
    for key in args:
        value = ctx.keyspace.lookup(key)
        result.append(value if isinstance(value, bytes) else None)
    return result


@command("MSET", -3, write=True)
def cmd_mset(ctx: Context, args: list[bytes]) -> object:
    if len(args) % 2:
        raise CommandError("ERR wrong number of arguments for 'mset' command")
    for key, value in zip(args[::2], args[1::2], strict=True):
        ctx.keyspace.set(key, value)
    return OK


def _incr_by(ctx: Context, key: bytes, delta: int) -> int:
    raw = _get_str(ctx, key)
    current = 0 if raw is None else to_int(raw)
    result = current + delta
    if not INT64_MIN <= result <= INT64_MAX:
        raise CommandError("ERR increment or decrement would overflow")
    ctx.keyspace.set(key, b(result), keep_ttl=True)  # INCR не сбрасывает TTL
    return result


@command("INCR", 2, write=True)
def cmd_incr(ctx: Context, args: list[bytes]) -> object:
    return _incr_by(ctx, args[0], 1)


@command("DECR", 2, write=True)
def cmd_decr(ctx: Context, args: list[bytes]) -> object:
    return _incr_by(ctx, args[0], -1)


@command("INCRBY", 3, write=True)
def cmd_incrby(ctx: Context, args: list[bytes]) -> object:
    return _incr_by(ctx, args[0], to_int(args[1]))


@command("DECRBY", 3, write=True)
def cmd_decrby(ctx: Context, args: list[bytes]) -> object:
    return _incr_by(ctx, args[0], -to_int(args[1]))


@command("APPEND", 3, write=True)
def cmd_append(ctx: Context, args: list[bytes]) -> object:
    current = _get_str(ctx, args[0]) or b""
    value = current + args[1]
    ctx.keyspace.set(args[0], value, keep_ttl=True)
    return len(value)


@command("STRLEN", 2)
def cmd_strlen(ctx: Context, args: list[bytes]) -> object:
    return len(_get_str(ctx, args[0]) or b"")


# ============================================================================ lists


def _push(ctx: Context, args: list[bytes], left: bool) -> int:
    key = args[0]
    items = _get_list(ctx, key)
    if items is None:
        items = deque()
        ctx.keyspace.set(key, items)
    if left:
        items.extendleft(args[1:])  # LPUSH k a b → [b, a]
    else:
        items.extend(args[1:])
    ctx.keyspace.touch_modified(key)
    return len(items)


@command("LPUSH", -3, write=True)
def cmd_lpush(ctx: Context, args: list[bytes]) -> object:
    return _push(ctx, args, left=True)


@command("RPUSH", -3, write=True)
def cmd_rpush(ctx: Context, args: list[bytes]) -> object:
    return _push(ctx, args, left=False)


def _pop(ctx: Context, args: list[bytes], left: bool) -> object:
    if len(args) > 2:
        raise CommandError("ERR syntax error")
    key = args[0]
    count = None
    if len(args) == 2:
        count = to_int(args[1], "value is out of range, must be positive")
        if count < 0:
            raise CommandError("ERR value is out of range, must be positive")
    items = _get_list(ctx, key)
    if items is None:
        ctx.rewrite_as()
        return None if count is None else NULL_ARRAY
    take = items.popleft if left else items.pop
    popped = [take() for _ in range(min(count if count is not None else 1, len(items)))]
    if not items:
        ctx.keyspace.delete(key)  # пустой список не хранится, как в Redis
    else:
        ctx.keyspace.touch_modified(key)
    return popped if count is not None else popped[0]


@command("LPOP", -2, write=True)
def cmd_lpop(ctx: Context, args: list[bytes]) -> object:
    return _pop(ctx, args, left=True)


@command("RPOP", -2, write=True)
def cmd_rpop(ctx: Context, args: list[bytes]) -> object:
    return _pop(ctx, args, left=False)


@command("LLEN", 2)
def cmd_llen(ctx: Context, args: list[bytes]) -> object:
    return len(_get_list(ctx, args[0]) or ())


@command("LRANGE", 4)
def cmd_lrange(ctx: Context, args: list[bytes]) -> object:
    items = _get_list(ctx, args[0])
    if items is None:
        return []
    n = len(items)
    start, stop = to_int(args[1]), to_int(args[2])
    if start < 0:
        start += n
    if stop < 0:
        stop += n
    start, stop = max(start, 0), min(stop, n - 1)
    if start > stop:
        return []
    return list(itertools.islice(items, start, stop + 1))


@command("LINDEX", 3)
def cmd_lindex(ctx: Context, args: list[bytes]) -> object:
    items = _get_list(ctx, args[0])
    index = to_int(args[1])
    if items is None or not -len(items) <= index < len(items):
        return None
    return items[index]


# ============================================================================ hashes


@command("HSET", -4, write=True)
def cmd_hset(ctx: Context, args: list[bytes]) -> object:
    key, pairs = args[0], args[1:]
    if len(pairs) % 2:
        raise CommandError("ERR wrong number of arguments for 'hset' command")
    fields = _get_hash(ctx, key)
    if fields is None:
        fields = {}
        ctx.keyspace.set(key, fields)
    added = 0
    for f, v in zip(pairs[::2], pairs[1::2], strict=True):
        added += f not in fields
        fields[f] = v
    ctx.keyspace.touch_modified(key)
    return added


@command("HGET", 3)
def cmd_hget(ctx: Context, args: list[bytes]) -> object:
    return (_get_hash(ctx, args[0]) or {}).get(args[1])


@command("HDEL", -3, write=True)
def cmd_hdel(ctx: Context, args: list[bytes]) -> object:
    fields = _get_hash(ctx, args[0])
    if fields is None:
        return 0
    removed = sum(fields.pop(f, None) is not None for f in args[1:])
    if not fields:
        ctx.keyspace.delete(args[0])
    elif removed:
        ctx.keyspace.touch_modified(args[0])
    return removed


@command("HGETALL", 2)
def cmd_hgetall(ctx: Context, args: list[bytes]) -> object:
    return Map(_get_hash(ctx, args[0]) or {})


@command("HLEN", 2)
def cmd_hlen(ctx: Context, args: list[bytes]) -> object:
    return len(_get_hash(ctx, args[0]) or {})


@command("HEXISTS", 3)
def cmd_hexists(ctx: Context, args: list[bytes]) -> object:
    return int(args[1] in (_get_hash(ctx, args[0]) or {}))


@command("HKEYS", 2)
def cmd_hkeys(ctx: Context, args: list[bytes]) -> object:
    return list((_get_hash(ctx, args[0]) or {}).keys())


@command("HVALS", 2)
def cmd_hvals(ctx: Context, args: list[bytes]) -> object:
    return list((_get_hash(ctx, args[0]) or {}).values())


@command("HINCRBY", 4, write=True)
def cmd_hincrby(ctx: Context, args: list[bytes]) -> object:
    key, f = args[0], args[1]
    fields = _get_hash(ctx, key)
    if fields is None:
        fields = {}
        ctx.keyspace.set(key, fields)
    current = to_int(fields[f], "hash value is not an integer") if f in fields else 0
    result = current + to_int(args[2])
    if not INT64_MIN <= result <= INT64_MAX:
        raise CommandError("ERR increment or decrement would overflow")
    fields[f] = b(result)
    ctx.keyspace.touch_modified(key)
    return result


# ============================================================================ sets


@command("SADD", -3, write=True)
def cmd_sadd(ctx: Context, args: list[bytes]) -> object:
    members = _get_set(ctx, args[0])
    if members is None:
        members = set()
        ctx.keyspace.set(args[0], members)
    before = len(members)
    members.update(args[1:])
    ctx.keyspace.touch_modified(args[0])
    return len(members) - before


@command("SREM", -3, write=True)
def cmd_srem(ctx: Context, args: list[bytes]) -> object:
    members = _get_set(ctx, args[0])
    if members is None:
        return 0
    before = len(members)
    members.difference_update(args[1:])
    if not members:
        ctx.keyspace.delete(args[0])
    elif before != len(members):
        ctx.keyspace.touch_modified(args[0])
    return before - len(members)


@command("SMEMBERS", 2)
def cmd_smembers(ctx: Context, args: list[bytes]) -> object:
    return set(_get_set(ctx, args[0]) or ())


@command("SISMEMBER", 3)
def cmd_sismember(ctx: Context, args: list[bytes]) -> object:
    return int(args[1] in (_get_set(ctx, args[0]) or ()))


@command("SCARD", 2)
def cmd_scard(ctx: Context, args: list[bytes]) -> object:
    return len(_get_set(ctx, args[0]) or ())


# ============================================================================ pub/sub


def _sub_reply(kind: bytes, name: bytes | None, client: ClientState) -> Push:
    return Push([kind, name, client.subscriptions])


@command("SUBSCRIBE", -2, flags=("pubsub-ok",))
def cmd_subscribe(ctx: Context, args: list[bytes]) -> object:
    replies = MultiReply()
    for channel in args:
        ctx.executor.pubsub.subscribe(ctx.client, channel)
        replies.append(_sub_reply(b"subscribe", channel, ctx.client))
    return replies


@command("PSUBSCRIBE", -2, flags=("pubsub-ok",))
def cmd_psubscribe(ctx: Context, args: list[bytes]) -> object:
    replies = MultiReply()
    for pattern in args:
        ctx.executor.pubsub.psubscribe(ctx.client, pattern)
        replies.append(_sub_reply(b"psubscribe", pattern, ctx.client))
    return replies


@command("UNSUBSCRIBE", -1, flags=("pubsub-ok",))
def cmd_unsubscribe(ctx: Context, args: list[bytes]) -> object:
    targets = args or sorted(ctx.client.channels)
    if not targets:
        return MultiReply([_sub_reply(b"unsubscribe", None, ctx.client)])
    replies = MultiReply()
    for channel in targets:
        ctx.executor.pubsub.unsubscribe(ctx.client, channel)
        replies.append(_sub_reply(b"unsubscribe", channel, ctx.client))
    return replies


@command("PUNSUBSCRIBE", -1, flags=("pubsub-ok",))
def cmd_punsubscribe(ctx: Context, args: list[bytes]) -> object:
    targets = args or sorted(ctx.client.patterns)
    if not targets:
        return MultiReply([_sub_reply(b"punsubscribe", None, ctx.client)])
    replies = MultiReply()
    for pattern in targets:
        ctx.executor.pubsub.punsubscribe(ctx.client, pattern)
        replies.append(_sub_reply(b"punsubscribe", pattern, ctx.client))
    return replies


@command("PUBLISH", 3)
def cmd_publish(ctx: Context, args: list[bytes]) -> object:
    return ctx.executor.pubsub.publish(args[0], args[1])


# ============================================================================ executor

TRANSACTION_COMMANDS = {"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"}


@dataclass
class Executor:
    keyspace: Keyspace
    pubsub: PubSub = field(default_factory=PubSub)
    aof: AppendLog | None = None
    rewrite_aof: Callable[[], None] | None = None
    info_provider: Callable[[], dict[str, dict[str, object]]] | None = None
    commands_processed: int = 0

    def info(self) -> dict[str, dict[str, object]]:
        base = {
            "Server": {"redis_version": "7.4.0", "minikv_version": __version__},
            "Stats": {
                "total_commands_processed": self.commands_processed,
                **self.keyspace.stats.__dict__,
            },
            "Keyspace": {
                "db0": f"keys={len(self.keyspace)},expires={self.keyspace.expires_count},avg_ttl=0"
            },
        }
        if self.info_provider:
            for section, values in self.info_provider().items():
                base.setdefault(section, {}).update(values)
        return base

    def execute(self, client: ClientState, argv: list[bytes]) -> object:
        self.commands_processed += 1
        name = argv[0].decode("utf-8", "replace").upper()

        if name in TRANSACTION_COMMANDS:
            return self._transaction_command(client, name, argv[1:])

        spec = REGISTRY.get(name)
        if spec is None:
            preview = " ".join(f"'{a.decode('utf-8', 'replace')}'" for a in argv[1:4])
            error = ErrorReply(
                f"ERR unknown command '{argv[0].decode('utf-8', 'replace')}', "
                f"with args beginning with: {preview}"
            )
            if client.in_multi:
                client.multi_error = True
            return error
        if not spec.arity_ok(len(argv)):
            if client.in_multi:
                client.multi_error = True
            return ErrorReply(f"ERR wrong number of arguments for '{name.lower()}' command")

        if client.in_multi:
            client.queued.append(argv)
            return QUEUED

        if client.proto == 2 and client.subscriptions and "pubsub-ok" not in spec.flags:
            return ErrorReply(
                f"ERR Can't execute '{name.lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE "
                "/ PING / QUIT / RESET are allowed in this context"
            )
        return self._run(spec, client, argv)

    def _run(self, spec: CommandSpec, client: ClientState, argv: list[bytes]) -> object:
        ctx = Context(self.keyspace, client, self)
        try:
            reply = spec.handler(ctx, argv[1:])
        except CommandError as exc:
            return ErrorReply(str(exc))
        except WrongTypeError:
            return ErrorReply(WRONGTYPE)
        except OutOfMemoryError:
            return ErrorReply("OOM command not allowed when used memory > 'maxmemory'.")
        if spec.write and self.aof is not None and not isinstance(reply, ErrorReply):
            for args in ctx.propagate if ctx.propagate is not None else [argv]:
                self.aof.append(args)
        return reply

    # ------------------------------------------------------------------ MULTI / EXEC / WATCH

    def _transaction_command(self, client: ClientState, name: str, args: list[bytes]) -> object:
        if name == "MULTI":
            if client.in_multi:
                return ErrorReply("ERR MULTI calls can not be nested")
            client.in_multi, client.multi_error, client.queued = True, False, []
            return OK
        if name == "DISCARD":
            if not client.in_multi:
                return ErrorReply("ERR DISCARD without MULTI")
            self._reset_multi(client)
            return OK
        if name == "WATCH":
            if client.in_multi:
                return ErrorReply("ERR WATCH inside MULTI is not allowed")
            if not args:
                return ErrorReply("ERR wrong number of arguments for 'watch' command")
            for key in args:
                client.watched.add(key)
                self.keyspace.watch(key, client)
            return OK
        if name == "UNWATCH":
            self._unwatch_all(client)
            return OK
        # EXEC
        if not client.in_multi:
            return ErrorReply("ERR EXEC without MULTI")
        queued, failed, dirty = client.queued, client.multi_error, client.dirty_cas
        self._reset_multi(client)
        if failed:
            return ErrorReply("EXECABORT Transaction discarded because of previous errors.")
        if dirty:
            return NULL_ARRAY  # кто-то изменил наблюдаемый ключ — транзакция не выполняется
        if self.aof is not None and any(REGISTRY[a[0].decode().upper()].write for a in queued):
            self.aof.append([b"MULTI"])
            replies = [self._run(REGISTRY[a[0].decode().upper()], client, a) for a in queued]
            self.aof.append([b"EXEC"])
            return replies
        return [self._run(REGISTRY[a[0].decode().upper()], client, a) for a in queued]

    def _reset_multi(self, client: ClientState) -> None:
        client.in_multi = client.multi_error = False
        client.queued = []
        self._unwatch_all(client)

    def _unwatch_all(self, client: ClientState) -> None:
        for key in client.watched:
            self.keyspace.unwatch(key, client)
        client.watched.clear()
        client.dirty_cas = False

    def drop_client(self, client: ClientState) -> None:
        self._unwatch_all(client)
        self.pubsub.drop_client(client)
