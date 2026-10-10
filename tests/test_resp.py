import pytest

from minikv.resp import (
    NULL_ARRAY,
    OK,
    ErrorReply,
    Map,
    Parser,
    ProtocolError,
    Push,
    encode,
)


@pytest.mark.parametrize(
    ("value", "proto", "expected"),
    [
        (OK, 2, b"+OK\r\n"),
        (ErrorReply("ERR boom"), 2, b"-ERR boom\r\n"),
        (42, 2, b":42\r\n"),
        (b"hi", 2, b"$2\r\nhi\r\n"),
        (b"", 2, b"$0\r\n\r\n"),
        (None, 2, b"$-1\r\n"),
        (None, 3, b"_\r\n"),
        (NULL_ARRAY, 2, b"*-1\r\n"),
        (True, 2, b":1\r\n"),
        (True, 3, b"#t\r\n"),
        (1.5, 3, b",1.5\r\n"),
        ([b"a", 1], 2, b"*2\r\n$1\r\na\r\n:1\r\n"),
        (Map({b"k": b"v"}), 2, b"*2\r\n$1\r\nk\r\n$1\r\nv\r\n"),
        (Map({b"k": b"v"}), 3, b"%1\r\n$1\r\nk\r\n$1\r\nv\r\n"),
        (Push([b"message"]), 3, b">1\r\n$7\r\nmessage\r\n"),
        ({b"b", b"a"}, 3, b"~2\r\n$1\r\na\r\n$1\r\nb\r\n"),
    ],
)
def test_encode(value, proto, expected):
    assert encode(value, proto) == expected


def test_error_cannot_break_framing():
    assert encode(ErrorReply("bad\r\nline")) == b"-bad  line\r\n"


def test_encode_rejects_unknown_types():
    with pytest.raises(TypeError):
        encode(object())


COMMANDS = [[b"SET", b"key", b"value with spaces"], [b"GET", b"key"], [b"PING"]]
STREAM = b"".join(encode(c) for c in COMMANDS)


def test_parser_handles_pipelined_commands_in_one_packet():
    parser = Parser()
    parser.feed(STREAM)
    assert list(parser) == COMMANDS
    assert parser.pending == 0


@pytest.mark.parametrize("split", range(1, len(STREAM)))
def test_parser_handles_any_split_point(split):
    # TCP может разрезать поток в любом месте — результат должен быть тем же.
    parser = Parser()
    parser.feed(STREAM[:split])
    got = list(parser)
    parser.feed(STREAM[split:])
    got += list(parser)
    assert got == COMMANDS


def test_parser_byte_by_byte():
    parser = Parser()
    got = []
    for i in range(len(STREAM)):
        parser.feed(STREAM[i : i + 1])
        got += list(parser)
    assert got == COMMANDS


def test_binary_safe_bulk_strings():
    payload = b"\x00\r\n\xff"
    parser = Parser()
    parser.feed(encode([b"SET", b"k", payload]))
    assert list(parser) == [[b"SET", b"k", payload]]


def test_inline_commands_like_telnet():
    parser = Parser()
    parser.feed(b"SET a 1\r\nPING\r\n\r\n")
    assert list(parser) == [[b"SET", b"a", b"1"], [b"PING"], []]


@pytest.mark.parametrize(
    "bad",
    [b"*x\r\n", b"*1\r\n$abc\r\n", b"*1\r\n$3\r\nabcXY", b"*1\r\n:5\r\n"],
)
def test_protocol_errors(bad):
    parser = Parser()
    parser.feed(bad)
    with pytest.raises(ProtocolError):
        list(parser)


def test_limits_protect_against_huge_inputs():
    parser = Parser(max_bulk=10)
    parser.feed(b"*1\r\n$11\r\n")
    with pytest.raises(ProtocolError, match="too large"):
        list(parser)

    parser = Parser(max_inline=8)
    parser.feed(b"A" * 20)
    with pytest.raises(ProtocolError, match="too long"):
        list(parser)

    parser = Parser(server_mode=False, max_depth=3)
    parser.feed(b"*1\r\n" * 10 + b":1\r\n")
    with pytest.raises(ProtocolError, match="deep"):
        list(parser)


def test_client_mode_parses_every_reply_type():
    replies = [
        OK,
        ErrorReply("ERR x"),
        7,
        b"bulk",
        None,
        [b"a", [1, 2]],
        Map({b"k": 1}),
        Push([b"message", b"ch", b"hi"]),
        {b"m"},
        True,
        2.5,
    ]
    parser = Parser(server_mode=False)
    parser.feed(b"".join(encode(r, 3) for r in replies))
    got = list(parser)
    assert got[:7] == [OK, ErrorReply("ERR x"), 7, b"bulk", None, [b"a", [1, 2]], {b"k": 1}]
    assert isinstance(got[7], Push) and got[7] == [b"message", b"ch", b"hi"]
    assert got[8:] == [{b"m"}, True, 2.5]
    assert isinstance(got[1], ErrorReply)


def test_consumed_counts_only_complete_messages():
    parser = Parser()
    parser.feed(STREAM + b"*1\r\n$4\r\nPI")
    list(parser)
    assert parser.consumed == len(STREAM)
