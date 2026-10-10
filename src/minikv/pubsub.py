from __future__ import annotations

import fnmatch

from minikv.client_state import ClientState
from minikv.resp import Push


class PubSub:
    """Каналы и шаблоны подписок. Сообщения не хранятся: кто не подписан — не получит."""

    def __init__(self) -> None:
        self.channels: dict[bytes, set[ClientState]] = {}
        self.patterns: dict[bytes, set[ClientState]] = {}

    def subscribe(self, client: ClientState, channel: bytes) -> None:
        self.channels.setdefault(channel, set()).add(client)
        client.channels.add(channel)

    def unsubscribe(self, client: ClientState, channel: bytes) -> None:
        subs = self.channels.get(channel)
        if subs is not None:
            subs.discard(client)
            if not subs:
                del self.channels[channel]
        client.channels.discard(channel)

    def psubscribe(self, client: ClientState, pattern: bytes) -> None:
        self.patterns.setdefault(pattern, set()).add(client)
        client.patterns.add(pattern)

    def punsubscribe(self, client: ClientState, pattern: bytes) -> None:
        subs = self.patterns.get(pattern)
        if subs is not None:
            subs.discard(client)
            if not subs:
                del self.patterns[pattern]
        client.patterns.discard(pattern)

    def drop_client(self, client: ClientState) -> None:
        for channel in list(client.channels):
            self.unsubscribe(client, channel)
        for pattern in list(client.patterns):
            self.punsubscribe(client, pattern)

    def publish(self, channel: bytes, message: bytes) -> int:
        delivered = 0
        for client in list(self.channels.get(channel, ())):
            if client.push is not None:
                client.push(Push([b"message", channel, message]))
                delivered += 1
        text = channel.decode("latin-1")
        for pattern, clients in list(self.patterns.items()):
            if fnmatch.fnmatchcase(text, pattern.decode("latin-1")):
                for client in list(clients):
                    if client.push is not None:
                        client.push(Push([b"pmessage", pattern, channel, message]))
                        delivered += 1
        return delivered
