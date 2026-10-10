"""Нагрузочный тест: N клиентов параллельно выполняют SET/GET, с pipelining и без.

python scripts/benchmark.py --clients 50 --requests 2000 --pipeline 1
python scripts/benchmark.py --clients 50 --requests 2000 --pipeline 32
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time

from minikv.client import AsyncClient
from minikv.server import Config, MiniKVServer


async def worker(port: int, n: int, depth: int, wid: int, latencies: list[float]) -> None:
    async with await AsyncClient.connect(port=port) as c:
        for i in range(0, n, depth):
            batch = [
                ("SET", f"key:{wid}:{j}", "x" * 32) if j % 2 == 0 else ("GET", f"key:{wid}:{j - 1}")
                for j in range(i, min(i + depth, n))
            ]
            start = time.perf_counter()
            await c.pipeline(batch)
            latencies.append((time.perf_counter() - start) * 1000)


async def main(clients: int, requests: int, depth: int) -> None:
    server = MiniKVServer(Config(port=0))
    port = await server.start()
    latencies: list[float] = []
    start = time.perf_counter()
    await asyncio.gather(*(worker(port, requests, depth, w, latencies) for w in range(clients)))
    elapsed = time.perf_counter() - start
    await server.stop()

    total = clients * requests
    lat = sorted(latencies)
    p99 = lat[int(len(lat) * 0.99) - 1]
    print(
        f"clients={clients} requests={total} pipeline={depth}: "
        f"{total / elapsed:,.0f} ops/s, "
        f"latency per batch p50={statistics.median(lat):.2f}ms p99={p99:.2f}ms"
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--clients", type=int, default=50)
    p.add_argument("--requests", type=int, default=2000)
    p.add_argument("--pipeline", type=int, default=1)
    a = p.parse_args()
    asyncio.run(main(a.clients, a.requests, a.pipeline))
