from __future__ import annotations

import redis.asyncio as redis


async def get_redis(url: str) -> "redis.Redis":
    r = redis.from_url(url, decode_responses=True)
    await r.ping()
    return r
