from __future__ import annotations

import json
from typing import Any, Awaitable, Callable

from loguru import logger
from nats.aio.client import Client as NATS

MsgHandler = Callable[[str, dict[str, Any]], Awaitable[None]]


class Bus:
    def __init__(self, url: str, service: str):
        self.url = url
        self.service = service
        self.nc = NATS()

    async def connect(self) -> None:
        await self.nc.connect(
            servers=[self.url],
            name=f"{self.service}",
            reconnect_time_wait=1,
            max_reconnect_attempts=-1,
        )
        logger.info("nats connected: {}", self.url)

    async def publish(self, subject: str, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        await self.nc.publish(subject, data)

    async def subscribe(self, subject: str, handler: MsgHandler, queue: str = "") -> None:
        async def _cb(msg):
            try:
                payload = json.loads(msg.data.decode("utf-8"))
            except Exception:
                payload = {"_raw": msg.data.decode("utf-8", errors="ignore")}
            await handler(msg.subject, payload)

        await self.nc.subscribe(subject, queue=queue, cb=_cb)
        logger.info("subscribed: {} (queue={})", subject, queue)
