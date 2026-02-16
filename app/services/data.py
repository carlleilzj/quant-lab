from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict

from loguru import logger

from app.core.config import settings
from app.core.logging import setup_logging
from app.core.metrics import EVENTS_OUT, HEARTBEAT_TS, start_metrics_server
from app.core.bus import Bus
from app.core.redis import get_redis
from app.core.utils import safe_float, utc_ms
from app.db.models import Base, MarketTick
from app.db.session import SessionLocal, engine
from app.exchanges.okx.ws import OkxWsClient
from app.schemas.events import MarketTickEvent


async def ensure_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def main():
    log = setup_logging(settings.LOG_LEVEL, service=settings.SERVICE_NAME)
    start_metrics_server(settings.METRICS_PORT, settings.SERVICE_NAME)

    await ensure_db()

    bus = Bus(settings.NATS_URL, service=settings.SERVICE_NAME)
    await bus.connect()

    r = await get_redis(settings.REDIS_URL)

    ws = OkxWsClient(settings.OKX_WS_PUBLIC_URL, name="okx-public")
    await ws.connect()

    sub_args = [{"channel": "tickers", "instId": inst} for inst in settings.instruments]
    await ws.subscribe(sub_args, req_id="data-1")
    logger.info("Subscribed tickers: {}", settings.instruments)

    last_write_ms: Dict[str, int] = {}

    async for msg in ws.messages():
        HEARTBEAT_TS.labels(service=settings.SERVICE_NAME).set(int(datetime.now(timezone.utc).timestamp()))

        if isinstance(msg, str):
            continue
        if msg.get("event"):
            # subscribe ack / error
            continue

        arg = msg.get("arg") or {}
        if arg.get("channel") != "tickers":
            continue
        inst_id = arg.get("instId")
        if not inst_id:
            continue

        data_list = msg.get("data") or []
        if not data_list:
            continue
        d0 = data_list[0]

        tick = MarketTickEvent(
            ts_ms=int(safe_float(d0.get("ts"), utc_ms()) or utc_ms()),
            inst_id=inst_id,
            last=float(safe_float(d0.get("last"), 0.0) or 0.0),
            bid=safe_float(d0.get("bidPx")),
            ask=safe_float(d0.get("askPx")),
            raw=d0,
        )

        # publish to bus
        subj = f"md.ticker.{inst_id}"
        await bus.publish(subj, tick.model_dump())
        EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject=subj).inc()

        # store latest to redis
        await r.set(f"last_tick:{inst_id}", tick.model_dump_json(), ex=60)

        # throttle DB writes
        now_ms = utc_ms()
        prev_ms = last_write_ms.get(inst_id, 0)
        if now_ms - prev_ms >= settings.MD_DB_WRITE_EVERY_MS:
            last_write_ms[inst_id] = now_ms
            async with SessionLocal() as sess:
                sess.add(
                    MarketTick(
                        inst_id=inst_id,
                        last=tick.last,
                        bid=tick.bid,
                        ask=tick.ask,
                        raw=tick.raw,
                    )
                )
                await sess.commit()


if __name__ == "__main__":
    asyncio.run(main())
