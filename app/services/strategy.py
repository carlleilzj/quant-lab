from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from loguru import logger

from app.core.bus import Bus
from app.core.config import settings
from app.core.logging import setup_logging
from app.core.metrics import EVENTS_IN, EVENTS_OUT, HEARTBEAT_TS, start_metrics_server
from app.core.redis import get_redis
from app.schemas.events import MarketTickEvent, OrderIntent, OrderUpdate
from app.strategies.grid import GridStrategy
from app.strategies.pairs import PairsStrategy
from app.strategies.cash_carry import CashCarryStrategy


def load_strategies() -> List[Any]:
    enabled = set(settings.enabled_strategies)
    out: List[Any] = []

    if "grid" in enabled:
        for inst in settings.grid_instruments:
            out.append(
                GridStrategy(
                    inst_id=inst,
                    levels=settings.GRID_LEVELS,
                    spread_bps=settings.GRID_SPREAD_BPS,
                    order_usdt=settings.GRID_ORDER_USDT,
                    rebal_bps=settings.GRID_REBALANCE_BPS,
                    td_mode="cash" if not inst.endswith("SWAP") else "cross",
                )
            )

    if "pairs" in enabled:
        out.append(
            PairsStrategy(
                inst_a=settings.PAIRS_A,
                inst_b=settings.PAIRS_B,
                lookback=settings.PAIRS_LOOKBACK,
                entry_z=settings.PAIRS_ENTRY_Z,
                exit_z=settings.PAIRS_EXIT_Z,
                leg_usdt=settings.PAIRS_LEG_USDT,
                td_mode=settings.PAIRS_TD_MODE,
            )
        )

    if "cash_carry" in enabled:
        out.append(
            CashCarryStrategy(
                spot=settings.CARRY_SPOT,
                fut=settings.CARRY_FUT,
                leg_usdt=settings.CARRY_LEG_USDT,
                min_basis_bps=settings.CARRY_MIN_BASIS_BPS,
                exit_basis_bps=settings.CARRY_EXIT_BASIS_BPS,
                td_mode=settings.CARRY_TD_MODE,
            )
        )

    return out


async def main():
    log = setup_logging(settings.LOG_LEVEL, service=settings.SERVICE_NAME)
    start_metrics_server(settings.METRICS_PORT, settings.SERVICE_NAME)

    bus = Bus(settings.NATS_URL, service=settings.SERVICE_NAME)
    await bus.connect()

    r = await get_redis(settings.REDIS_URL)

    strategies = load_strategies()
    logger.info("Enabled strategies: {}", [s.strategy_id for s in strategies])

    # Subscribe to market data
    async def on_tick(subject: str, payload: dict[str, Any]):
        EVENTS_IN.labels(service=settings.SERVICE_NAME, subject=subject).inc()
        HEARTBEAT_TS.labels(service=settings.SERVICE_NAME).set(int(datetime.now(timezone.utc).timestamp()))

        try:
            tick = MarketTickEvent(**payload)
        except Exception as e:
            logger.warning("bad tick payload: {} {}", e, payload)
            return

        intents: List[OrderIntent] = []
        for s in strategies:
            try:
                # strategy may ignore unrelated inst_id internally
                intents.extend(s.on_tick(tick))
            except Exception as e:
                logger.exception("strategy error: {}", e)

        for intent in intents:
            subj_out = "signal.order_intent"
            await bus.publish(subj_out, intent.model_dump())
            EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject=subj_out).inc()

    async def on_order_update(subject: str, payload: dict[str, Any]):
        EVENTS_IN.labels(service=settings.SERVICE_NAME, subject=subject).inc()
        try:
            upd = OrderUpdate(**payload)
        except Exception:
            return
        # let strategies react to fills (e.g., grid rolling)
        intents: List[OrderIntent] = []
        for s in strategies:
            if hasattr(s, "on_order_update"):
                try:
                    intents.extend(s.on_order_update(upd))
                except Exception:
                    logger.exception("on_order_update error")

        for intent in intents:
            subj_out = "signal.order_intent"
            await bus.publish(subj_out, intent.model_dump())
            EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject=subj_out).inc()

    # Wildcard subscribe: md.ticker.<instId>
    await bus.subscribe("md.ticker.*", on_tick, queue="strategy")

    # Strategy reacts to fills/updates (published by portfolio / dryrun execution)
    await bus.subscribe("portfolio.order_update", on_order_update, queue="strategy")

    async def on_ai_params(subject: str, payload: dict[str, Any]):
        # subject: ai.grid_params.<instId>
        try:
            inst_id = payload.get("inst_id")
            spread = float(payload.get("grid_spread_bps"))
        except Exception:
            return
        for s in strategies:
            if getattr(s, "strategy_id", "") == "grid" and getattr(s, "inst_id", None) == inst_id:
                s.spread_bps = spread
                logger.info("grid params updated for {} -> spread_bps={}", inst_id, spread)

    await bus.subscribe("ai.grid_params.*", on_ai_params, queue="strategy")

    # run forever
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
