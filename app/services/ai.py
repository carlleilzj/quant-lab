from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from math import log
from typing import Any, List

import optuna
from loguru import logger
from sqlalchemy import select

from app.core.bus import Bus
from app.core.config import settings
from app.core.logging import setup_logging
from app.core.metrics import EVENTS_OUT, HEARTBEAT_TS, start_metrics_server
from app.core.redis import get_redis
from app.db.models import Base, MarketTick
from app.db.session import SessionLocal, engine
from app.core.utils import utc_ms


def _grid_profit_proxy(prices: List[float], spread_bps: float) -> float:
    """
    一个非常轻量的“收益代理函数”：
    - 统计价格在对数空间穿越网格步长的次数
    - 每穿越一次假设捕获 1 个 spread
    这不是严谨回测，但足够演示“AI 参数调优->写入 Redis->策略读取”链路。
    """
    if len(prices) < 50:
        return 0.0
    step = spread_bps / 10000.0
    if step <= 0:
        return 0.0
    # log grid
    base = log(prices[0])
    step_log = log(1 + step)
    level_prev = int((log(prices[0]) - base) / step_log)
    profit = 0.0
    for p in prices[1:]:
        lvl = int((log(max(p, 1e-9)) - base) / step_log)
        dl = lvl - level_prev
        if dl != 0:
            profit += abs(dl) * step  # proxy
            level_prev = lvl
    return profit


async def ensure_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def fetch_prices(inst_id: str, limit: int = 2000) -> List[float]:
    async with SessionLocal() as sess:
        res = await sess.execute(
            select(MarketTick.last).where(MarketTick.inst_id == inst_id).order_by(MarketTick.ts.desc()).limit(limit)
        )
        rows = res.all()
        prices = [float(x[0]) for x in rows][::-1]
        return prices


async def main():
    log = setup_logging(settings.LOG_LEVEL, service=settings.SERVICE_NAME)
    start_metrics_server(settings.METRICS_PORT, settings.SERVICE_NAME)

    await ensure_db()

    bus = Bus(settings.NATS_URL, service=settings.SERVICE_NAME)
    await bus.connect()

    r = await get_redis(settings.REDIS_URL)

    async def tune_once():
        for inst in settings.grid_instruments:
            prices = await fetch_prices(inst)
            if len(prices) < 200:
                logger.info("ai: not enough data for {} ({} pts)", inst, len(prices))
                continue

            def objective(trial: optuna.Trial) -> float:
                spread_bps = trial.suggest_float("spread_bps", 10.0, 200.0)
                return _grid_profit_proxy(prices, spread_bps)

            study = optuna.create_study(direction="maximize")
            study.optimize(objective, n_trials=25, show_progress_bar=False)

            best = study.best_params
            spread_bps = float(best["spread_bps"])

            # store to redis and publish
            await r.set(f"ai:grid_spread_bps:{inst}", str(spread_bps), ex=3600)

            payload = {"ts_ms": utc_ms(), "inst_id": inst, "grid_spread_bps": spread_bps}
            subj = f"ai.grid_params.{inst}"
            await bus.publish(subj, payload)
            EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject=subj).inc()
            logger.info("ai tuned {} -> spread_bps={}", inst, spread_bps)

    while True:
        HEARTBEAT_TS.labels(service=settings.SERVICE_NAME).set(int(datetime.now(timezone.utc).timestamp()))
        try:
            await tune_once()
        except Exception as e:
            logger.warning("ai tune failed: {}", e)
        await asyncio.sleep(1800)  # 30 min


if __name__ == "__main__":
    asyncio.run(main())
