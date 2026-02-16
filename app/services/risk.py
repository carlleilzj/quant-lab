from __future__ import annotations

import asyncio
import json
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Optional

from loguru import logger

from app.core.bus import Bus
from app.core.config import settings
from app.core.logging import setup_logging
from app.core.metrics import (
    EVENTS_IN,
    EVENTS_OUT,
    HEARTBEAT_TS,
    ORDERS_APPROVED,
    ORDERS_REJECTED,
    start_metrics_server,
)
from app.core.redis import get_redis
from app.core.utils import safe_float, utc_ms
from app.schemas.events import OrderIntent, RiskDecision


def bps_to_frac(bps: float) -> float:
    return bps / 10000.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_derivative(inst_id: str, td_mode: str) -> bool:
    u = inst_id.upper()
    return td_mode != "cash" or u.endswith("-SWAP") or "-FUT" in u or "-OPTION" in u


async def redis_rate_limit(r, key: str, limit: int, ttl_s: int) -> bool:
    """Return True if allowed."""
    try:
        v = await r.incr(key)
        if v == 1:
            await r.expire(key, ttl_s)
        return int(v) <= int(limit)
    except Exception:
        # fail-open (do not block trading if redis hiccups)
        return True


async def get_last_price(r, inst_id: str) -> Optional[float]:
    last_json = await r.get(f"last_tick:{inst_id}")
    if not last_json:
        return None
    try:
        return safe_float(json.loads(last_json).get("last"))
    except Exception:
        return None


async def get_equity_snapshot(r) -> tuple[float, float]:
    """Return (total_eq, avail_eq) in USD/USDT-like units."""
    v = await r.get("equity:latest")
    if v:
        try:
            j = json.loads(v)
            total = safe_float(j.get("total_eq_usd"))
            avail = safe_float(j.get("avail_eq_usd"))
            if total is not None and total > 0:
                return float(total), float(avail if avail is not None else total)
        except Exception:
            pass
    # fallback
    total = float(settings.RISK_FALLBACK_EQUITY_USDT)
    return total, total


async def update_peak_and_drawdown(r, total_eq: float) -> float:
    """Update equity peak & return drawdown pct."""
    peak_key = "equity:peak"
    peak = safe_float(await r.get(peak_key))
    if peak is None or peak <= 0:
        peak = total_eq
        await r.set(peak_key, str(peak))
        return 0.0
    if total_eq > float(peak):
        await r.set(peak_key, str(total_eq))
        return 0.0
    dd = (float(peak) - total_eq) / max(float(peak), 1e-9)
    await r.set("equity:drawdown", str(dd), ex=3600)
    return float(dd)


async def daily_pnl_check(r, total_eq: float) -> float:
    """Return daily pnl (today total - day_start)."""
    day = _utc_now().strftime("%Y%m%d")
    key = f"equity:day_start:{day}"
    v = safe_float(await r.get(key))
    if v is None:
        await r.set(key, str(total_eq), ex=60 * 60 * 36)  # keep ~1.5 days
        v = total_eq
    pnl = total_eq - float(v)
    await r.set("equity:daily_pnl", str(pnl), ex=3600)
    return float(pnl)


async def set_kill_switch(r, state: int, reason: str):
    await r.set("kill_switch", str(int(state)))
    await r.set("kill_switch:reason", reason, ex=3600)
    await r.set("kill_switch:ts_ms", str(utc_ms()), ex=3600)


async def main():
    setup_logging(settings.LOG_LEVEL, service=settings.SERVICE_NAME)
    start_metrics_server(settings.METRICS_PORT, settings.SERVICE_NAME)

    bus = Bus(settings.NATS_URL, service=settings.SERVICE_NAME)
    await bus.connect()

    r = await get_redis(settings.REDIS_URL)

    # initialize kill switch in redis if not present
    if await r.get("kill_switch") is None:
        await r.set("kill_switch", str(int(settings.KILL_SWITCH)))

    # local fallback limiter (per process)
    order_ts: Deque[int] = deque(maxlen=max(1000, settings.RISK_MAX_ORDERS_PER_MIN * 5))

    async def on_intent(subject: str, payload: dict[str, Any]):
        EVENTS_IN.labels(service=settings.SERVICE_NAME, subject=subject).inc()
        HEARTBEAT_TS.labels(service=settings.SERVICE_NAME).set(int(_utc_now().timestamp()))

        try:
            intent = OrderIntent(**payload)
        except Exception as e:
            logger.warning("bad intent: {}", e)
            return

        # cancel is always allowed (even under kill switch)
        if intent.action == "cancel":
            decision = RiskDecision(ts_ms=utc_ms(), approved=True, reason="cancel_ok", intent=intent)
            await bus.publish("order.request", decision.model_dump())
            EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="order.request").inc()
            ORDERS_APPROVED.labels(service=settings.SERVICE_NAME).inc()
            return

        # kill switch
        ks = await r.get("kill_switch")
        if ks and int(ks) == 1:
            decision = RiskDecision(ts_ms=utc_ms(), approved=False, reason="KILL_SWITCH=1", intent=intent)
            await bus.publish("risk.order_rejected", decision.model_dump())
            EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
            ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
            return

        # --- rate limits (redis, cluster-wide) ---
        now_ms = utc_ms()
        minute_bucket = now_ms // 60_000

        if settings.RISK_USE_REDIS_RATE_LIMIT:
            ok = await redis_rate_limit(r, f"rl:global:{minute_bucket}", settings.RISK_RATE_LIMIT_GLOBAL_PER_MIN, ttl_s=120)
            if not ok:
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="rate_limit_global", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return

            ok = await redis_rate_limit(
                r,
                f"rl:strat:{intent.strategy}:{minute_bucket}",
                settings.RISK_RATE_LIMIT_PER_STRATEGY_PER_MIN,
                ttl_s=120,
            )
            if not ok:
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="rate_limit_strategy", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return
        else:
            # local fallback
            one_min_ago = now_ms - 60_000
            while order_ts and order_ts[0] < one_min_ago:
                order_ts.popleft()
            if len(order_ts) >= settings.RISK_MAX_ORDERS_PER_MIN:
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="rate_limit_local", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return

        # --- equity/capital snapshots ---
        total_eq, avail_eq = await get_equity_snapshot(r)

        # drawdown kill
        if settings.RISK_ENABLE_DRAWDOWN_KILL:
            dd = await update_peak_and_drawdown(r, total_eq)
            if dd > float(settings.RISK_MAX_DRAWDOWN_PCT):
                await set_kill_switch(r, 1, f"drawdown>{settings.RISK_MAX_DRAWDOWN_PCT:.3f}")
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="drawdown_kill", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return

        # daily loss kill
        if settings.RISK_ENABLE_DAILY_LOSS_KILL:
            pnl = await daily_pnl_check(r, total_eq)
            if pnl < -float(settings.RISK_MAX_DAILY_LOSS_USDT):
                await set_kill_switch(r, 1, f"daily_loss>{settings.RISK_MAX_DAILY_LOSS_USDT:.2f}")
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="daily_loss_kill", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return

        # --- validate intent basics ---
        if intent.action == "place":
            if intent.sz is None or float(intent.sz) <= 0:
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="bad_size", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return
            if intent.ord_type == "limit" and intent.px is None:
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="limit_missing_px", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return
        elif intent.action == "amend":
            # amend should provide at least one of (new_px/new_sz) or (px/sz)
            new_px = intent.new_px if intent.new_px is not None else intent.px
            new_sz = intent.new_sz if intent.new_sz is not None else intent.sz
            if new_px is None and new_sz is None:
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="amend_missing_params", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return

        # --- price and notional checks (place only) ---
        last = await get_last_price(r, intent.inst_id)
        est_px = intent.px or last
        if intent.action == "place":
            if est_px is None:
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="no_price", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return

            notional = float(est_px) * float(intent.sz or 0.0)

            if notional > float(settings.RISK_MAX_NOTIONAL_USDT):
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="max_notional", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return

            # max order as % equity
            if notional > float(total_eq) * float(settings.RISK_MAX_ORDER_PCT_OF_EQUITY):
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="order_pct_of_equity", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return

            if intent.ord_type == "limit" and intent.px and last:
                dev = abs(float(intent.px) - float(last)) / max(float(last), 1e-9)
                if dev > bps_to_frac(settings.RISK_MAX_PRICE_DEVIATION_BPS):
                    decision = RiskDecision(ts_ms=now_ms, approved=False, reason="price_deviation", intent=intent)
                    await bus.publish("risk.order_rejected", decision.model_dump())
                    EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                    ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                    return

            # --- position limit (projected) ---
            pos_qty = safe_float(await r.get(f"position:{intent.inst_id}"), 0.0) or 0.0
            signed = float(intent.sz or 0.0) if intent.side == "buy" else -float(intent.sz or 0.0)
            proj_qty = float(pos_qty) + signed
            proj_notional = abs(proj_qty * float(est_px))
            if proj_notional > float(settings.RISK_MAX_POSITION_USDT):
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="max_position", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return

            # --- exposure checks (gross/net) ---
            pos_gross = safe_float(await r.get("exposure:pos_gross_usd"), 0.0) or 0.0
            pos_net = safe_float(await r.get("exposure:pos_net_usd"), 0.0) or 0.0
            oo_total = safe_float(await r.get("oo_notional:total"), 0.0) or 0.0

            projected_gross = float(pos_gross) + float(oo_total) + abs(notional)
            max_gross = float(total_eq) * float(settings.RISK_MAX_GROSS_EXPOSURE_PCT_OF_EQUITY)
            if projected_gross > max_gross:
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="max_gross_exposure", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return

            projected_net = float(pos_net) + (notional if intent.side == "buy" else -notional)
            max_net = float(total_eq) * float(settings.RISK_MAX_NET_EXPOSURE_PCT_OF_EQUITY)
            if abs(projected_net) > max_net:
                decision = RiskDecision(ts_ms=now_ms, approved=False, reason="max_net_exposure", intent=intent)
                await bus.publish("risk.order_rejected", decision.model_dump())
                EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                return

            # --- capital reservation ---
            if settings.RISK_USE_CAPITAL_RESERVATION:
                reserved = safe_float(await r.get("capital:reserved_usdt"), 0.0) or 0.0
                margin_factor = float(settings.RISK_DERIV_MARGIN_FACTOR) if _is_derivative(intent.inst_id, intent.td_mode) else float(settings.RISK_CASH_MARGIN_FACTOR)
                required = notional * margin_factor
                free = float(avail_eq) - float(reserved)
                if required > max(free, 0.0):
                    decision = RiskDecision(ts_ms=now_ms, approved=False, reason="insufficient_free_capital", intent=intent)
                    await bus.publish("risk.order_rejected", decision.model_dump())
                    EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="risk.order_rejected").inc()
                    ORDERS_REJECTED.labels(service=settings.SERVICE_NAME).inc()
                    return

        # approved
        order_ts.append(now_ms)
        decision = RiskDecision(ts_ms=now_ms, approved=True, reason="ok", intent=intent)
        await bus.publish("order.request", decision.model_dump())
        EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="order.request").inc()
        ORDERS_APPROVED.labels(service=settings.SERVICE_NAME).inc()

    await bus.subscribe("signal.order_intent", on_intent, queue="risk")

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
