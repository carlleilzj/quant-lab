from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from loguru import logger
from sqlalchemy import select

from app.core.bus import Bus
from app.core.config import settings
from app.core.logging import setup_logging
from app.core.metrics import EVENTS_IN, EVENTS_OUT, HEARTBEAT_TS, start_metrics_server
from app.core.redis import get_redis
from app.core.utils import safe_float, utc_ms
from app.db.models import Base, EquitySnapshot, Order, OrderCommand, Trade
from app.db.session import SessionLocal, engine
from app.exchanges.okx.ws import OkxWsClient
from app.exchanges.okx.rest import OkxRestClient
from app.schemas.events import EquityEvent, ExecutionAck, OrderUpdate


FINAL_ORDER_STATES = {"filled", "canceled", "rejected", "expired", "failed"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_derivative(inst_id: str, td_mode: str) -> bool:
    u = inst_id.upper()
    return td_mode != "cash" or u.endswith("-SWAP") or "-FUT" in u or "-OPTION" in u


async def ensure_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def recompute_position_exposure(r) -> Tuple[float, float]:
    """Recompute position gross/net exposure using last ticks."""
    keys = await r.keys("position:*")
    net = 0.0
    gross = 0.0
    for k in keys:
        inst = k.split("position:", 1)[1]
        qty = safe_float(await r.get(k), 0.0) or 0.0
        tick = await r.get(f"last_tick:{inst}")
        if not tick:
            continue
        try:
            last = safe_float(json.loads(tick).get("last"))
        except Exception:
            last = None
        if last is None:
            continue
        notional = float(qty) * float(last)
        net += notional
        gross += abs(notional)
    await r.set("exposure:pos_net_usd", str(net))
    await r.set("exposure:pos_gross_usd", str(gross))
    return net, gross


async def incr_float(r, key: str, delta: float):
    try:
        await r.incrbyfloat(key, float(delta))
    except Exception:
        # fallback: read+set
        v = safe_float(await r.get(key), 0.0) or 0.0
        await r.set(key, str(float(v) + float(delta)))


async def get_float(r, key: str, default: float = 0.0) -> float:
    v = safe_float(await r.get(key), default)
    return float(v if v is not None else default)


async def update_open_order_reserve_on_ack(r, ack: ExecutionAck):
    """On place-limit ack, create open-order reserve in redis."""
    if not ack.success:
        return
    if (ack.action or "place") != "place":
        return
    if (ack.ord_type or "").lower() != "limit":
        # market orders fill quickly; no reserve
        return
    if ack.px is None or ack.sz is None:
        return

    notional = float(ack.px) * float(ack.sz)
    td_mode = (ack.td_mode or "cash").lower()
    margin_factor = float(settings.RISK_DERIV_MARGIN_FACTOR) if _is_derivative(ack.inst_id, td_mode) else float(settings.RISK_CASH_MARGIN_FACTOR)
    reserved = notional * margin_factor

    key = f"oo:{ack.cl_ord_id}"
    meta = {
        "inst_id": ack.inst_id,
        "cl_ord_id": ack.cl_ord_id,
        "ord_id": ack.ord_id,
        "side": ack.side,
        "td_mode": td_mode,
        "px": float(ack.px),
        "sz": float(ack.sz),
        "acc_fill_sz": 0.0,
        "notional": notional,
        "margin_factor": margin_factor,
        "reserved": reserved,
        "state": "acked",
        "ts_ms": ack.ts_ms,
    }
    await r.set(key, json.dumps(meta, separators=(",", ":"), ensure_ascii=False), ex=60 * 60 * 24 * 3)
    await incr_float(r, f"oo_notional:{ack.inst_id}", notional)
    await incr_float(r, "oo_notional:total", notional)
    await incr_float(r, "capital:reserved_usdt", reserved)


async def update_open_order_reserve_on_update(r, upd: OrderUpdate):
    if not upd.cl_ord_id:
        return
    key = f"oo:{upd.cl_ord_id}"
    v = await r.get(key)
    if not v:
        return

    try:
        meta = json.loads(v)
    except Exception:
        return

    px = safe_float(meta.get("px")) or upd.px or upd.avg_px
    if px is None:
        px = 0.0
    total_sz = safe_float(meta.get("sz")) or upd.sz or 0.0
    prev_acc = safe_float(meta.get("acc_fill_sz"), 0.0) or 0.0
    acc = upd.acc_fill_sz if upd.acc_fill_sz is not None else prev_acc
    acc = float(acc or 0.0)

    remaining = max(float(total_sz) - acc, 0.0)
    new_notional = remaining * float(px)
    old_notional = safe_float(meta.get("notional"), 0.0) or 0.0
    delta_notional = float(new_notional) - float(old_notional)

    margin_factor = safe_float(meta.get("margin_factor"), 1.0) or 1.0
    old_reserved = safe_float(meta.get("reserved"), 0.0) or 0.0
    new_reserved = float(new_notional) * float(margin_factor)
    delta_reserved = float(new_reserved) - float(old_reserved)

    # apply deltas
    await incr_float(r, f"oo_notional:{meta.get('inst_id')}", delta_notional)
    await incr_float(r, "oo_notional:total", delta_notional)
    await incr_float(r, "capital:reserved_usdt", delta_reserved)

    # update meta
    meta["acc_fill_sz"] = acc
    meta["notional"] = new_notional
    meta["reserved"] = new_reserved
    meta["state"] = upd.state
    meta["ts_ms"] = upd.ts_ms

    # if final -> delete reserve
    if upd.state in FINAL_ORDER_STATES or remaining <= 0:
        await r.delete(key)
        return

    await r.set(key, json.dumps(meta, separators=(",", ":"), ensure_ascii=False), ex=60 * 60 * 24 * 3)


async def complete_commands_on_update(upd: OrderUpdate):
    """Mark OMS commands as completed when we receive exchange-confirmed order updates."""
    if not upd.cl_ord_id:
        return
    async with SessionLocal() as sess:
        res = await sess.execute(
            select(OrderCommand).where(
                OrderCommand.cl_ord_id == upd.cl_ord_id,
                OrderCommand.status.in_(["acked", "sending", "pending", "retry"]),
            )
        )
        cmds = res.scalars().all()
        if not cmds:
            return

        changed = False
        for cmd in cmds:
            if cmd.action == "place":
                # any order channel update counts as confirmation
                cmd.status = "completed"
                cmd.updated_at = _utc_now()
                changed = True
            elif cmd.action == "cancel":
                if upd.state == "canceled" or upd.state == "filled":
                    cmd.status = "completed"
                    cmd.updated_at = _utc_now()
                    changed = True
            elif cmd.action == "amend":
                # okx order channel includes amendResult on updates after amendment
                if isinstance(upd.raw, dict) and ("amendResult" in upd.raw or "amend_result" in upd.raw):
                    cmd.status = "completed"
                    cmd.updated_at = _utc_now()
                    changed = True

        if changed:
            await sess.commit()


async def upsert_order_from_ack(ack: ExecutionAck):
    async with SessionLocal() as sess:
        res = await sess.execute(select(Order).where(Order.cl_ord_id == ack.cl_ord_id))
        row = res.scalar_one_or_none()
        if row is None:
            row = Order(
                strategy=ack.strategy,
                inst_id=ack.inst_id,
                cl_ord_id=ack.cl_ord_id,
                ord_id=ack.ord_id,
                side=ack.side or "",
                ord_type=ack.ord_type or "",
                td_mode=ack.td_mode,
                px=float(ack.px) if ack.px is not None else None,
                sz=float(ack.sz or 0.0),
                state="acked" if ack.success else "rejected",
                raw={"execution_ack": ack.raw, "action": ack.action, "request_id": ack.request_id},
            )
            sess.add(row)
        else:
            row.updated_at = _utc_now()
            row.ord_id = ack.ord_id or row.ord_id
            row.state = "acked" if ack.success else "rejected"
            row.raw = {**(row.raw or {}), "execution_ack": ack.raw, "action": ack.action, "request_id": ack.request_id}
        await sess.commit()


async def upsert_order_from_update(upd: OrderUpdate):
    async with SessionLocal() as sess:
        if upd.cl_ord_id:
            res = await sess.execute(select(Order).where(Order.cl_ord_id == upd.cl_ord_id))
            row = res.scalar_one_or_none()
        else:
            row = None

        if row is None and upd.ord_id:
            res = await sess.execute(select(Order).where(Order.ord_id == upd.ord_id))
            row = res.scalar_one_or_none()

        if row is None:
            # create minimal row
            row = Order(
                strategy="unknown",
                inst_id=upd.inst_id,
                cl_ord_id=upd.cl_ord_id or f"unknown:{upd.ord_id or utc_ms()}",
                ord_id=upd.ord_id,
                side=upd.side or "",
                ord_type="",
                td_mode=None,
                px=upd.px,
                sz=float(upd.sz or 0.0),
                state=upd.state,
                acc_fill_sz=float(upd.acc_fill_sz or 0.0),
                avg_px=upd.avg_px,
                raw={"order_update": upd.raw},
            )
            sess.add(row)
        else:
            row.updated_at = _utc_now()
            row.state = upd.state
            row.acc_fill_sz = float(upd.acc_fill_sz or row.acc_fill_sz or 0.0)
            row.avg_px = upd.avg_px if upd.avg_px is not None else row.avg_px
            if upd.px is not None:
                row.px = upd.px
            if upd.sz is not None:
                row.sz = float(upd.sz)
            row.raw = {**(row.raw or {}), "order_update": upd.raw}

        await sess.commit()


async def apply_fill_to_position(r, upd: OrderUpdate):
    """Update net position (simplified)."""
    if upd.fill_sz is None or upd.fill_sz == 0:
        return
    if not upd.side:
        return

    key = f"position:{upd.inst_id}"
    prev = safe_float(await r.get(key), 0.0) or 0.0
    delta = float(upd.fill_sz) if upd.side == "buy" else -float(upd.fill_sz)
    new = float(prev) + delta
    await r.set(key, str(new))

    await recompute_position_exposure(r)


async def save_trade_from_update(upd: OrderUpdate):
    if upd.fill_sz is None or upd.fill_sz == 0:
        return
    async with SessionLocal() as sess:
        t = Trade(
            inst_id=upd.inst_id,
            side=upd.side or "",
            px=float(upd.avg_px or upd.px or 0.0),
            sz=float(upd.fill_sz),
            cl_ord_id=upd.cl_ord_id,
            ord_id=upd.ord_id,
            raw=upd.raw,
        )
        sess.add(t)
        await sess.commit()


async def parse_okx_order_update(item: dict[str, Any]) -> OrderUpdate:
    # OKX orders channel payload fields are strings; normalize best-effort.
    state = item.get("state") or ""
    cl_id = item.get("clOrdId") or None
    ord_id = item.get("ordId") or None
    side = item.get("side") or None

    px = safe_float(item.get("px"))
    sz = safe_float(item.get("sz"))
    acc_fill_sz = safe_float(item.get("accFillSz"))
    fill_sz = safe_float(item.get("fillSz"))
    avg_px = safe_float(item.get("avgPx"))

    ts_ms = int(safe_float(item.get("uTime")) or safe_float(item.get("cTime")) or utc_ms())

    return OrderUpdate(
        ts_ms=ts_ms,
        inst_id=item.get("instId") or "",
        cl_ord_id=cl_id,
        ord_id=ord_id,
        state=state,
        side=side,
        px=px,
        sz=sz,
        acc_fill_sz=acc_fill_sz,
        fill_sz=fill_sz,
        avg_px=avg_px,
        raw=item,
    )


async def main():
    setup_logging(settings.LOG_LEVEL, service=settings.SERVICE_NAME)
    start_metrics_server(settings.METRICS_PORT, settings.SERVICE_NAME)
    await ensure_db()

    bus = Bus(settings.NATS_URL, service=settings.SERVICE_NAME)
    await bus.connect()
    r = await get_redis(settings.REDIS_URL)

    mode = (settings.EXECUTION_MODE or "DRYRUN").upper()
    logger.info("Portfolio mode: {}", mode)

    okx_ws: Optional[OkxWsClient] = None
    okx_rest: Optional[OkxRestClient] = None

    if mode == "OKX":
        okx_ws = OkxWsClient(
            settings.OKX_WS_PRIVATE_URL,
            api_key=settings.OKX_API_KEY,
            secret_key=settings.OKX_SECRET_KEY,
            passphrase=settings.OKX_PASSPHRASE,
            name="okx-private",
        )
        okx_rest = OkxRestClient(
            base_url=settings.OKX_REST_BASE_URL,
            api_key=settings.OKX_API_KEY,
            secret_key=settings.OKX_SECRET_KEY,
            passphrase=settings.OKX_PASSPHRASE,
            simulated_trading=int(settings.OKX_SIMULATED_TRADING),
            exp_time_ms=settings.OKX_EXP_TIME_MS if settings.OKX_EXP_TIME_MS > 0 else None,
        )

    async def on_ack(subject: str, payload: dict[str, Any]):
        EVENTS_IN.labels(service=settings.SERVICE_NAME, subject=subject).inc()
        HEARTBEAT_TS.labels(service=settings.SERVICE_NAME).set(int(_utc_now().timestamp()))

        try:
            ack = ExecutionAck(**payload)
        except Exception as e:
            logger.warning("bad ack: {}", e)
            return

        await upsert_order_from_ack(ack)
        await update_open_order_reserve_on_ack(r, ack)

    async def on_update(subject: str, payload: dict[str, Any]):
        EVENTS_IN.labels(service=settings.SERVICE_NAME, subject=subject).inc()
        HEARTBEAT_TS.labels(service=settings.SERVICE_NAME).set(int(_utc_now().timestamp()))

        try:
            upd = OrderUpdate(**payload)
        except Exception as e:
            logger.warning("bad order update: {}", e)
            return

        await upsert_order_from_update(upd)
        await save_trade_from_update(upd)
        await apply_fill_to_position(r, upd)
        await update_open_order_reserve_on_update(r, upd)
        await complete_commands_on_update(upd)

    async def publish_equity(ev: EquityEvent):
        await bus.publish("portfolio.equity", ev.model_dump())
        EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="portfolio.equity").inc()
        await r.set("equity:latest", json.dumps(ev.model_dump(), separators=(",", ":"), ensure_ascii=False), ex=3600)

    async def poll_balance_loop():
        assert okx_rest is not None
        while True:
            try:
                resp = await okx_rest.get_balance()
                total = None
                avail = None
                upl = None
                if resp.get("code") == "0" and resp.get("data"):
                    d0 = resp["data"][0]
                    total = safe_float(d0.get("totalEq"))
                    avail = safe_float(d0.get("availEq"))
                    upl = safe_float(d0.get("upl"))
                ev = EquityEvent(ts_ms=utc_ms(), total_eq_usd=total, avail_eq_usd=avail, upl_usd=upl, raw=resp)
                await publish_equity(ev)
                async with SessionLocal() as sess:
                    sess.add(EquitySnapshot(raw=resp, total_eq_usd=total, avail_eq_usd=avail, upl_usd=upl))
                    await sess.commit()
            except Exception as e:
                logger.warning("balance poll failed: {}", e)
            await asyncio.sleep(max(3, int(settings.PF_BALANCE_POLL_S)))

    async def poll_positions_loop():
        assert okx_rest is not None
        while True:
            try:
                resp = await okx_rest.get_positions()
                if resp.get("code") == "0" and resp.get("data"):
                    # aggregate net by instId
                    agg: Dict[str, float] = {}
                    details: Dict[str, list[dict[str, Any]]] = {}
                    for p in resp["data"]:
                        inst = p.get("instId")
                        if not inst:
                            continue
                        pos = safe_float(p.get("pos"), 0.0) or 0.0
                        pos_side = (p.get("posSide") or "").lower()
                        if pos_side == "short":
                            pos = -abs(float(pos))
                        elif pos_side == "long":
                            pos = abs(float(pos))
                        else:
                            pos = float(pos)
                        agg[inst] = agg.get(inst, 0.0) + pos
                        details.setdefault(inst, []).append(p)

                    for inst, qty in agg.items():
                        await r.set(f"position:{inst}", str(qty))
                        await r.set(f"position:detail:{inst}", json.dumps(details.get(inst, []), separators=(",", ":"), ensure_ascii=False), ex=3600)

                    await recompute_position_exposure(r)
            except Exception as e:
                logger.warning("positions poll failed: {}", e)
            await asyncio.sleep(max(5, int(settings.PF_POSITIONS_POLL_S)))

    # --- OKX WS order updates (authoritative confirmation) ---
    async def okx_ws_loop():
        """Listen to OKX private orders channel and forward into portfolio.order_update.

        OKX 文档强调：REST 下单/撤单/改单的成功响应只表示交易所收到请求，
        真实成交/撤单/改单结果应以 WebSocket 订单频道推送为准。
        """
        assert okx_ws is not None

        while True:
            try:
                await okx_ws.connect()
                await okx_ws.login()
                await okx_ws.subscribe([{"channel": "orders", "instType": "ANY"}], req_id="orders")

                async for msg in okx_ws.messages():
                    if isinstance(msg, str):
                        continue
                    if msg.get("event"):
                        continue
                    if msg.get("arg", {}).get("channel") != "orders":
                        continue
                    data = msg.get("data") or []
                    for item in data:
                        upd = await parse_okx_order_update(item)
                        await bus.publish("portfolio.order_update", upd.model_dump())
                        EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="portfolio.order_update").inc()

            except Exception as e:
                logger.warning("okx ws loop error: {}", e)
                try:
                    await okx_ws.close()
                except Exception:
                    pass
                await asyncio.sleep(3)

    await bus.subscribe("execution.ack", on_ack, queue="portfolio")
    await bus.subscribe("portfolio.order_update", on_update, queue="portfolio")

    if mode == "OKX":
        asyncio.create_task(okx_ws_loop())
        asyncio.create_task(poll_balance_loop())
        asyncio.create_task(poll_positions_loop())

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
