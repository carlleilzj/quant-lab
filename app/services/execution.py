from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import httpx
from loguru import logger
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError

from app.core.bus import Bus
from app.core.config import settings
from app.core.logging import setup_logging
from app.core.metrics import (
    EVENTS_IN,
    EVENTS_OUT,
    HEARTBEAT_TS,
    ORDERS_ACKED,
    ORDERS_SENT,
    start_metrics_server,
)
from app.core.redis import get_redis
from app.core.utils import safe_float, utc_ms
from app.db.models import Base, Order, OrderCommand
from app.db.session import SessionLocal, engine
from app.exchanges.okx.rest import OkxRestClient
from app.schemas.events import ExecutionAck, OrderIntent, OrderUpdate, RiskDecision


FINAL_ORDER_STATES = {"filled", "canceled", "rejected", "expired", "failed"}
ACTIVE_ORDER_STATES = {"created", "approved", "sent", "acked", "live", "partially_filled", "amend_sent", "cancel_sent"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_live_env() -> bool:
    return int(settings.OKX_SIMULATED_TRADING) == 0


def _is_derivative(inst_id: str, td_mode: str) -> bool:
    # heuristic: SWAP/FUT/OPTION are derivatives; also non-cash margin modes.
    u = inst_id.upper()
    return td_mode != "cash" or u.endswith("-SWAP") or "-FUT" in u or "-OPTION" in u


def compute_request_id(intent: OrderIntent) -> str:
    # Prefer explicit request_id from upstream.
    if intent.request_id:
        return intent.request_id

    # Fallbacks that are stable enough for idempotency.
    if intent.action == "place":
        return intent.cl_ord_id

    if intent.action == "cancel":
        return f"cancel:{intent.cl_ord_id}"

    # amend: allow repeated same amendment to dedupe
    new_px = intent.new_px if intent.new_px is not None else intent.px
    new_sz = intent.new_sz if intent.new_sz is not None else intent.sz
    return f"amend:{intent.cl_ord_id}:{new_px}:{new_sz}:{int(intent.cxl_on_fail)}"


def backoff_ms(attempt: int) -> int:
    # Exponential backoff with jitter.
    base = max(int(settings.OMS_RETRY_BASE_MS), 50)
    cap = max(int(settings.OMS_RETRY_MAX_MS), base)
    exp = min(cap, int(base * (2 ** max(attempt - 1, 0))))
    jitter = int(exp * (0.15 * random.random()))
    return min(cap, exp + jitter)


def is_retryable_error(exc: Exception) -> bool:
    # Network / timeout errors -> retryable
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    return False


def okx_resp_success(resp: dict[str, Any]) -> bool:
    return str(resp.get("code")) == "0"


def okx_err_code(resp: dict[str, Any]) -> str:
    return str(resp.get("code") or "")


# Conservative retry list (can be extended by config later).
OKX_RETRYABLE_CODES = {
    "50000",  # general server error
    "50001",  # server busy/timeout (varies)
    "50002",
    "50004",
    "50011",
    "50013",
    "50100",
    "50101",
    "50102",
    "50103",
    "50104",
    "50105",
    "50106",
    "50107",
    "50108",
    "50109",
    "50110",
    "50111",
    "50112",
    "50113",
    "50114",
    "50115",
}


def okx_is_retryable_response(resp: dict[str, Any]) -> bool:
    code = okx_err_code(resp)
    if code in OKX_RETRYABLE_CODES:
        return True
    msg = (resp.get("msg") or "").lower()
    if "timeout" in msg or "busy" in msg or "system" in msg:
        return True
    return False


@dataclass
class DryOpenOrder:
    ord_id: str
    intent: OrderIntent


async def ensure_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def publish_ack(bus: Bus, ack: ExecutionAck):
    await bus.publish("execution.ack", ack.model_dump())
    EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="execution.ack").inc()
    ORDERS_ACKED.labels(service=settings.SERVICE_NAME).inc()


async def publish_update(bus: Bus, upd: OrderUpdate):
    await bus.publish("portfolio.order_update", upd.model_dump())
    EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="portfolio.order_update").inc()


async def upsert_order_from_intent(intent: OrderIntent, state: str, ord_id: Optional[str] = None, raw: Optional[dict[str, Any]] = None):
    """Ensure Order row exists for place/amend/cancel visibility & ops."""
    async with SessionLocal() as sess:
        res = await sess.execute(select(Order).where(Order.cl_ord_id == intent.cl_ord_id))
        row = res.scalar_one_or_none()
        if row is None:
            row = Order(
                strategy=intent.strategy,
                inst_id=intent.inst_id,
                cl_ord_id=intent.cl_ord_id,
                ord_id=ord_id or intent.ord_id,
                side=intent.side or "",
                ord_type=intent.ord_type or "",
                td_mode=intent.td_mode,
                px=float(intent.px) if intent.px is not None else None,
                sz=float(intent.sz or 0.0),
                state=state,
                raw=raw or {"meta": intent.meta, "action": intent.action},
            )
            sess.add(row)
        else:
            row.state = state
            if ord_id:
                row.ord_id = ord_id
            row.updated_at = _utc_now()
            if raw:
                row.raw = {**(row.raw or {}), **raw}
        await sess.commit()


async def insert_command_if_new(intent: OrderIntent, request_id: str) -> Optional[OrderCommand]:
    cmd = OrderCommand(
        request_id=request_id,
        strategy=intent.strategy,
        inst_id=intent.inst_id,
        cl_ord_id=intent.cl_ord_id,
        ord_id=intent.ord_id,
        action=intent.action,
        status="pending",
        attempt=0,
        next_retry_at=_utc_now(),
        payload=intent.model_dump(),
        response={},
    )
    async with SessionLocal() as sess:
        sess.add(cmd)
        try:
            await sess.commit()
        except IntegrityError:
            await sess.rollback()
            return None
        await sess.refresh(cmd)
        return cmd


async def mark_command(
    request_id: str,
    *,
    status: Optional[str] = None,
    ord_id: Optional[str] = None,
    response: Optional[dict[str, Any]] = None,
    last_error: Optional[str] = None,
    next_retry_at: Optional[datetime] = None,
    bump_attempt: bool = False,
) -> None:
    async with SessionLocal() as sess:
        res = await sess.execute(select(OrderCommand).where(OrderCommand.request_id == request_id))
        row = res.scalar_one_or_none()
        if row is None:
            return
        if status is not None:
            row.status = status
        if ord_id is not None:
            row.ord_id = ord_id
        if response is not None:
            row.response = response
        if last_error is not None:
            row.last_error = last_error
        if next_retry_at is not None:
            row.next_retry_at = next_retry_at
        if bump_attempt:
            row.attempt = int(row.attempt or 0) + 1
        row.updated_at = _utc_now()
        await sess.commit()


async def fetch_due_commands(limit: int) -> list[OrderCommand]:
    now = _utc_now()
    async with SessionLocal() as sess:
        q = (
            select(OrderCommand)
            .where(
                and_(
                    OrderCommand.status.in_(["pending", "retry"]),
                    or_(OrderCommand.next_retry_at.is_(None), OrderCommand.next_retry_at <= now),
                )
            )
            .order_by(OrderCommand.created_at.asc())
            .limit(limit)
        )
        res = await sess.execute(q)
        return list(res.scalars().all())


async def main():
    setup_logging(settings.LOG_LEVEL, service=settings.SERVICE_NAME)
    start_metrics_server(settings.METRICS_PORT, settings.SERVICE_NAME)

    await ensure_db()

    bus = Bus(settings.NATS_URL, service=settings.SERVICE_NAME)
    await bus.connect()

    r = await get_redis(settings.REDIS_URL)

    mode = (settings.EXECUTION_MODE or "DRYRUN").upper()
    logger.info("Execution/OMS mode: {}", mode)

    okx: Optional[OkxRestClient] = None
    if mode == "OKX":
        if _is_live_env() and not settings.ALLOW_LIVE_TRADING:
            raise RuntimeError("Refuse to run LIVE trading unless ALLOW_LIVE_TRADING=true")

        okx = OkxRestClient(
            base_url=settings.OKX_REST_BASE_URL,
            api_key=settings.OKX_API_KEY,
            secret_key=settings.OKX_SECRET_KEY,
            passphrase=settings.OKX_PASSPHRASE,
            simulated_trading=int(settings.OKX_SIMULATED_TRADING),
            exp_time_ms=settings.OKX_EXP_TIME_MS if settings.OKX_EXP_TIME_MS > 0 else None,
        )

    # --- DRYRUN simulation state ---
    dry_open: Dict[str, DryOpenOrder] = {}  # cl_ord_id -> order

    async def handle_place_okx(intent: OrderIntent, request_id: str) -> ExecutionAck:
        assert okx is not None
        payload: dict[str, Any] = {
            "instId": intent.inst_id,
            "tdMode": intent.td_mode,
            "side": intent.side,
            "ordType": intent.ord_type,
            "sz": str(intent.sz),
            "clOrdId": intent.cl_ord_id,
            "tag": intent.strategy,
        }
        if intent.ord_type == "limit":
            payload["px"] = str(intent.px)
        if intent.pos_side:
            payload["posSide"] = intent.pos_side
        if intent.reduce_only:
            payload["reduceOnly"] = True

        resp = await okx.place_order(payload)
        success = okx_resp_success(resp)
        ord_id = None
        msg = resp.get("msg") or ""
        if success and resp.get("data"):
            ord_id = resp["data"][0].get("ordId")
            msg = resp["data"][0].get("sMsg") or msg

        return ExecutionAck(
            ts_ms=utc_ms(),
            strategy=intent.strategy,
            inst_id=intent.inst_id,
            cl_ord_id=intent.cl_ord_id,
            ord_id=ord_id,
            action=intent.action,
            request_id=request_id,
            side=intent.side,
            ord_type=intent.ord_type,
            td_mode=intent.td_mode,
            px=intent.px,
            sz=float(intent.sz or 0.0),
            success=success,
            msg=msg,
            raw=resp,
        )

    async def handle_cancel_okx(intent: OrderIntent, request_id: str) -> ExecutionAck:
        assert okx is not None
        payload: dict[str, Any] = {"instId": intent.inst_id}
        if intent.ord_id:
            payload["ordId"] = intent.ord_id
        else:
            payload["clOrdId"] = intent.cl_ord_id
        resp = await okx.cancel_order(payload)
        success = okx_resp_success(resp)
        msg = resp.get("msg") or ""
        return ExecutionAck(
            ts_ms=utc_ms(),
            strategy=intent.strategy,
            inst_id=intent.inst_id,
            cl_ord_id=intent.cl_ord_id,
            ord_id=intent.ord_id,
            action=intent.action,
            request_id=request_id,
            side=intent.side,
            ord_type=intent.ord_type,
            td_mode=intent.td_mode,
            px=intent.px,
            sz=float(intent.sz or 0.0),
            success=success,
            msg=msg,
            raw=resp,
        )

    async def handle_amend_okx(intent: OrderIntent, request_id: str) -> ExecutionAck:
        assert okx is not None
        new_px = intent.new_px if intent.new_px is not None else intent.px
        new_sz = intent.new_sz if intent.new_sz is not None else intent.sz
        payload: dict[str, Any] = {"instId": intent.inst_id}
        if intent.ord_id:
            payload["ordId"] = intent.ord_id
        else:
            payload["clOrdId"] = intent.cl_ord_id
        if new_px is not None:
            payload["newPx"] = str(new_px)
        if new_sz is not None:
            payload["newSz"] = str(new_sz)
        if intent.cxl_on_fail:
            payload["cxlOnFail"] = True
        resp = await okx.amend_order(payload)
        success = okx_resp_success(resp)
        msg = resp.get("msg") or ""
        ord_id = intent.ord_id
        if success and resp.get("data"):
            ord_id = resp["data"][0].get("ordId") or ord_id
            msg = resp["data"][0].get("sMsg") or msg
        return ExecutionAck(
            ts_ms=utc_ms(),
            strategy=intent.strategy,
            inst_id=intent.inst_id,
            cl_ord_id=intent.cl_ord_id,
            ord_id=ord_id,
            action=intent.action,
            request_id=request_id,
            side=intent.side,
            ord_type=intent.ord_type,
            td_mode=intent.td_mode,
            px=float(new_px) if new_px is not None else None,
            sz=float(new_sz) if new_sz is not None else None,
            success=success,
            msg=msg,
            raw=resp,
        )

    async def reconcile_okx_by_clid(inst_id: str, cl_ord_id: str) -> Optional[dict[str, Any]]:
        assert okx is not None
        try:
            resp = await okx.get_order(inst_id=inst_id, cl_ord_id=cl_ord_id)
            if okx_resp_success(resp) and resp.get("data"):
                return resp["data"][0]
        except Exception:
            return None
        return None

    async def process_command(cmd: OrderCommand):
        intent = OrderIntent(**cmd.payload)
        request_id = cmd.request_id

        # bump attempt for each processing round
        attempt = int(cmd.attempt or 0) + 1
        await mark_command(request_id, status="sending", bump_attempt=True)

        # hard stop to avoid infinite retries
        if attempt > int(settings.OMS_MAX_ATTEMPTS):
            msg = f"max_attempts_exceeded:{attempt}>{int(settings.OMS_MAX_ATTEMPTS)}"
            await mark_command(request_id, status="failed", last_error=msg)
            await upsert_order_from_intent(intent, state="failed", raw={"oms": {"error": msg}})
            ack = ExecutionAck(
                ts_ms=utc_ms(),
                strategy=intent.strategy,
                inst_id=intent.inst_id,
                cl_ord_id=intent.cl_ord_id,
                action=intent.action,
                request_id=request_id,
                success=False,
                msg=msg,
                raw={"request_id": request_id},
            )
            await publish_ack(bus, ack)
            return

        # --- DRYRUN ---
        if mode == "DRYRUN":
            if intent.action == "place":
                # basic validations
                if intent.sz is None or (intent.ord_type == "limit" and intent.px is None):
                    ack = ExecutionAck(
                        ts_ms=utc_ms(),
                        strategy=intent.strategy,
                        inst_id=intent.inst_id,
                        cl_ord_id=intent.cl_ord_id,
                        action=intent.action,
                        request_id=request_id,
                        success=False,
                        msg="dryrun_invalid_intent",
                        raw={"intent": intent.model_dump()},
                    )
                    await publish_ack(bus, ack)
                    await mark_command(request_id, status="failed", last_error=ack.msg, response=ack.raw)
                    await upsert_order_from_intent(intent, state="rejected", raw={"error": ack.msg})
                    return

                ord_id = f"dry-{utc_ms()}"
                ack = ExecutionAck(
                    ts_ms=utc_ms(),
                    strategy=intent.strategy,
                    inst_id=intent.inst_id,
                    cl_ord_id=intent.cl_ord_id,
                    ord_id=ord_id,
                    action=intent.action,
                    request_id=request_id,
                    side=intent.side,
                    ord_type=intent.ord_type,
                    td_mode=intent.td_mode,
                    px=intent.px,
                    sz=float(intent.sz or 0.0),
                    success=True,
                    msg="dryrun_accepted",
                    raw={"mode": "dryrun"},
                )
                await publish_ack(bus, ack)
                await mark_command(request_id, status="acked", ord_id=ord_id, response=ack.raw)
                await upsert_order_from_intent(intent, state="acked", ord_id=ord_id, raw={"oms": {"request_id": request_id}})

                if intent.ord_type == "market":
                    last_json = await r.get(f"last_tick:{intent.inst_id}")
                    last = None
                    if last_json:
                        try:
                            last = safe_float(json.loads(last_json).get("last"))
                        except Exception:
                            last = None
                    fill_px = float(last or 0.0)
                    upd = OrderUpdate(
                        ts_ms=utc_ms(),
                        inst_id=intent.inst_id,
                        cl_ord_id=intent.cl_ord_id,
                        ord_id=ord_id,
                        state="filled",
                        side=intent.side,
                        px=fill_px,
                        sz=float(intent.sz or 0.0),
                        acc_fill_sz=float(intent.sz or 0.0),
                        fill_sz=float(intent.sz or 0.0),
                        avg_px=fill_px,
                        raw={"mode": "dryrun"},
                    )
                    await publish_update(bus, upd)
                    await mark_command(request_id, status="completed", response={"filled": True})
                    await upsert_order_from_intent(intent, state="filled", ord_id=ord_id, raw={"filled_px": fill_px})
                else:
                    dry_open[intent.cl_ord_id] = DryOpenOrder(ord_id=ord_id, intent=intent)
                    # keep as acked until portfolio confirms fill/cancel
                return

            if intent.action == "cancel":
                oo = dry_open.pop(intent.cl_ord_id, None)
                ord_id = oo.ord_id if oo else None
                ack = ExecutionAck(
                    ts_ms=utc_ms(),
                    strategy=intent.strategy,
                    inst_id=intent.inst_id,
                    cl_ord_id=intent.cl_ord_id,
                    ord_id=ord_id,
                    action=intent.action,
                    request_id=request_id,
                    success=True,
                    msg="dryrun_cancel_sent",
                    raw={"mode": "dryrun", "had_open": bool(oo)},
                )
                await publish_ack(bus, ack)
                await mark_command(request_id, status="acked", ord_id=ord_id, response=ack.raw)
                await upsert_order_from_intent(intent, state="cancel_sent", ord_id=ord_id, raw={"oms": {"request_id": request_id}})

                upd = OrderUpdate(
                    ts_ms=utc_ms(),
                    inst_id=intent.inst_id,
                    cl_ord_id=intent.cl_ord_id,
                    ord_id=ord_id,
                    state="canceled",
                    side=intent.side,
                    px=intent.px,
                    sz=float(intent.sz or 0.0) if intent.sz is not None else None,
                    acc_fill_sz=None,
                    fill_sz=0.0,
                    avg_px=None,
                    raw={"mode": "dryrun", "reason": "cancel"},
                )
                await publish_update(bus, upd)
                await mark_command(request_id, status="completed", response={"canceled": True})
                await upsert_order_from_intent(intent, state="canceled", ord_id=ord_id, raw={"canceled": True})
                return

            if intent.action == "amend":
                oo = dry_open.get(intent.cl_ord_id)
                if not oo:
                    ack = ExecutionAck(
                        ts_ms=utc_ms(),
                        strategy=intent.strategy,
                        inst_id=intent.inst_id,
                        cl_ord_id=intent.cl_ord_id,
                        action=intent.action,
                        request_id=request_id,
                        success=False,
                        msg="dryrun_no_such_order",
                        raw={"mode": "dryrun"},
                    )
                    await publish_ack(bus, ack)
                    await mark_command(request_id, status="failed", last_error=ack.msg, response=ack.raw)
                    return
                new_px = intent.new_px if intent.new_px is not None else intent.px
                new_sz = intent.new_sz if intent.new_sz is not None else intent.sz
                if new_px is not None:
                    oo.intent.px = float(new_px)
                if new_sz is not None:
                    oo.intent.sz = float(new_sz)
                ack = ExecutionAck(
                    ts_ms=utc_ms(),
                    strategy=intent.strategy,
                    inst_id=intent.inst_id,
                    cl_ord_id=intent.cl_ord_id,
                    ord_id=oo.ord_id,
                    action=intent.action,
                    request_id=request_id,
                    success=True,
                    msg="dryrun_amend_sent",
                    raw={"mode": "dryrun", "new_px": new_px, "new_sz": new_sz},
                )
                await publish_ack(bus, ack)
                await mark_command(request_id, status="completed", ord_id=oo.ord_id, response=ack.raw)
                await upsert_order_from_intent(intent, state="amend_sent", ord_id=oo.ord_id, raw={"amend": ack.raw})
                return

            # unknown action
            await mark_command(request_id, status="failed", last_error="unknown_action")
            return

        # --- OKX ---
        assert mode == "OKX"
        assert okx is not None

        try:
            if intent.action == "place":
                if intent.sz is None:
                    raise ValueError("place intent sz is required")
                if intent.ord_type == "limit" and intent.px is None:
                    raise ValueError("limit order px is required")

                ack = await handle_place_okx(intent, request_id)
                await publish_ack(bus, ack)

                if ack.success:
                    await mark_command(request_id, status="acked", ord_id=ack.ord_id, response=ack.raw)
                    await upsert_order_from_intent(intent, state="sent", ord_id=ack.ord_id, raw={"oms": {"request_id": request_id}})
                else:
                    # OKX non-zero code: usually non-retryable unless configured
                    if okx_is_retryable_response(ack.raw):
                        delay = backoff_ms(attempt)
                        await mark_command(
                            request_id,
                            status="retry",
                            last_error=f"okx_code={okx_err_code(ack.raw)} msg={ack.msg}",
                            response=ack.raw,
                            next_retry_at=_utc_now() + timedelta(milliseconds=delay),
                        )
                        await upsert_order_from_intent(intent, state="approved", raw={"oms": {"retry_in_ms": delay}})
                    else:
                        await mark_command(request_id, status="failed", last_error=f"okx_code={okx_err_code(ack.raw)} msg={ack.msg}", response=ack.raw)
                        await upsert_order_from_intent(intent, state="rejected", raw={"okx": {"code": okx_err_code(ack.raw), "msg": ack.msg}})
                return

            if intent.action == "cancel":
                ack = await handle_cancel_okx(intent, request_id)
                await publish_ack(bus, ack)
                if ack.success:
                    await mark_command(request_id, status="acked", ord_id=ack.ord_id, response=ack.raw)
                    await upsert_order_from_intent(intent, state="cancel_sent", ord_id=ack.ord_id, raw={"oms": {"request_id": request_id}})
                else:
                    if okx_is_retryable_response(ack.raw):
                        delay = backoff_ms(attempt)
                        await mark_command(request_id, status="retry", last_error=f"okx_code={okx_err_code(ack.raw)} msg={ack.msg}", response=ack.raw, next_retry_at=_utc_now() + timedelta(milliseconds=delay))
                    else:
                        await mark_command(request_id, status="failed", last_error=f"okx_code={okx_err_code(ack.raw)} msg={ack.msg}", response=ack.raw)
                return

            if intent.action == "amend":
                ack = await handle_amend_okx(intent, request_id)
                await publish_ack(bus, ack)
                if ack.success:
                    await mark_command(request_id, status="acked", ord_id=ack.ord_id, response=ack.raw)
                    await upsert_order_from_intent(intent, state="amend_sent", ord_id=ack.ord_id, raw={"oms": {"request_id": request_id}})
                else:
                    if okx_is_retryable_response(ack.raw):
                        delay = backoff_ms(attempt)
                        await mark_command(request_id, status="retry", last_error=f"okx_code={okx_err_code(ack.raw)} msg={ack.msg}", response=ack.raw, next_retry_at=_utc_now() + timedelta(milliseconds=delay))
                    else:
                        await mark_command(request_id, status="failed", last_error=f"okx_code={okx_err_code(ack.raw)} msg={ack.msg}", response=ack.raw)
                return

            await mark_command(request_id, status="failed", last_error="unknown_action")
            return

        except Exception as e:
            # Network/timeout -> reconcile then retry.
            logger.exception("command processing exception: {}", e)
            if is_retryable_error(e):
                # reconcile for place/cancel/amend
                reconciled = None
                if intent.action in ("place", "cancel", "amend"):
                    reconciled = await reconcile_okx_by_clid(intent.inst_id, intent.cl_ord_id)

                if reconciled:
                    ord_id = reconciled.get("ordId") or intent.ord_id
                    state = reconciled.get("state") or "unknown"
                    await mark_command(
                        request_id,
                        status="acked",
                        ord_id=ord_id,
                        response={"reconciled": True, "order": reconciled},
                        last_error=str(e),
                    )
                    await upsert_order_from_intent(intent, state=state, ord_id=ord_id, raw={"oms": {"reconciled": True}})
                    ack = ExecutionAck(
                        ts_ms=utc_ms(),
                        strategy=intent.strategy,
                        inst_id=intent.inst_id,
                        cl_ord_id=intent.cl_ord_id,
                        ord_id=ord_id,
                        action=intent.action,
                        request_id=request_id,
                        success=True,
                        msg="reconciled_after_exception",
                        raw={"exception": str(e), "order": reconciled},
                    )
                    await publish_ack(bus, ack)
                    return

                delay = backoff_ms(attempt)
                await mark_command(
                    request_id,
                    status="retry",
                    last_error=str(e),
                    next_retry_at=_utc_now() + timedelta(milliseconds=delay),
                )
                await upsert_order_from_intent(intent, state="approved", raw={"oms": {"retry_in_ms": delay, "last_error": str(e)}})
                return

            # Non-retryable exception
            await mark_command(request_id, status="failed", last_error=str(e))
            await upsert_order_from_intent(intent, state="failed", raw={"oms": {"exception": str(e)}})
            ack = ExecutionAck(
                ts_ms=utc_ms(),
                strategy=intent.strategy,
                inst_id=intent.inst_id,
                cl_ord_id=intent.cl_ord_id,
                action=intent.action,
                request_id=request_id,
                success=False,
                msg=str(e),
                raw={"exception": str(e)},
            )
            await publish_ack(bus, ack)
            return

    async def command_worker():
        while True:
            try:
                due = await fetch_due_commands(limit=int(settings.OMS_WORKER_BATCH))
                if not due:
                    await asyncio.sleep(0.2)
                    continue
                for cmd in due:
                    await process_command(cmd)
            except Exception as e:
                logger.exception("OMS worker loop error: {}", e)
                await asyncio.sleep(1)

    async def on_order_request(subject: str, payload: dict[str, Any]):
        EVENTS_IN.labels(service=settings.SERVICE_NAME, subject=subject).inc()
        HEARTBEAT_TS.labels(service=settings.SERVICE_NAME).set(int(_utc_now().timestamp()))

        try:
            decision = RiskDecision(**payload)
        except Exception as e:
            logger.warning("bad risk decision: {}", e)
            return

        if not decision.approved:
            return

        intent = decision.intent
        request_id = compute_request_id(intent)

        # Ensure visibility in orders table early (ops friendly)
        if intent.action == "place":
            await upsert_order_from_intent(intent, state="approved", raw={"risk_reason": decision.reason, "oms": {"request_id": request_id}})
        elif intent.action == "cancel":
            await upsert_order_from_intent(intent, state="cancel_sent", raw={"risk_reason": decision.reason, "oms": {"request_id": request_id}})
        elif intent.action == "amend":
            await upsert_order_from_intent(intent, state="amend_sent", raw={"risk_reason": decision.reason, "oms": {"request_id": request_id}})

        cmd = await insert_command_if_new(intent, request_id)
        if cmd is None:
            # idempotent duplicate
            logger.info("duplicate command ignored: {}", request_id)
            return

        EVENTS_OUT.labels(service=settings.SERVICE_NAME, subject="oms.command_queued").inc()
        ORDERS_SENT.labels(service=settings.SERVICE_NAME).inc()

    async def on_tick(subject: str, payload: dict[str, Any]):
        # Only for DRYRUN to simulate limit fills
        if mode != "DRYRUN":
            return
        try:
            inst_id = payload.get("inst_id")
            last = float(payload.get("last"))
        except Exception:
            return

        # iterate a copy to allow deletion
        for cl_id, oo in list(dry_open.items()):
            intent = oo.intent
            if intent.inst_id != inst_id:
                continue
            if intent.px is None or intent.sz is None:
                continue
            if intent.side == "buy" and last <= float(intent.px):
                dry_open.pop(cl_id, None)
            elif intent.side == "sell" and last >= float(intent.px):
                dry_open.pop(cl_id, None)
            else:
                continue

            # filled
            upd = OrderUpdate(
                ts_ms=utc_ms(),
                inst_id=intent.inst_id,
                cl_ord_id=intent.cl_ord_id,
                ord_id=oo.ord_id,
                state="filled",
                side=intent.side,
                px=float(intent.px),
                sz=float(intent.sz),
                acc_fill_sz=float(intent.sz),
                fill_sz=float(intent.sz),
                avg_px=float(intent.px),
                raw={"mode": "dryrun", "trigger": {"last": last}},
            )
            await publish_update(bus, upd)

    asyncio.create_task(command_worker())

    await bus.subscribe("order.request", on_order_request, queue="execution")
    await bus.subscribe("md.ticker.*", on_tick, queue="execution")

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
