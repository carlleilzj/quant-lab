from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
from loguru import logger
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from sqlalchemy import select

from app.core.bus import Bus
from app.core.config import settings
from app.core.logging import setup_logging
from app.core.redis import get_redis
from app.core.utils import utc_ms
from app.db.models import Base, Order, OrderCommand, Trade
from app.db.session import SessionLocal, engine
from app.schemas.events import OrderIntent


def create_app() -> FastAPI:
    app = FastAPI(title="okx-quant-lab", version="0.2.0")

    setup_logging(settings.LOG_LEVEL, service=settings.SERVICE_NAME)

    redis_holder: dict[str, Any] = {"r": None}
    bus_holder: dict[str, Any] = {"bus": None}

    @app.on_event("startup")
    async def _startup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        redis_holder["r"] = await get_redis(settings.REDIS_URL)

        bus = Bus(settings.NATS_URL, service=settings.SERVICE_NAME)
        await bus.connect()
        bus_holder["bus"] = bus

    @app.get("/health")
    async def health():
        return {"ok": True, "service": settings.SERVICE_NAME, "ts": datetime.now(timezone.utc).isoformat()}

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics():
        return PlainTextResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)

    # --- basic controls ---
    @app.get("/v1/kill-switch")
    async def get_kill_switch():
        r = redis_holder["r"]
        ks = await r.get("kill_switch")
        if ks is None:
            ks = str(int(settings.KILL_SWITCH))
        reason = await r.get("kill_switch:reason")
        return {"kill_switch": int(ks), "reason": reason or ""}

    @app.post("/v1/kill-switch")
    async def set_kill_switch(state: int = Query(..., ge=0, le=1), reason: str = ""):
        r = redis_holder["r"]
        await r.set("kill_switch", str(state))
        if reason:
            await r.set("kill_switch:reason", reason, ex=3600)
        await r.set("kill_switch:ts_ms", str(utc_ms()), ex=3600)
        return {"kill_switch": state, "reason": reason}

    @app.get("/v1/risk/status")
    async def risk_status():
        r = redis_holder["r"]
        keys = [
            "kill_switch",
            "kill_switch:reason",
            "kill_switch:ts_ms",
            "equity:latest",
            "equity:peak",
            "equity:drawdown",
            "equity:daily_pnl",
            "exposure:pos_gross_usd",
            "exposure:pos_net_usd",
            "oo_notional:total",
            "capital:reserved_usdt",
        ]
        out: dict[str, Any] = {}
        for k in keys:
            v = await r.get(k)
            if v is None:
                continue
            if k.endswith(":latest"):
                try:
                    out[k] = json.loads(v)
                except Exception:
                    out[k] = v
            else:
                out[k] = v
        return out

    # --- market/state ---
    @app.get("/v1/last-tick/{inst_id}")
    async def last_tick(inst_id: str):
        r = redis_holder["r"]
        v = await r.get(f"last_tick:{inst_id}")
        if not v:
            raise HTTPException(status_code=404, detail="no tick")
        return json.loads(v)

    @app.get("/v1/positions")
    async def positions():
        r = redis_holder["r"]
        keys = await r.keys("position:*")
        out = {}
        for k in keys:
            if k.startswith("position:detail:"):
                continue
            out[k.replace("position:", "")] = float(await r.get(k) or 0.0)
        return out

    @app.get("/v1/open-orders/reserve")
    async def open_order_reserve():
        r = redis_holder["r"]
        total = await r.get("oo_notional:total")
        reserved = await r.get("capital:reserved_usdt")
        return {"oo_notional_total": float(total or 0.0), "capital_reserved_usdt": float(reserved or 0.0)}

    @app.get("/v1/equity")
    async def equity():
        r = redis_holder["r"]
        v = await r.get("equity:latest")
        return json.loads(v) if v else {}

    # --- db views ---
    @app.get("/v1/orders")
    async def orders(limit: int = Query(50, ge=1, le=500)):
        async with SessionLocal() as sess:
            res = await sess.execute(select(Order).order_by(Order.created_at.desc()).limit(limit))
            rows = res.scalars().all()
            return [
                {
                    "created_at": o.created_at.isoformat(),
                    "updated_at": o.updated_at.isoformat(),
                    "strategy": o.strategy,
                    "inst_id": o.inst_id,
                    "cl_ord_id": o.cl_ord_id,
                    "ord_id": o.ord_id,
                    "side": o.side,
                    "ord_type": o.ord_type,
                    "td_mode": o.td_mode,
                    "px": o.px,
                    "sz": o.sz,
                    "state": o.state,
                    "acc_fill_sz": o.acc_fill_sz,
                    "avg_px": o.avg_px,
                }
                for o in rows
            ]

    @app.get("/v1/trades")
    async def trades(limit: int = Query(50, ge=1, le=500)):
        async with SessionLocal() as sess:
            res = await sess.execute(select(Trade).order_by(Trade.ts.desc()).limit(limit))
            rows = res.scalars().all()
            return [
                {
                    "ts": t.ts.isoformat(),
                    "inst_id": t.inst_id,
                    "side": t.side,
                    "px": t.px,
                    "sz": t.sz,
                    "cl_ord_id": t.cl_ord_id,
                    "ord_id": t.ord_id,
                }
                for t in rows
            ]

    @app.get("/v1/oms/commands")
    async def oms_commands(limit: int = Query(100, ge=1, le=500)):
        async with SessionLocal() as sess:
            res = await sess.execute(select(OrderCommand).order_by(OrderCommand.created_at.desc()).limit(limit))
            rows = res.scalars().all()
            return [
                {
                    "created_at": c.created_at.isoformat(),
                    "updated_at": c.updated_at.isoformat(),
                    "request_id": c.request_id,
                    "strategy": c.strategy,
                    "inst_id": c.inst_id,
                    "cl_ord_id": c.cl_ord_id,
                    "ord_id": c.ord_id,
                    "action": c.action,
                    "status": c.status,
                    "attempt": c.attempt,
                    "next_retry_at": c.next_retry_at.isoformat() if c.next_retry_at else None,
                    "last_error": c.last_error,
                }
                for c in rows
            ]

    # --- manual trading (goes through risk -> OMS) ---
    class ManualPlace(BaseModel):
        strategy: str = "manual"
        inst_id: str
        side: Literal["buy", "sell"]
        ord_type: Literal["limit", "market"] = "limit"
        td_mode: str = "cash"
        sz: float = Field(..., gt=0)
        px: Optional[float] = None
        cl_ord_id: Optional[str] = None
        reduce_only: bool = False

    class ManualCancel(BaseModel):
        strategy: str = "manual"
        inst_id: str
        cl_ord_id: str
        ord_id: Optional[str] = None

    class ManualAmend(BaseModel):
        strategy: str = "manual"
        inst_id: str
        cl_ord_id: str
        ord_id: Optional[str] = None
        new_px: Optional[float] = None
        new_sz: Optional[float] = None
        cxl_on_fail: bool = False

    def _mk_cl_id(strategy: str, inst_id: str) -> str:
        return f"{strategy}:{inst_id}:{utc_ms()}"

    async def _publish_intent(intent: OrderIntent):
        bus: Bus = bus_holder["bus"]
        await bus.publish("signal.order_intent", intent.model_dump())

    @app.post("/v1/manual/place")
    async def manual_place(req: ManualPlace):
        if req.ord_type == "limit" and req.px is None:
            raise HTTPException(status_code=400, detail="limit order requires px")
        cl_id = req.cl_ord_id or _mk_cl_id(req.strategy, req.inst_id)
        intent = OrderIntent(
            ts_ms=utc_ms(),
            strategy=req.strategy,
            inst_id=req.inst_id,
            action="place",
            side=req.side,
            ord_type=req.ord_type,
            td_mode=req.td_mode,
            sz=req.sz,
            px=req.px,
            cl_ord_id=cl_id,
            reduce_only=req.reduce_only,
            meta={"source": "api"},
        )
        await _publish_intent(intent)
        return {"ok": True, "cl_ord_id": cl_id}

    @app.post("/v1/manual/cancel")
    async def manual_cancel(req: ManualCancel):
        # Try to fill side/ord_type/td_mode from DB (for better downstream logs); fallback to placeholders.
        side = "buy"
        ord_type = "limit"
        td_mode = "cash"
        async with SessionLocal() as sess:
            res = await sess.execute(select(Order).where(Order.cl_ord_id == req.cl_ord_id))
            o = res.scalar_one_or_none()
            if o:
                side = o.side or side
                ord_type = o.ord_type or ord_type
                td_mode = o.td_mode or td_mode

        intent = OrderIntent(
            ts_ms=utc_ms(),
            strategy=req.strategy,
            inst_id=req.inst_id,
            action="cancel",
            side=side,  # placeholder ok
            ord_type=ord_type,
            td_mode=td_mode,
            sz=None,
            px=None,
            cl_ord_id=req.cl_ord_id,
            ord_id=req.ord_id,
            meta={"source": "api"},
        )
        await _publish_intent(intent)
        return {"ok": True, "cl_ord_id": req.cl_ord_id}

    @app.post("/v1/manual/amend")
    async def manual_amend(req: ManualAmend):
        if req.new_px is None and req.new_sz is None:
            raise HTTPException(status_code=400, detail="need new_px or new_sz")

        side = "buy"
        ord_type = "limit"
        td_mode = "cash"
        px = None
        sz = None
        async with SessionLocal() as sess:
            res = await sess.execute(select(Order).where(Order.cl_ord_id == req.cl_ord_id))
            o = res.scalar_one_or_none()
            if o:
                side = o.side or side
                ord_type = o.ord_type or ord_type
                td_mode = o.td_mode or td_mode
                px = o.px
                sz = o.sz

        intent = OrderIntent(
            ts_ms=utc_ms(),
            strategy=req.strategy,
            inst_id=req.inst_id,
            action="amend",
            side=side,
            ord_type=ord_type,
            td_mode=td_mode,
            sz=sz,
            px=px,
            new_px=req.new_px,
            new_sz=req.new_sz,
            cxl_on_fail=req.cxl_on_fail,
            cl_ord_id=req.cl_ord_id,
            ord_id=req.ord_id,
            meta={"source": "api"},
        )
        await _publish_intent(intent)
        return {"ok": True, "cl_ord_id": req.cl_ord_id}

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.services.api:app", host="0.0.0.0", port=8000, reload=False)
