from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import BigInteger, DateTime, Float, Index, Integer, JSON, Numeric, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class MarketTick(Base):
    __tablename__ = "market_ticks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)
    inst_id: Mapped[str] = mapped_column(String(64), index=True)
    last: Mapped[float] = mapped_column(Float)
    bid: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ask: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    raw: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


Index("ix_market_ticks_inst_ts", MarketTick.inst_id, MarketTick.ts)


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)

    strategy: Mapped[str] = mapped_column(String(64), index=True)
    inst_id: Mapped[str] = mapped_column(String(64), index=True)

    cl_ord_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    ord_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

    side: Mapped[str] = mapped_column(String(8))
    ord_type: Mapped[str] = mapped_column(String(16))
    td_mode: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)

    px: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    sz: Mapped[float] = mapped_column(Float)

    state: Mapped[str] = mapped_column(String(32), default="created")
    acc_fill_sz: Mapped[float] = mapped_column(Float, default=0.0)
    avg_px: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    raw: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)

    inst_id: Mapped[str] = mapped_column(String(64), index=True)
    side: Mapped[str] = mapped_column(String(8))
    px: Mapped[float] = mapped_column(Float)
    sz: Mapped[float] = mapped_column(Float)

    cl_ord_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    ord_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

    raw: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class EquitySnapshot(Base):
    __tablename__ = "equity_snapshots"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)
    total_eq_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    avail_eq_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    upl_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    raw: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class OrderCommand(Base):
    """
    OMS/Execution 层的命令队列表（生产级增强）：
    - 幂等：request_id 唯一
    - 可重试：attempt/next_retry_at/last_error
    - 可观测：status/response
    """

    __tablename__ = "order_commands"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)

    request_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)

    strategy: Mapped[str] = mapped_column(String(64), index=True)
    inst_id: Mapped[str] = mapped_column(String(64), index=True)
    cl_ord_id: Mapped[str] = mapped_column(String(64), index=True)
    ord_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

    action: Mapped[str] = mapped_column(String(16))  # place/cancel/amend
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)  # pending/sent/acked/failed/completed

    attempt: Mapped[int] = mapped_column(Integer, default=0)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    response: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


Index("ix_order_commands_cl_action", OrderCommand.cl_ord_id, OrderCommand.action)
