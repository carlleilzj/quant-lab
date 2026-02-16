from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class MarketTickEvent(BaseModel):
    ts_ms: int
    inst_id: str
    last: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    raw: dict[str, Any] = Field(default_factory=dict)


class OrderIntent(BaseModel):
    """
    Strategy -> Risk

    生产级增强点：
    - action 增加 amend（改单）
    - ord_id 可选（部分交易所改单/撤单可用 ordId 或 clOrdId）
    - sz 改为 Optional：cancel/amend 场景下并不一定需要
    - new_px/new_sz/cxl_on_fail：用于改单（amend），同时兼容旧版用 px/sz 传新值
    - request_id：可显式提供幂等键（推荐），否则由 OMS 自动推导
    """

    ts_ms: int
    strategy: str
    inst_id: str
    action: Literal["place", "cancel", "amend"] = "place"

    # 对于 cancel/amend，这些字段可能不重要，但为了兼容既有策略仍保留
    side: Literal["buy", "sell"]
    ord_type: Literal["limit", "market"] = "limit"
    td_mode: str = "cash"  # cash / cross / isolated
    pos_side: Optional[str] = None  # long/short (if account is long-short mode)

    # place: sz 必填；cancel/amend: 可为空（OMS 会尽力从 DB/缓存补齐）
    sz: Optional[float] = None
    px: Optional[float] = None

    # amend: 推荐用 new_px/new_sz（若为空则 fallback 到 px/sz）
    new_px: Optional[float] = None
    new_sz: Optional[float] = None
    cxl_on_fail: bool = False  # amend 失败是否自动撤单（OKX 参数：cxlOnFail）

    # 幂等键（建议由上游生成；例如 UUID / ULID / hash(strategy+signal)）
    request_id: Optional[str] = None

    cl_ord_id: str
    ord_id: Optional[str] = None
    reduce_only: bool = False

    meta: dict[str, Any] = Field(default_factory=dict)


class RiskDecision(BaseModel):
    ts_ms: int
    approved: bool
    reason: str = ""
    intent: OrderIntent


class ExecutionAck(BaseModel):
    ts_ms: int
    strategy: str
    inst_id: str
    cl_ord_id: str
    ord_id: Optional[str] = None

    # 新增：便于 OMS/运维追踪（不影响旧逻辑）
    action: Optional[str] = None
    request_id: Optional[str] = None

    side: Optional[str] = None
    ord_type: Optional[str] = None
    td_mode: Optional[str] = None
    px: Optional[float] = None
    sz: Optional[float] = None
    success: bool = True
    msg: str = ""
    raw: dict[str, Any] = Field(default_factory=dict)


class OrderUpdate(BaseModel):
    ts_ms: int
    inst_id: str
    cl_ord_id: Optional[str] = None
    ord_id: Optional[str] = None
    state: str
    side: Optional[str] = None
    px: Optional[float] = None
    sz: Optional[float] = None
    acc_fill_sz: Optional[float] = None
    fill_sz: Optional[float] = None
    avg_px: Optional[float] = None
    raw: dict[str, Any] = Field(default_factory=dict)


class EquityEvent(BaseModel):
    ts_ms: int
    total_eq_usd: Optional[float] = None
    avail_eq_usd: Optional[float] = None
    upl_usd: Optional[float] = None
    raw: dict[str, Any] = Field(default_factory=dict)
