from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from app.core.utils import utc_ms
from app.schemas.events import MarketTickEvent, OrderIntent, OrderUpdate


def bps_to_frac(bps: float) -> float:
    return bps / 10000.0


@dataclass
class GridStrategy:
    """
    简化版智能网格（自建网格，非交易所网格产品）：
    - 以 tickers 的 last 作为参考价
    - 围绕 center_price 建立 levels * 2 的限价单
    - 价格偏离 center 超过 rebal_bps 重新建网格
    - 收到成交(fill)后，在下一格补反向单（基本的网格滚动）

    生产级需要补齐：
    - 撤单/改单状态机
    - 交易所最小下单量/价格精度（可接入 instruments 接口）
    - 多账户/多策略隔离
    """
    inst_id: str
    levels: int
    spread_bps: float
    order_usdt: float
    rebal_bps: float
    td_mode: str = "cash"

    strategy_id: str = "grid"

    center_price: Optional[float] = None
    open_orders: Dict[str, OrderIntent] = field(default_factory=dict)  # clOrdId -> intent

    def _mk_cl_id(self, side: str, lvl: int) -> str:
        return f"{self.strategy_id}:{self.inst_id}:{side}:{lvl}:{utc_ms()}"

    def _mk_intent(self, side: str, px: float, lvl: int) -> OrderIntent:
        # sz = usdt / price (rough)
        sz = max(self.order_usdt / max(px, 1e-9), 0.0001)
        cl_id = self._mk_cl_id(side, lvl)
        return OrderIntent(
            ts_ms=utc_ms(),
            strategy=self.strategy_id,
            inst_id=self.inst_id,
            action="place",
            side=side,  # buy/sell
            ord_type="limit",
            td_mode=self.td_mode,
            sz=sz,
            px=px,
            cl_ord_id=cl_id,
            meta={"lvl": lvl, "center": self.center_price},
        )

    def _desired_grid(self, last: float) -> List[OrderIntent]:
        if self.center_price is None:
            self.center_price = last

        spread = bps_to_frac(self.spread_bps)

        intents: List[OrderIntent] = []
        for i in range(1, self.levels + 1):
            buy_px = self.center_price * (1 - spread * i)
            sell_px = self.center_price * (1 + spread * i)
            intents.append(self._mk_intent("buy", buy_px, i))
            intents.append(self._mk_intent("sell", sell_px, i))
        return intents

    def on_tick(self, tick: MarketTickEvent) -> List[OrderIntent]:
        last = tick.last
        if self.center_price is None:
            self.center_price = last

        # Rebalance grid center if drifted too far
        drift = abs(last - self.center_price) / max(self.center_price, 1e-9)
        if drift >= bps_to_frac(self.rebal_bps):
            self.center_price = last
            # 触发重心：先尝试撤掉旧单（用 clOrdId 撤单）
            # NOTE: 这里仍是简化版，真实系统需维护 ordId/状态机。
            cancels: List[OrderIntent] = []
            for cl_id, old in list(self.open_orders.items()):
                cancels.append(
                    OrderIntent(
                        ts_ms=utc_ms(),
                        strategy=self.strategy_id,
                        inst_id=self.inst_id,
                        action="cancel",
                        side=old.side,
                        ord_type=old.ord_type,
                        td_mode=old.td_mode,
                        sz=old.sz,
                        px=old.px,
                        cl_ord_id=cl_id,
                        meta={"reason": "recenter"},
                    )
                )
            self.open_orders.clear()
            # 先撤单
            if cancels:
                return cancels

        desired = self._desired_grid(last)

        # Only place intents that are not already open (by price+side+lvl signature)
        existing_sig = {(i.side, round(i.px or 0, 6), i.meta.get("lvl")) for i in self.open_orders.values()}
        out: List[OrderIntent] = []
        for intent in desired:
            sig = (intent.side, round(intent.px or 0, 6), intent.meta.get("lvl"))
            if sig in existing_sig:
                continue
            self.open_orders[intent.cl_ord_id] = intent
            out.append(intent)
        return out

    def on_order_update(self, upd: OrderUpdate) -> List[OrderIntent]:
        # remove tracked order if we know it
        if upd.cl_ord_id and upd.cl_ord_id in self.open_orders and upd.state in ("filled", "canceled", "cancelled"):
            self.open_orders.pop(upd.cl_ord_id, None)

        # When filled, place opposite order one grid step away from avg_px
        if not upd.fill_sz or upd.fill_sz <= 0:
            return []

        px = upd.avg_px or upd.px
        if not px:
            return []

        spread = bps_to_frac(self.spread_bps)

        # infer which side was filled; if missing, ignore
        if upd.side not in ("buy", "sell"):
            return []

        if upd.side == "buy":
            next_px = px * (1 + spread)
            side = "sell"
        else:
            next_px = px * (1 - spread)
            side = "buy"

        # size: use filled size (approx)
        cl_id = f"{self.strategy_id}:{self.inst_id}:roll:{side}:{utc_ms()}"
        intent = OrderIntent(
            ts_ms=utc_ms(),
            strategy=self.strategy_id,
            inst_id=self.inst_id,
            action="place",
            side=side,
            ord_type="limit",
            td_mode=self.td_mode,
            sz=float(upd.fill_sz),
            px=next_px,
            cl_ord_id=cl_id,
            meta={"roll_from": upd.cl_ord_id or upd.ord_id, "center": self.center_price},
        )
        self.open_orders[cl_id] = intent
        return [intent]
