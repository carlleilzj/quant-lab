from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from app.core.utils import utc_ms
from app.schemas.events import MarketTickEvent, OrderIntent


def frac_to_bps(frac: float) -> float:
    return frac * 10000.0


@dataclass
class CashCarryStrategy:
    """
    简化版期现套利（cash & carry）：
    - basis = (fut - spot) / spot
    - basis_bps >= min_basis_bps: 买现货 + 卖合约
    - basis_bps <= exit_basis_bps: 平仓（卖现货 + 买合约）

    注意：真实环境要考虑：
    - 资金费率（永续）
    - 手续费/滑点/保证金/借币成本
    - 交割合约到期与展期
    """
    spot: str
    fut: str
    leg_usdt: float
    min_basis_bps: float
    exit_basis_bps: float
    td_mode: str = "cross"
    strategy_id: str = "cash_carry"

    prices: Dict[str, float] = field(default_factory=dict)
    in_trade: bool = False

    def _mk(self, inst_id: str, side: str, px: float) -> OrderIntent:
        sz = max(self.leg_usdt / max(px, 1e-9), 0.0001)
        return OrderIntent(
            ts_ms=utc_ms(),
            strategy=self.strategy_id,
            inst_id=inst_id,
            action="place",
            side=side,
            ord_type="market",
            td_mode=self.td_mode if inst_id.endswith("SWAP") or inst_id.endswith("FUTURES") else "cash",
            sz=sz,
            px=None,
            cl_ord_id=f"{self.strategy_id}:{inst_id}:{side}:{utc_ms()}",
            meta={"kind": "carry"},
        )

    def on_tick(self, tick: MarketTickEvent) -> List[OrderIntent]:
        self.prices[tick.inst_id] = tick.last
        if self.spot not in self.prices or self.fut not in self.prices:
            return []

        spot_px = self.prices[self.spot]
        fut_px = self.prices[self.fut]
        basis = (fut_px - spot_px) / max(spot_px, 1e-9)
        basis_bps = frac_to_bps(basis)

        intents: List[OrderIntent] = []
        if (not self.in_trade) and basis_bps >= self.min_basis_bps:
            # enter: buy spot, sell fut
            intents.append(self._mk(self.spot, "buy", spot_px))
            intents.append(self._mk(self.fut, "sell", fut_px))
            self.in_trade = True
        elif self.in_trade and basis_bps <= self.exit_basis_bps:
            # exit: sell spot, buy fut
            intents.append(self._mk(self.spot, "sell", spot_px))
            intents.append(self._mk(self.fut, "buy", fut_px))
            self.in_trade = False

        return intents
