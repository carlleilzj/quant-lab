from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from math import sqrt
from typing import Deque, Dict, List, Optional, Tuple

from app.core.utils import utc_ms
from app.schemas.events import MarketTickEvent, OrderIntent


def mean(xs):
    return sum(xs) / max(len(xs), 1)


def stdev(xs):
    if len(xs) < 2:
        return 0.0
    m = mean(xs)
    v = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return sqrt(v)


@dataclass
class PairsStrategy:
    """
    简化版配对交易（比值 z-score）：
    - 维护 ratio = price(A)/price(B)
    - z > entry: 做空 A + 做多 B
    - z < -entry: 做多 A + 做空 B
    - |z| < exit: 平仓

    注意：要做空，建议使用 SWAP/FUTURES 并保证账户模式允许。
    """
    inst_a: str
    inst_b: str
    lookback: int
    entry_z: float
    exit_z: float
    leg_usdt: float
    td_mode: str = "cross"
    strategy_id: str = "pairs"

    prices: Dict[str, float] = field(default_factory=dict)
    ratios: Deque[float] = field(default_factory=lambda: deque(maxlen=200))
    position: int = 0  # -1 shortA/longB, +1 longA/shortB, 0 flat

    def _mk_order(self, inst_id: str, side: str, px: float, usdt: float) -> OrderIntent:
        sz = max(usdt / max(px, 1e-9), 0.0001)
        return OrderIntent(
            ts_ms=utc_ms(),
            strategy=self.strategy_id,
            inst_id=inst_id,
            action="place",
            side=side,
            ord_type="market",
            td_mode=self.td_mode,
            sz=sz,
            px=None,
            cl_ord_id=f"{self.strategy_id}:{inst_id}:{side}:{utc_ms()}",
            meta={"kind": "pairs"},
        )

    def on_tick(self, tick: MarketTickEvent) -> List[OrderIntent]:
        self.prices[tick.inst_id] = tick.last
        if self.inst_a not in self.prices or self.inst_b not in self.prices:
            return []

        pa = self.prices[self.inst_a]
        pb = self.prices[self.inst_b]
        ratio = pa / max(pb, 1e-9)
        if self.ratios.maxlen != self.lookback:
            self.ratios = deque(self.ratios, maxlen=self.lookback)
        self.ratios.append(ratio)

        if len(self.ratios) < max(30, int(self.lookback * 0.5)):
            return []

        m = mean(self.ratios)
        s = stdev(self.ratios)
        if s <= 1e-9:
            return []
        z = (ratio - m) / s

        intents: List[OrderIntent] = []

        # entry
        if self.position == 0 and z >= self.entry_z:
            # short A, long B
            intents.append(self._mk_order(self.inst_a, "sell", pa, self.leg_usdt))
            intents.append(self._mk_order(self.inst_b, "buy", pb, self.leg_usdt))
            self.position = -1
        elif self.position == 0 and z <= -self.entry_z:
            # long A, short B
            intents.append(self._mk_order(self.inst_a, "buy", pa, self.leg_usdt))
            intents.append(self._mk_order(self.inst_b, "sell", pb, self.leg_usdt))
            self.position = +1

        # exit
        elif self.position != 0 and abs(z) <= self.exit_z:
            if self.position == -1:
                # close: buy A, sell B
                intents.append(self._mk_order(self.inst_a, "buy", pa, self.leg_usdt))
                intents.append(self._mk_order(self.inst_b, "sell", pb, self.leg_usdt))
            else:
                intents.append(self._mk_order(self.inst_a, "sell", pa, self.leg_usdt))
                intents.append(self._mk_order(self.inst_b, "buy", pb, self.leg_usdt))
            self.position = 0

        return intents
