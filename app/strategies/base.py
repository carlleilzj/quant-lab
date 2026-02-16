from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional

from app.schemas.events import MarketTickEvent, OrderIntent, OrderUpdate


class BaseStrategy(ABC):
    strategy_id: str

    @abstractmethod
    def on_tick(self, tick: MarketTickEvent) -> List[OrderIntent]:
        ...

    def on_order_update(self, upd: OrderUpdate) -> List[OrderIntent]:
        return []
