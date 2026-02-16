from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_ms() -> int:
    return int(utc_now().timestamp() * 1000)


def safe_float(x: Any, default: float | None = None) -> float | None:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default
