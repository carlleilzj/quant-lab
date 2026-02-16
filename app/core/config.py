from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    统一配置入口：
    - Docker 环境下来自 .env / 环境变量
    - 所有服务复用同一份配置（SERVICE_NAME 区分）

    生产级增强（本次升级）：
    - OMS：命令队列、幂等、重试/回滚窗口、expTime
    - Risk+Capital：基于 Redis/权益/敞口/预留资金的更严格风控
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # meta
    ENV: str = "dev"
    LOG_LEVEL: str = "INFO"
    SERVICE_NAME: str = "unknown"

    # infra
    NATS_URL: str = "nats://nats:4222"
    REDIS_URL: str = "redis://redis:6379/0"
    DATABASE_URL: str = "postgresql+asyncpg://quant:quant@postgres:5432/quant"
    DATABASE_URL_PG: str = "postgresql://quant:quant@postgres:5432/quant"

    # trading universe
    INSTRUMENTS: str = "BTC-USDT,ETH-USDT"
    ENABLED_STRATEGIES: str = "grid"

    # execution
    EXECUTION_MODE: str = "DRYRUN"  # DRYRUN or OKX
    ALLOW_LIVE_TRADING: bool = False

    # OMS (execution service)
    OMS_WORKER_BATCH: int = 10
    OMS_MAX_ATTEMPTS: int = 8
    OMS_RETRY_BASE_MS: int = 300
    OMS_RETRY_MAX_MS: int = 10_000

    # OKX
    OKX_SIMULATED_TRADING: int = 1
    OKX_REST_BASE_URL: str = "https://www.okx.com"
    OKX_WS_PUBLIC_URL: str = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
    OKX_WS_PRIVATE_URL: str = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
    OKX_WS_BUSINESS_URL: str = "wss://wspap.okx.com:8443/ws/v5/business?brokerId=9999"
    OKX_API_KEY: str | None = None
    OKX_SECRET_KEY: str | None = None
    OKX_PASSPHRASE: str | None = None

    # Optional: for order-related endpoints, add expTime header = now + OKX_EXP_TIME_MS
    OKX_EXP_TIME_MS: int = 0  # 0 disables

    # risk (basic)
    KILL_SWITCH: int = 1
    RISK_MAX_NOTIONAL_USDT: float = 200.0
    RISK_MAX_POSITION_USDT: float = 1000.0
    RISK_MAX_PRICE_DEVIATION_BPS: float = 80.0
    RISK_MAX_ORDERS_PER_MIN: int = 60  # legacy local limiter (still supported)

    # risk (production-grade: redis limiter / equity / drawdown / exposure / capital)
    RISK_USE_REDIS_RATE_LIMIT: bool = True
    RISK_RATE_LIMIT_GLOBAL_PER_MIN: int = 120
    RISK_RATE_LIMIT_PER_STRATEGY_PER_MIN: int = 90

    RISK_FALLBACK_EQUITY_USDT: float = 1000.0
    RISK_MAX_ORDER_PCT_OF_EQUITY: float = 0.05  # per order, as % of equity
    RISK_MAX_GROSS_EXPOSURE_PCT_OF_EQUITY: float = 2.0
    RISK_MAX_NET_EXPOSURE_PCT_OF_EQUITY: float = 1.0

    RISK_ENABLE_DRAWDOWN_KILL: bool = True
    RISK_MAX_DRAWDOWN_PCT: float = 0.2

    RISK_ENABLE_DAILY_LOSS_KILL: bool = True
    RISK_MAX_DAILY_LOSS_USDT: float = 200.0

    # capital reservation (open orders reserve)
    RISK_USE_CAPITAL_RESERVATION: bool = True
    # Margin estimation factors (heuristic)
    RISK_CASH_MARGIN_FACTOR: float = 1.0
    RISK_DERIV_MARGIN_FACTOR: float = 0.2

    # portfolio polling (OKX mode)
    PF_BALANCE_POLL_S: int = 15
    PF_POSITIONS_POLL_S: int = 15

    # grid params
    GRID_LEVELS: int = 5
    GRID_SPREAD_BPS: float = 30.0
    GRID_ORDER_USDT: float = 20.0
    GRID_REBALANCE_BPS: float = 80.0
    GRID_INSTRUMENTS: str = ""  # empty => use first in INSTRUMENTS

    # pairs params
    PAIRS_A: str = "BTC-USDT-SWAP"
    PAIRS_B: str = "ETH-USDT-SWAP"
    PAIRS_LOOKBACK: int = 200
    PAIRS_ENTRY_Z: float = 2.0
    PAIRS_EXIT_Z: float = 0.3
    PAIRS_LEG_USDT: float = 30.0
    PAIRS_TD_MODE: str = "cross"

    # cash&carry params
    CARRY_SPOT: str = "BTC-USDT"
    CARRY_FUT: str = "BTC-USDT-SWAP"
    CARRY_LEG_USDT: float = 30.0
    CARRY_MIN_BASIS_BPS: float = 50.0
    CARRY_EXIT_BASIS_BPS: float = 10.0
    CARRY_TD_MODE: str = "cross"

    # metrics
    METRICS_PORT: int = 9100

    # data sampling
    MD_DB_WRITE_EVERY_MS: int = 5000  # throttle market tick writes

    @property
    def instruments(self) -> List[str]:
        return [x.strip() for x in self.INSTRUMENTS.split(",") if x.strip()]

    @property
    def grid_instruments(self) -> List[str]:
        if self.GRID_INSTRUMENTS.strip():
            return [x.strip() for x in self.GRID_INSTRUMENTS.split(",") if x.strip()]
        ins = self.instruments
        return ins[:1]

    @property
    def enabled_strategies(self) -> List[str]:
        return [x.strip() for x in self.ENABLED_STRATEGIES.split(",") if x.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
