from __future__ import annotations

import json
from typing import Any, Optional
from urllib.parse import urlencode

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.utils import utc_ms
from app.exchanges.okx.auth import iso_timestamp, sign_rest


class OkxRestClient:
    """Minimal async REST client for OKX API v5.

    Notes (production-ish):
    - Built-in HTTP retry is intentionally small; the OMS layer (execution service) will do higher-level retries.
    - Optional exp_time_ms: if set, adds 'expTime' header to order-related endpoints to bound request validity.
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str],
        secret_key: Optional[str],
        passphrase: Optional[str],
        simulated_trading: int = 1,
        timeout_s: float = 10.0,
        exp_time_ms: Optional[int] = None,  # relative window (ms)
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.simulated_trading = str(int(simulated_trading))
        self.exp_time_ms = exp_time_ms

        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout_s)

    def _auth_headers(self, method: str, request_path: str, body_json: Optional[dict[str, Any]] = None, *, add_exp_time: bool = False) -> dict[str, str]:
        if not (self.api_key and self.secret_key and self.passphrase):
            raise RuntimeError("OKX API key/secret/passphrase not configured")

        timestamp = iso_timestamp()
        body = "" if body_json is None else json.dumps(body_json, separators=(",", ":"), ensure_ascii=False)

        sign = sign_rest(self.secret_key, timestamp, method, request_path, body)

        headers: dict[str, str] = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            # 0=live, 1=simulated
            "x-simulated-trading": self.simulated_trading,
            "Content-Type": "application/json",
        }

        if add_exp_time and self.exp_time_ms and self.exp_time_ms > 0:
            headers["expTime"] = str(int(utc_ms() + int(self.exp_time_ms)))

        return headers

    def _should_add_exp_time(self, path: str) -> bool:
        # Order operation endpoints only.
        return path.startswith("/api/v5/trade/") and (
            path.endswith("/order")
            or path.endswith("/cancel-order")
            or path.endswith("/amend-order")
            or path.endswith("/batch-orders")
            or path.endswith("/cancel-batch-orders")
            or path.endswith("/amend-batch-orders")
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.3, min=0.3, max=2))
    async def request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
        auth: bool = False,
    ) -> dict[str, Any]:
        method = method.upper()
        request_path = path
        if params:
            qs = urlencode({k: v for k, v in params.items() if v is not None})
            request_path = f"{path}?{qs}"

        headers = {}
        if auth:
            headers = self._auth_headers(method, request_path, body_json=body, add_exp_time=self._should_add_exp_time(path))

        logger.debug("OKX {} {}", method, request_path)
        resp = await self.client.request(method, request_path, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()
        return data

    # --- public ---
    async def get_server_time(self) -> dict[str, Any]:
        return await self.request("GET", "/api/v5/public/time", auth=False)

    # --- trade ---
    async def place_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self.request("POST", "/api/v5/trade/order", body=payload, auth=True)

    async def cancel_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self.request("POST", "/api/v5/trade/cancel-order", body=payload, auth=True)

    async def amend_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self.request("POST", "/api/v5/trade/amend-order", body=payload, auth=True)

    async def get_order(self, *, inst_id: str, ord_id: Optional[str] = None, cl_ord_id: Optional[str] = None) -> dict[str, Any]:
        params: dict[str, Any] = {"instId": inst_id, "ordId": ord_id, "clOrdId": cl_ord_id}
        return await self.request("GET", "/api/v5/trade/order", params=params, auth=True)

    async def get_orders_pending(self, *, inst_type: Optional[str] = None, inst_id: Optional[str] = None) -> dict[str, Any]:
        params: dict[str, Any] = {"instType": inst_type, "instId": inst_id}
        return await self.request("GET", "/api/v5/trade/orders-pending", params=params, auth=True)

    # --- account ---
    async def get_balance(self, ccy: Optional[str] = None) -> dict[str, Any]:
        params = {"ccy": ccy} if ccy else None
        return await self.request("GET", "/api/v5/account/balance", params=params, auth=True)

    async def get_positions(self, *, inst_type: Optional[str] = None, inst_id: Optional[str] = None) -> dict[str, Any]:
        params: dict[str, Any] = {"instType": inst_type, "instId": inst_id}
        return await self.request("GET", "/api/v5/account/positions", params=params, auth=True)

    async def close(self):
        await self.client.aclose()
