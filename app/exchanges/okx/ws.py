from __future__ import annotations

import asyncio
import json
import time
from typing import Any, AsyncIterator, Optional

import websockets
from loguru import logger

from app.exchanges.okx.auth import sign_ws_login


class OkxWsClient:
    """
    Minimal OKX WS client:
    - public: subscribe tickers/trades/books...
    - private: login + subscribe orders channel

    Notes:
    - OKX recommends sending string 'ping' if no messages are received for <30s, and expect 'pong'.
    """

    def __init__(
        self,
        url: str,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        passphrase: Optional[str] = None,
        name: str = "okx-ws",
        idle_ping_s: int = 25,
    ):
        self.url = url
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.name = name
        self.idle_ping_s = idle_ping_s
        self._ws = None
        self._last_msg_ts = time.time()

    async def connect(self) -> None:
        self._ws = await websockets.connect(self.url, ping_interval=None, close_timeout=3)
        self._last_msg_ts = time.time()
        logger.info("[{}] ws connected: {}", self.name, self.url)

    async def send_json(self, obj: dict[str, Any]) -> None:
        assert self._ws is not None
        await self._ws.send(json.dumps(obj, separators=(",", ":"), ensure_ascii=False))

    async def recv(self) -> str:
        assert self._ws is not None
        msg = await self._ws.recv()
        self._last_msg_ts = time.time()
        return msg

    async def login(self) -> None:
        if not (self.api_key and self.secret_key and self.passphrase):
            raise RuntimeError("OKX WS login requires api_key/secret_key/passphrase")

        ts = str(int(time.time()))
        sign = sign_ws_login(self.secret_key, ts)
        await self.send_json(
            {
                "op": "login",
                "args": [
                    {
                        "apiKey": self.api_key,
                        "passphrase": self.passphrase,
                        "timestamp": ts,
                        "sign": sign,
                    }
                ],
            }
        )
        # Wait for login response
        raw = await self.recv()
        data = json.loads(raw)
        if data.get("event") != "login" or data.get("code") != "0":
            raise RuntimeError(f"OKX WS login failed: {data}")
        logger.info("[{}] ws login ok", self.name)

    async def subscribe(self, args: list[dict[str, Any]], req_id: str = "1") -> None:
        await self.send_json({"id": req_id, "op": "subscribe", "args": args})

    async def messages(self) -> AsyncIterator[dict[str, Any] | str]:
        """
        Yield incoming messages. This helper also sends 'ping' on idle.
        """
        assert self._ws is not None

        async def _ping_loop():
            while True:
                await asyncio.sleep(self.idle_ping_s)
                if time.time() - self._last_msg_ts >= self.idle_ping_s:
                    try:
                        await self._ws.send("ping")
                    except Exception:
                        return

        ping_task = asyncio.create_task(_ping_loop())
        try:
            while True:
                msg = await self.recv()
                if msg == "pong":
                    continue
                try:
                    yield json.loads(msg)
                except Exception:
                    yield msg
        finally:
            ping_task.cancel()

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
