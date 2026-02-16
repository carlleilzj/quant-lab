from __future__ import annotations

import base64
import hashlib
import hmac
from datetime import datetime, timezone


def iso_timestamp() -> str:
    # REST API uses ISO8601 UTC with milliseconds, e.g. 2020-12-08T09:08:57.715Z
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def sign_rest(secret: str, timestamp: str, method: str, request_path: str, body: str = "") -> str:
    msg = f"{timestamp}{method.upper()}{request_path}{body}"
    mac = hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")


def sign_ws_login(secret: str, timestamp_unix_s: str) -> str:
    # WS login: sign=Base64(HMAC_SHA256(timestamp + 'GET' + '/users/self/verify', secret))
    msg = f"{timestamp_unix_s}GET/users/self/verify"
    mac = hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")
