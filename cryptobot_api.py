from __future__ import annotations

import http.client
import json
import socket
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

BASE_URL = "https://app.send.tg"
BASE_HOST = urlparse(BASE_URL).hostname or "app.send.tg"
ORIGIN = "https://app.send.tg"
REFERER = "https://app.send.tg/p2c/orders"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
TAKE_BAGGAGE = "sentry-environment=mainnet,sentry-public_key=abd78de7be54ff3fb9f6b677ba8c3ae6,sentry-trace_id=328cbff155d3474eabdaa190238c187b,sentry-sampled=true,sentry-sample_rand=0.2629407266783538,sentry-sample_rate=1"
TAKE_SENTRY_TRACE = "328cbff155d3474eabdaa190238c187b-af31f40ae5e4add9-1"


@dataclass(slots=True)
class CryptoBotAPI:
    cookie: str
    timeout: int = 30
    _connection: http.client.HTTPSConnection | None = field(default=None, init=False, repr=False)

    def open(self) -> None:
        self._ensure_connection()

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    def get_onboarding_state(self) -> dict[str, Any]:
        return self._request_json("GET", "/internal/v1/p2c/onboarding/state")

    def take_payment(self, order_id: str) -> dict[str, Any]:
        return self._request_json(
            "POST",
            f"/internal/v1/p2c/payments/take/{order_id}",
            baggage=TAKE_BAGGAGE,
            sentry_trace=TAKE_SENTRY_TRACE,
        )

    def _request_json(self, method: str, path: str, baggage: str | None = None, sentry_trace: str | None = None) -> dict[str, Any]:
        headers = self._headers(baggage, sentry_trace)
        body = b"" if method == "POST" else None
        for retry in range(2):
            conn = self._ensure_connection()
            try:
                conn.request(method, path, body=body, headers=headers)
                response = conn.getresponse()
                payload = response.read().decode("utf-8", "replace")
                if 200 <= response.status < 300:
                    return json.loads(payload) if payload else {}
                raise RuntimeError(f"HTTP {response.status}: {payload}")
            except (http.client.HTTPException, OSError, ConnectionError) as exc:
                self._drop_connection()
                if retry == 0:
                    continue
                raise RuntimeError(f"Request failed: {exc}") from exc
        raise RuntimeError("Request failed")

    def _headers(self, baggage: str | None = None, sentry_trace: str | None = None) -> dict[str, str]:
        headers = {
            "accept": "application/json, text/plain, */*",
            "cookie": self.cookie,
            "origin": ORIGIN,
            "referer": REFERER,
            "user-agent": USER_AGENT,
        }
        if baggage:
            headers["baggage"] = baggage
        if sentry_trace:
            headers["sentry-trace"] = sentry_trace
        return headers

    def _ensure_connection(self) -> http.client.HTTPSConnection:
        if self._connection is None:
            self._connection = http.client.HTTPSConnection(BASE_HOST, timeout=self.timeout)
        if self._connection.sock is None:
            self._connection.connect()
            try:
                self._connection.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
        return self._connection

    def _drop_connection(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None
