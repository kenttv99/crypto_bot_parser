from __future__ import annotations

import random
import secrets
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

import httpx

BASE_URL = "https://app.send.tg"
ORIGIN = "https://app.send.tg"
REFERER = "https://app.send.tg/p2c/orders"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
SENTRY_RELEASE = "0.0.1037"
SENTRY_PUBLIC_KEY = "abd78de7be54ff3fb9f6b677ba8c3ae6"


@dataclass(slots=True)
class CryptoBotAPI:
    cookie: str
    timeout: float = 30
    wait_take_response: bool = True
    max_connections: int = 1
    _client: httpx.Client | None = field(default=None, init=False, repr=False)
    _client_lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def open(self) -> None:
        self._ensure_client()

    def close(self) -> None:
        with self._client_lock:
            client, self._client = self._client, None
        if client is not None:
            client.close()

    def get_onboarding_state(self) -> dict[str, Any]:
        return self._request_json("GET", "/internal/v1/p2c/onboarding/state")

    def take_payment(self, order_id: str) -> dict[str, Any] | None:
        path = f"/internal/v1/p2c/payments/take/{order_id}"
        baggage, sentry_trace = self._sentry_headers()
        if not self.wait_take_response:
            self._send_headers("POST", path, baggage=baggage, sentry_trace=sentry_trace)
            return None
        return self._request_json("POST", path, baggage=baggage, sentry_trace=sentry_trace)

    def _request_json(self, method: str, path: str, baggage: str | None = None, sentry_trace: str | None = None) -> dict[str, Any]:
        response = self._ensure_client().request(method, path, content=b"" if method == "POST" else None, headers=self._headers(baggage, sentry_trace))
        if 200 <= response.status_code < 300:
            return response.json() if response.content else {}
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    def _send_headers(self, method: str, path: str, baggage: str | None = None, sentry_trace: str | None = None) -> None:
        request = self._ensure_client().build_request(method, path, content=b"" if method == "POST" else None, headers=self._headers(baggage, sentry_trace))
        response = self._ensure_client().send(request, stream=True)
        response.close()

    def _ensure_client(self) -> httpx.Client:
        with self._client_lock:
            if self._client is None:
                self._client = httpx.Client(
                    base_url=BASE_URL,
                    http2=True,
                    timeout=httpx.Timeout(self.timeout),
                    limits=httpx.Limits(max_connections=self.max_connections, max_keepalive_connections=self.max_connections),
                )
            return self._client

    def _headers(self, baggage: str | None = None, sentry_trace: str | None = None) -> dict[str, str]:
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
            "cookie": self.cookie,
            "origin": ORIGIN,
            "priority": "u=1, i",
            "referer": REFERER,
            "sec-ch-ua": '"Not)A;Brand";v="24", "Microsoft Edge WebView2";v="149", "Microsoft Edge";v="149", "Chromium";v="149"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": USER_AGENT,
        }
        if baggage:
            headers["baggage"] = baggage
        if sentry_trace:
            headers["sentry-trace"] = sentry_trace
        return headers

    def _sentry_headers(self) -> tuple[str, str]:
        trace_id = secrets.token_hex(16)
        span_id = secrets.token_hex(8)
        sample_rand = random.random()
        baggage = (
            f"sentry-environment=mainnet,sentry-release={SENTRY_RELEASE},sentry-public_key={SENTRY_PUBLIC_KEY},"
            f"sentry-trace_id={trace_id},sentry-sampled=true,sentry-sample_rand={sample_rand},sentry-sample_rate=1"
        )
        return baggage, f"{trace_id}-{span_id}-1"
