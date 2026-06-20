from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = "https://app.send.tg"
ORIGIN = "https://app.send.tg"
REFERER = "https://app.send.tg/p2c/orders"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
TAKE_BAGGAGE = "sentry-environment=mainnet,sentry-public_key=abd78de7be54ff3fb9f6b677ba8c3ae6,sentry-trace_id=328cbff155d3474eabdaa190238c187b,sentry-sampled=true,sentry-sample_rand=0.2629407266783538,sentry-sample_rate=1"
TAKE_SENTRY_TRACE = "328cbff155d3474eabdaa190238c187b-af31f40ae5e4add9-1"


@dataclass(slots=True)
class CryptoBotAPI:
    cookie: str
    timeout: int = 30

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
        req = Request(BASE_URL + path, data=b"" if method == "POST" else None, method=method)
        for key, value in self._headers(baggage, sentry_trace).items():
            req.add_header(key, value)
        try:
            with urlopen(req, timeout=self.timeout) as response:
                body = response.read().decode("utf-8", "replace")
                return json.loads(body) if body else {}
        except HTTPError as exc:
            raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode('utf-8', 'replace')}") from exc
        except URLError as exc:
            raise RuntimeError(f"Request failed: {exc.reason}") from exc

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
