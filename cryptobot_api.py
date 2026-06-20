from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass(slots=True)
class CryptoBotAPI:
    cookie: str
    base_url: str = "https://app.send.tg"
    timeout: int = 30
    referer: str = "https://app.send.tg/p2c/orders"
    origin: str = "https://app.send.tg"
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
    accept_language: str = "ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7"
    baggage: str | None = None
    sentry_trace: str | None = None
    extra_headers: dict[str, str] = field(default_factory=dict)

    def get_onboarding_state(self) -> dict[str, Any]:
        return self._get_json("/internal/v1/p2c/onboarding/state")

    def take_payment(self, order_id: str) -> dict[str, Any]:
        return self._post_json(f"/internal/v1/p2c/payments/take/{order_id}")

    def _get_json(self, path: str) -> dict[str, Any]:
        req = Request(self.base_url + path, method="GET")
        req.add_header("content-type", "application/json")
        for key, value in self._headers().items():
            req.add_header(key, value)
        try:
            with urlopen(req, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8", "replace"))
        except HTTPError as exc:
            raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode('utf-8', 'replace')}") from exc
        except URLError as exc:
            raise RuntimeError(f"Request failed: {exc.reason}") from exc

    def _post_json(self, path: str) -> dict[str, Any]:
        req = Request(self.base_url + path, data=b"", method="POST")
        for key, value in self._headers().items():
            req.add_header(key, value)
        try:
            with urlopen(req, timeout=self.timeout) as response:
                body = response.read().decode("utf-8", "replace")
                return json.loads(body) if body else {}
        except HTTPError as exc:
            raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode('utf-8', 'replace')}") from exc
        except URLError as exc:
            raise RuntimeError(f"Request failed: {exc.reason}") from exc

    def _headers(self) -> dict[str, str]:
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": self.accept_language,
            "cookie": self.cookie,
            "origin": self.origin,
            "priority": "u=1, i",
            "referer": self.referer,
            "sec-ch-ua": '"Not)A;Brand";v="24", "Microsoft Edge WebView2";v="149", "Microsoft Edge";v="149", "Chromium";v="149"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "Windows",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": self.user_agent,
        }
        if self.baggage:
            headers["baggage"] = self.baggage
        if self.sentry_trace:
            headers["sentry-trace"] = self.sentry_trace
        headers.update(self.extra_headers)
        return headers
