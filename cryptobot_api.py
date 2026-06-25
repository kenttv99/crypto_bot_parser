from __future__ import annotations

import http.client
import json
import random
import socket
import secrets
from dataclasses import dataclass, field
from threading import Lock, Thread
from typing import Any
from urllib.parse import urlparse

BASE_URL = "https://app.send.tg"
BASE_HOST = urlparse(BASE_URL).hostname or "app.send.tg"
ORIGIN = "https://app.send.tg"
REFERER = "https://app.send.tg/p2c/orders"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
SENTRY_RELEASE = "0.0.1037"
SENTRY_PUBLIC_KEY = "abd78de7be54ff3fb9f6b677ba8c3ae6"


@dataclass(slots=True)
class CryptoBotAPI:
    cookie: str
    timeout: int = 30
    wait_take_response: bool = True
    preconnect_after_send: bool = False
    _connection: http.client.HTTPSConnection | None = field(default=None, init=False, repr=False)
    _connection_lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def open(self) -> None:
        self._ensure_connection()

    def close(self) -> None:
        self._drop_connection()

    def get_onboarding_state(self) -> dict[str, Any]:
        return self._request_json("GET", "/internal/v1/p2c/onboarding/state")

    def take_payment(self, order_id: str) -> dict[str, Any] | None:
        path = f"/internal/v1/p2c/payments/take/{order_id}"
        baggage, sentry_trace = self._sentry_headers()
        if not self.wait_take_response:
            self._send("POST", path, baggage=baggage, sentry_trace=sentry_trace)
            return None
        return self._request_json(
            "POST",
            path,
            baggage=baggage,
            sentry_trace=sentry_trace,
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

    def _send(self, method: str, path: str, baggage: str | None = None, sentry_trace: str | None = None) -> None:
        headers = self._headers(baggage, sentry_trace)
        body = b"" if method == "POST" else None
        conn = self._ensure_connection()
        try:
            conn.request(method, path, body=body, headers=headers)
        finally:
            self._drop_connection()
            if self.preconnect_after_send:
                Thread(target=self._preconnect_silently, daemon=True).start()

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

    def _ensure_connection(self) -> http.client.HTTPSConnection:
        with self._connection_lock:
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
        with self._connection_lock:
            if self._connection is not None:
                try:
                    self._connection.close()
                finally:
                    self._connection = None

    def _preconnect_silently(self) -> None:
        try:
            self.open()
        except OSError:
            self._drop_connection()
