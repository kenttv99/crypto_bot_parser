from __future__ import annotations

import json
import random
import secrets
import socket
import ssl
from collections import deque
from dataclasses import dataclass, field
from threading import Lock, Thread
from time import perf_counter_ns
from typing import Any
from urllib.parse import urlparse

from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import ConnectionTerminated, DataReceived, ResponseReceived, StreamEnded, StreamReset
import httpx

BASE_URL = "https://app.send.tg"
ORIGIN = "https://app.send.tg"
REFERER = "https://app.send.tg/p2c/orders"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
SENTRY_RELEASE = "0.0.1037"
SENTRY_PUBLIC_KEY = "abd78de7be54ff3fb9f6b677ba8c3ae6"


@dataclass(slots=True)
class TakeHTTPResult:
    status_code: int | None
    response_json: dict[str, Any] | None
    response_text: str
    headers: dict[str, str]
    error: str | None
    started_ns: int
    headers_ns: int
    finished_ns: int


@dataclass(slots=True)
class RawH2ResponseEvent:
    order_id: str
    stream_id: int
    status_code: int | None
    headers: dict[str, str]
    response_json: dict[str, Any] | None
    response_text: str
    error: str | None
    sent_ns: int
    received_ns: int


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

    def preconnect(self) -> None:
        self.get_onboarding_state()

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

    def take_payment_timed(self, order_id: str) -> TakeHTTPResult:
        started_ns = perf_counter_ns()
        response: httpx.Response | None = None
        try:
            response = self._send("POST", f"/internal/v1/p2c/payments/take/{order_id}", stream=True)
            headers_ns = perf_counter_ns()
            body = response.read() if self.wait_take_response else b""
            finished_ns = perf_counter_ns()
            text = body.decode(response.encoding or "utf-8", "replace") if body else ""
            parsed = self._parse_json(response) if body and self._is_json(response) else None
            error = None if 200 <= response.status_code < 300 else text or f"HTTP {response.status_code}"
            return TakeHTTPResult(response.status_code, parsed, text, self._telemetry_headers(response), error, started_ns, headers_ns, finished_ns)
        except Exception as exc:
            finished_ns = perf_counter_ns()
            return TakeHTTPResult(None, None, "", {}, str(exc), started_ns, finished_ns, finished_ns)
        finally:
            if response is not None:
                response.close()

    def _request_json(self, method: str, path: str, baggage: str | None = None, sentry_trace: str | None = None) -> dict[str, Any]:
        response = self._ensure_client().request(method, path, content=b"" if method == "POST" else None, headers=self._headers(baggage, sentry_trace))
        if 200 <= response.status_code < 300:
            return response.json() if response.content else {}
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    def _send_headers(self, method: str, path: str, baggage: str | None = None, sentry_trace: str | None = None) -> None:
        request = self._ensure_client().build_request(method, path, content=b"" if method == "POST" else None, headers=self._headers(baggage, sentry_trace))
        response = self._ensure_client().send(request, stream=True)
        response.close()

    def _send(self, method: str, path: str, stream: bool) -> httpx.Response:
        baggage, sentry_trace = self._sentry_headers()
        request = self._ensure_client().build_request(method, path, content=b"" if method == "POST" else None, headers=self._headers(baggage, sentry_trace))
        return self._ensure_client().send(request, stream=stream)

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

    def _telemetry_headers(self, response: httpx.Response) -> dict[str, str]:
        headers = {name: response.headers[name] for name in ("cf-ray", "server", "content-type") if name in response.headers}
        cf_ray = headers.get("cf-ray", "")
        if "-" in cf_ray:
            headers["cf-colo"] = cf_ray.rsplit("-", 1)[1]
        return headers

    def _is_json(self, response: httpx.Response) -> bool:
        return "application/json" in response.headers.get("content-type", "")

    def _parse_json(self, response: httpx.Response) -> dict[str, Any] | None:
        try:
            parsed = response.json()
        except ValueError:
            return None
        return parsed if isinstance(parsed, dict) else {"data": parsed}

    def _sentry_headers(self) -> tuple[str, str]:
        trace_id = secrets.token_hex(16)
        span_id = secrets.token_hex(8)
        sample_rand = random.random()
        baggage = (
            f"sentry-environment=mainnet,sentry-release={SENTRY_RELEASE},sentry-public_key={SENTRY_PUBLIC_KEY},"
            f"sentry-trace_id={trace_id},sentry-sampled=true,sentry-sample_rand={sample_rand},sentry-sample_rate=1"
        )
        return baggage, f"{trace_id}-{span_id}-1"


class RawH2TakeConnection:
    def __init__(self, connection_id: int, cookie: str, timeout: float) -> None:
        self.connection_id = connection_id
        self.timeout = timeout
        self.closed = False
        self.lock = Lock()
        self.header_builder = CryptoBotAPI(cookie)
        self.sock: ssl.SSLSocket | None = None
        self.conn: H2Connection | None = None
        self.reader: Thread | None = None
        self.pending: dict[int, dict[str, Any]] = {}
        self.events: deque[RawH2ResponseEvent] = deque(maxlen=1024)

    def open(self) -> None:
        self._connect_locked()

    def close(self) -> None:
        with self.lock:
            self.closed = True
            sock, self.sock = self.sock, None
        if sock is not None:
            self._close_socket(sock)

    def send_take(self, order_id: str) -> TakeHTTPResult:
        started_ns = perf_counter_ns()
        for _ in range(2):
            try:
                with self.lock:
                    if self.sock is None or self.conn is None:
                        self._connect_locked()
                    assert self.sock is not None and self.conn is not None
                    stream_id = self.conn.get_next_available_stream_id()
                    self.conn.send_headers(stream_id, self._headers(order_id), end_stream=True)
                    data = self.conn.data_to_send()
                    started_ns = perf_counter_ns()
                    self.sock.sendall(data)
                    sent_ns = perf_counter_ns()
                    self.pending[stream_id] = {"order_id": order_id, "sent_ns": sent_ns, "headers": {}, "body": bytearray()}
                return TakeHTTPResult(
                    None,
                    None,
                    "",
                    {"mode": "raw-h2", "connection_id": str(self.connection_id), "stream_id": str(stream_id)},
                    None,
                    started_ns,
                    sent_ns,
                    sent_ns,
                )
            except Exception as exc:
                error = str(exc)
                with self.lock:
                    self._reset_locked()
                continue
        finished_ns = perf_counter_ns()
        return TakeHTTPResult(None, None, "", {"mode": "raw-h2", "connection_id": str(self.connection_id)}, error, started_ns, finished_ns, finished_ns)

    def _headers(self, order_id: str) -> list[tuple[str, str]]:
        baggage, sentry_trace = self.header_builder._sentry_headers()
        headers = self.header_builder._headers(baggage, sentry_trace)
        path = f"/internal/v1/p2c/payments/take/{order_id}"
        result = [
            (":method", "POST"),
            (":scheme", "https"),
            (":authority", "app.send.tg"),
            (":path", path),
            ("content-length", "0"),
        ]
        result.extend((name.lower(), value) for name, value in headers.items() if name.lower() not in {"connection", "host", "content-length"})
        return result

    def _connect_locked(self) -> None:
        if self.closed:
            raise RuntimeError("raw h2 connection pool is closed")
        parsed = urlparse(BASE_URL)
        host = parsed.hostname or "app.send.tg"
        raw = socket.create_connection((host, parsed.port or 443), timeout=self.timeout)
        raw.settimeout(self.timeout)
        try:
            raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        context = ssl.create_default_context()
        context.set_alpn_protocols(["h2"])
        sock = context.wrap_socket(raw, server_hostname=host)
        if sock.selected_alpn_protocol() != "h2":
            self._close_socket(sock)
            raise RuntimeError("server did not negotiate h2")
        conn = H2Connection(config=H2Configuration(client_side=True, header_encoding="utf-8"))
        conn.initiate_connection()
        sock.sendall(conn.data_to_send())
        self.sock = sock
        self.conn = conn
        self.reader = Thread(target=self._read_loop, daemon=True)
        self.reader.start()

    def _read_loop(self) -> None:
        while True:
            with self.lock:
                if self.closed:
                    return
                sock = self.sock
            if sock is None:
                return
            try:
                data = sock.recv(65536)
                if not data:
                    raise OSError("h2 socket closed")
                with self.lock:
                    if sock is not self.sock or self.conn is None:
                        return
                    events = self.conn.receive_data(data)
                    for event in events:
                        self._handle_event_locked(event)
                    outbound = self.conn.data_to_send()
                    if outbound:
                        sock.sendall(outbound)
            except Exception:
                with self.lock:
                    if sock is self.sock:
                        self._reset_locked()
                return

    def _reset_locked(self) -> None:
        sock, self.sock = self.sock, None
        self.conn = None
        now = perf_counter_ns()
        for stream_id, meta in self.pending.items():
            self._append_event_locked(stream_id, meta, None, f"h2 connection reset", now)
        self.pending.clear()
        if sock is not None:
            self._close_socket(sock)

    def pop_events(self) -> list[RawH2ResponseEvent]:
        with self.lock:
            events = list(self.events)
            self.events.clear()
            return events

    def _handle_event_locked(self, event: Any) -> None:
        if isinstance(event, ResponseReceived):
            meta = self.pending.setdefault(event.stream_id, {"order_id": "", "sent_ns": 0, "headers": {}, "body": bytearray()})
            meta["headers"] = {name.lower(): value for name, value in event.headers}
            return
        if isinstance(event, DataReceived):
            meta = self.pending.setdefault(event.stream_id, {"order_id": "", "sent_ns": 0, "headers": {}, "body": bytearray()})
            meta["body"].extend(event.data)
            if self.conn is not None:
                self.conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
            return
        if isinstance(event, StreamEnded):
            meta = self.pending.pop(event.stream_id, None)
            if meta is not None:
                headers = meta.get("headers", {})
                status = self._status(headers)
                error = None if status is not None and 200 <= status < 300 else self._body_text(meta) or (f"HTTP {status}" if status is not None else "stream ended without status")
                self._append_event_locked(event.stream_id, meta, status, error, perf_counter_ns())
            return
        if isinstance(event, StreamReset):
            meta = self.pending.pop(event.stream_id, None)
            if meta is not None:
                self._append_event_locked(event.stream_id, meta, None, f"RST_STREAM {event.error_code}", perf_counter_ns())
            return
        if isinstance(event, ConnectionTerminated):
            now = perf_counter_ns()
            for stream_id, meta in self.pending.items():
                self._append_event_locked(stream_id, meta, None, f"GOAWAY {event.error_code}", now)
            self.pending.clear()

    def _append_event_locked(self, stream_id: int, meta: dict[str, Any], status: int | None, error: str | None, received_ns: int) -> None:
        text = self._body_text(meta)
        self.events.append(
            RawH2ResponseEvent(
                str(meta.get("order_id", "")),
                stream_id,
                status,
                {"connection_id": str(self.connection_id), **dict(meta.get("headers", {}))},
                self._parse_body_json(text),
                text,
                error,
                int(meta.get("sent_ns", 0)),
                received_ns,
            )
        )

    def _body_text(self, meta: dict[str, Any]) -> str:
        body = bytes(meta.get("body", b""))
        return body.decode("utf-8", "replace") if body else ""

    def _parse_body_json(self, text: str) -> dict[str, Any] | None:
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except ValueError:
            return None
        return parsed if isinstance(parsed, dict) else {"data": parsed}

    def _status(self, headers: dict[str, str]) -> int | None:
        value = headers.get(":status", "")
        try:
            return int(value)
        except ValueError:
            return None

    def _close_socket(self, sock: ssl.SSLSocket) -> None:
        try:
            sock.close()
        except OSError:
            pass


class RawH2TakePool:
    def __init__(self, cookie: str, size: int, timeout: float = 30) -> None:
        self.size = size
        self.lock = Lock()
        self.next_index = 0
        self.connections = [RawH2TakeConnection(index + 1, cookie, timeout) for index in range(size)]

    def open(self) -> None:
        errors: list[str] = []
        for connection in self.connections:
            try:
                connection.open()
            except Exception as exc:
                errors.append(f"{connection.connection_id}:{exc}")
        if errors:
            self.close()
            raise RuntimeError(f"raw h2 preconnect failed: {', '.join(errors[:5])}")

    def close(self) -> None:
        for connection in self.connections:
            connection.close()

    def take_payment_sent(self, order_id: str) -> TakeHTTPResult:
        with self.lock:
            connection = self.connections[self.next_index % self.size]
            self.next_index += 1
        return connection.send_take(order_id)

    def take_payment_burst_sent(self, order_id: str, attempts: int) -> list[TakeHTTPResult]:
        return [self.take_payment_sent(order_id) for _ in range(attempts)]

    def pop_events(self) -> list[RawH2ResponseEvent]:
        events: list[RawH2ResponseEvent] = []
        for connection in self.connections:
            events.extend(connection.pop_events())
        return events
