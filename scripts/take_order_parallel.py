from __future__ import annotations

import argparse
import json
import os
import sys
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Lock, Thread
from time import perf_counter_ns, sleep, time_ns
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cryptobot_api import CryptoBotAPI, RawH2TakePool, TakeHTTPResult
from cryptobot_socket import CryptoBotSocketClient
from runtime_config import env, load_env_file

SAVE_PATH = ROOT / "data" / "taken_orders_parallel.jsonl"
LOG_LIMIT = 180


def parse_limit(name: str) -> Decimal | None:
    value = os.getenv(name, "").strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise RuntimeError(f"Invalid decimal value in {name}: {value}") from exc


def parse_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        result = int(value)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer value in {name}: {value}") from exc
    if result < 1:
        raise RuntimeError(f"{name} must be greater than 0")
    return result


def ensure_limits(min_limit: Decimal | None, max_limit: Decimal | None) -> None:
    if min_limit is not None and max_limit is not None and min_limit > max_limit:
        raise RuntimeError("MIN_LIMIT_RUB cannot be greater than MAX_LIMIT_RUB")


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name, "").strip().lower()
    return default if not value else value in {"1", "true", "yes", "on"}


def amount_in_range(amount: str, min_limit: Decimal | None, max_limit: Decimal | None) -> bool:
    value = Decimal(amount)
    return (min_limit is None or value >= min_limit) and (max_limit is None or value <= max_limit)


def extract_candidates(record: dict[str, Any]):
    if record.get("type") != "socketio_event" or record.get("event") not in {"list:snapshot", "list:update"}:
        return
    payload = record.get("payload")
    if not isinstance(payload, list):
        return
    if record["event"] == "list:snapshot":
        for item in payload:
            if isinstance(item, dict):
                yield item
        return
    for item in payload:
        if isinstance(item, dict) and item.get("op") == "add" and isinstance(item.get("data"), dict):
            yield item["data"]


def append_record(path: Path, record: dict[str, Any], lock: Lock) -> None:
    with lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def short_text(value: Any, limit: int = LOG_LIMIT) -> str:
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":")) if isinstance(value, (dict, list)) else str(value)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def ms(ns: int) -> str:
    return f"{ns / 1_000_000:.3f}ms"


def order_created_ns(order_id: str) -> int | None:
    if len(order_id) < 8:
        return None
    try:
        return int(order_id[:8], 16) * 1_000_000_000
    except ValueError:
        return None


def age_ms(now_ns: int, created_ns: int | None) -> float | None:
    return round((now_ns - created_ns) / 1_000_000, 3) if created_ns is not None else None


def age_text(now_ns: int, created_ns: int | None) -> str:
    value = age_ms(now_ns, created_ns)
    return "unknown" if value is None else f"{value:.3f}ms"


def merge_set_cookie(cookie_header: str, set_cookie: str) -> str:
    pair = set_cookie.split(";", 1)[0].strip()
    if not pair or "=" not in pair:
        return cookie_header
    name, value = pair.split("=", 1)
    items = []
    replaced = False
    for item in cookie_header.split(";"):
        part = item.strip()
        if not part or "=" not in part:
            continue
        key, current = part.split("=", 1)
        if key == name:
            items.append(f"{name}={value}")
            replaced = True
        else:
            items.append(f"{key}={current}")
    if not replaced:
        items.append(f"{name}={value}")
    return "; ".join(items)


@dataclass(slots=True)
class TakeCandidate:
    worker_id: int
    order: dict[str, Any]
    amount: str
    attempts: int
    created_ns: int | None
    received_wall_ns: int
    received_ns: int
    queued_ns: int
    ws_edge_headers: dict[str, str]


class TakePool:
    def __init__(self, cookie: str, wait_take_response: bool, size: int) -> None:
        self.api: CryptoBotAPI | None = None
        self.raw: RawH2TakePool | None = None
        if wait_take_response:
            self.api = CryptoBotAPI(cookie, wait_take_response=True, max_connections=size)
            self.api.open()
            self.api.preconnect()
        else:
            self.raw = RawH2TakePool(cookie, size)
            self.raw.open()

    def take(self, order_id: str) -> TakeHTTPResult:
        if self.raw is not None:
            return self.raw.take_payment_sent(order_id)
        if self.api is None:
            raise RuntimeError("take pool is not initialized")
        return self.api.take_payment_timed(order_id)

    def take_burst(self, order_id: str, attempts: int) -> list[TakeHTTPResult]:
        if self.raw is not None:
            return self.raw.take_payment_burst_sent(order_id, attempts)
        if attempts == 1:
            return [self.take(order_id)]
        results: list[TakeHTTPResult | None] = [None] * attempts

        def run(index: int) -> None:
            results[index] = self.take(order_id)

        threads = [Thread(target=run, args=(index,), daemon=True) for index in range(attempts)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return [result for result in results if result is not None]

    def close(self) -> None:
        if self.api is not None:
            self.api.close()
        if self.raw is not None:
            self.raw.close()

    def update_cookie_header(self, cookie: str) -> None:
        if self.api is not None:
            self.api.update_cookie_header(cookie)
        if self.raw is not None:
            self.raw.update_cookie_header(cookie)

    def pop_events(self):
        return self.raw.pop_events() if self.raw is not None else []


class TakeRateLimiter:
    SECOND_NS = 1_000_000_000
    FIVE_MINUTES_NS = 300 * SECOND_NS

    def __init__(self, per_second: int, per_5m_primary: int, per_5m_secondary: int) -> None:
        self.limits = ((self.SECOND_NS, per_second), (self.FIVE_MINUTES_NS, per_5m_primary), (self.FIVE_MINUTES_NS, per_5m_secondary))
        self.events: deque[int] = deque()
        self.lock = Lock()

    def acquire(self, requested: int) -> int:
        now = perf_counter_ns()
        with self.lock:
            self._purge(now)
            allowed = min(requested, *(limit - self._count_since(now - window) for window, limit in self.limits))
            for _ in range(max(0, allowed)):
                self.events.append(now)
            return max(0, allowed)

    def _purge(self, now: int) -> None:
        oldest = now - max(window for window, _ in self.limits)
        while self.events and self.events[0] <= oldest:
            self.events.popleft()

    def _count_since(self, threshold: int) -> int:
        return sum(1 for event in self.events if event > threshold)


class ParallelTaker:
    def __init__(
        self,
        cookie: str,
        min_limit: Decimal | None,
        max_limit: Decimal | None,
        wait_take_response: bool,
        socket_timeout: int,
        reconnect_delay: float,
        take_pool_size: int,
        take_attempts: int,
        rate_limiter: TakeRateLimiter,
        log_skips: bool,
    ) -> None:
        self.cookie = cookie
        self.cookie_lock = Lock()
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.wait_take_response = wait_take_response
        self.socket_timeout = socket_timeout
        self.reconnect_delay = reconnect_delay
        self.take_pool = TakePool(cookie, wait_take_response, take_pool_size)
        self.take_attempts = take_attempts
        self.rate_limiter = rate_limiter
        self.log_skips = log_skips
        self.queue: Queue[TakeCandidate] = Queue()
        self.seen_ids: dict[str, tuple[int, int]] = {}
        self.seen_lock = Lock()
        self.file_lock = Lock()
        self.stop_event = Event()

    def run_take_worker(self, take_worker_id: int) -> None:
        while not self.stop_event.is_set():
            try:
                candidate = self.queue.get(timeout=0.2)
            except Empty:
                continue
            try:
                self._take_candidate(take_worker_id, candidate)
            except Exception as exc:
                print(f"take_worker={take_worker_id} error={short_text(str(exc))}", flush=True)
            finally:
                self.queue.task_done()

    def run_response_logger(self) -> None:
        while not self.stop_event.is_set():
            for event in self.take_pool.pop_events():
                record = {
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "type": "raw_h2_response",
                    "order_id": event.order_id,
                    "stream_id": event.stream_id,
                    "status_code": event.status_code,
                    "headers": event.headers,
                    "error": event.error,
                    "response_json": event.response_json,
                    "response_text": short_text(event.response_text) if event.response_text and event.response_json is None else "",
                    "timing_ms": {
                        "response_after_send": round((event.received_ns - event.sent_ns) / 1_000_000, 3) if event.sent_ns else None,
                    },
                }
                append_record(SAVE_PATH, record, self.file_lock)
                response_after_send = round((event.received_ns - event.sent_ns) / 1_000_000, 3) if event.sent_ns else None
                print(
                    f"raw_h2 response id={event.order_id} stream={event.stream_id} "
                    f"status={event.status_code} response_after_send={response_after_send}ms error={short_text(event.error or '')}",
                    flush=True,
                )
            sleep(0.05)

    def run_worker(self, worker_id: int) -> None:
        reconnects = 0
        while not self.stop_event.is_set():
            try:
                CryptoBotSocketClient(self.cookie, timeout=self.socket_timeout).run(on_record=self._handler(worker_id))
                reconnects += 1
                if not self.stop_event.is_set():
                    print(f"worker={worker_id} socket closed reconnect={reconnects}", flush=True)
            except Exception as exc:
                reconnects += 1
                if not self.stop_event.is_set():
                    print(f"worker={worker_id} reconnect={reconnects} error={short_text(str(exc))}", flush=True)
            if not self.stop_event.is_set():
                sleep(self.reconnect_delay)

    def _handler(self, worker_id: int):
        ws_edge_headers: dict[str, str] = {}

        def on_record(record: dict[str, Any]) -> bool | None:
            nonlocal ws_edge_headers
            if self.stop_event.is_set():
                return False
            if record.get("type") == "socketio_connect":
                payload = record.get("payload")
                sid = payload.get("sid", "") if isinstance(payload, dict) else ""
                edge_headers = record.get("edge_headers")
                cf_ray = edge_headers.get("cf-ray", "") if isinstance(edge_headers, dict) else ""
                cf_colo = edge_headers.get("cf-colo", "") if isinstance(edge_headers, dict) else ""
                print(f"worker={worker_id} socket connected sid={sid} cf_ray={cf_ray} colo={cf_colo}", flush=True)
                ws_edge_headers = edge_headers if isinstance(edge_headers, dict) else {}
                set_cookie = ws_edge_headers.get("set-cookie", "")
                if set_cookie:
                    with self.cookie_lock:
                        updated = merge_set_cookie(self.cookie, set_cookie)
                        if updated != self.cookie:
                            self.cookie = updated
                            self.take_pool.update_cookie_header(updated)
                return None
            if record.get("type") != "socketio_event":
                return None
            received_at = int(record.get("received_perf_ns") or perf_counter_ns())
            received_wall_at = int(record.get("received_wall_ns") or time_ns())
            for order in extract_candidates(record):
                self._try_enqueue(worker_id, order, received_at, received_wall_at, ws_edge_headers)
            return None

        return on_record

    def _try_enqueue(self, worker_id: int, order: dict[str, Any], received_at: int, received_wall_at: int, ws_edge_headers: dict[str, str]) -> None:
        order_id = str(order.get("id", ""))
        if not order_id:
            return
        created_at = order_created_ns(order_id)
        with self.seen_lock:
            seen = self.seen_ids.get(order_id)
            if seen is not None:
                first_worker, first_seen_ns = seen
                if self.log_skips:
                    print(f"worker={worker_id} duplicate id={order_id} first_worker={first_worker} lag={ms(received_at - first_seen_ns)}", flush=True)
                return
            self.seen_ids[order_id] = (worker_id, received_at)
        raw_amount = str(order.get("in_amount", "")).strip()
        if not raw_amount:
            if self.log_skips:
                print(f"worker={worker_id} skip order id={order_id} reason=no_amount", flush=True)
            return
        try:
            if not amount_in_range(raw_amount, self.min_limit, self.max_limit):
                if self.log_skips:
                    print(f"worker={worker_id} skip order id={order_id} amount={raw_amount} payload={short_text(order)}", flush=True)
                return
        except InvalidOperation:
            if self.log_skips:
                print(f"worker={worker_id} skip order id={order_id} amount={raw_amount} payload={short_text(order)}", flush=True)
            return
        allowed_attempts = self.rate_limiter.acquire(self.take_attempts)
        if allowed_attempts < 1:
            print(f"worker={worker_id} skip order id={order_id} amount={raw_amount} reason=take_rate_limit", flush=True)
            return
        self.queue.put(TakeCandidate(worker_id, order, raw_amount, allowed_attempts, created_at, received_wall_at, received_at, perf_counter_ns(), ws_edge_headers))

    def _take_candidate(self, take_worker_id: int, candidate: TakeCandidate) -> None:
        order_id = str(candidate.order.get("id", ""))
        take_started_at = perf_counter_ns()
        take_started_wall_at = time_ns()
        responses = self.take_pool.take_burst(order_id, candidate.attempts)
        take_finished_at = max((response.finished_ns for response in responses), default=perf_counter_ns())
        ok = any(response.error is None for response in responses)
        request_timing_name = "take" if self.wait_take_response else "send"
        timing_ms = {
            "age_receive": age_ms(candidate.received_wall_ns, candidate.created_ns),
            "age_send_start": age_ms(take_started_wall_at, candidate.created_ns),
            "queue": round((take_started_at - candidate.queued_ns) / 1_000_000, 3),
            "decision": round((candidate.queued_ns - candidate.received_ns) / 1_000_000, 3),
            request_timing_name: round((take_finished_at - take_started_at) / 1_000_000, 3),
            "total": round((take_finished_at - candidate.received_ns) / 1_000_000, 3),
        }
        result = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "worker_id": candidate.worker_id,
            "take_worker_id": take_worker_id,
            "type": "taken_order" if ok and self.wait_take_response else "take_request_sent" if ok else "take_failed",
            "order": candidate.order,
            "ws_edge_headers": candidate.ws_edge_headers,
            "take_attempts": candidate.attempts,
            "timing_ms": timing_ms,
            "take_responses": [self._response_record(response) for response in responses],
        }
        append_record(SAVE_PATH, result, self.file_lock)
        action = "take" if self.wait_take_response else "send"
        print(
            f"worker={candidate.worker_id} take_worker={take_worker_id} {action} {'ok' if ok else 'failed'} "
            f"id={order_id} amount={candidate.amount} attempts={candidate.attempts} "
            f"age_receive={age_text(candidate.received_wall_ns, candidate.created_ns)} "
            f"age_send={age_text(take_started_wall_at, candidate.created_ns)} "
            f"queue={ms(take_started_at - candidate.queued_ns)} decision={ms(candidate.queued_ns - candidate.received_ns)} "
            f"{request_timing_name}={ms(take_finished_at - take_started_at)} total={ms(take_finished_at - candidate.received_ns)}",
            flush=True,
        )
        for response in responses:
            if response.response_json is not None:
                print(json.dumps(response.response_json, ensure_ascii=False, indent=2), flush=True)
            elif response.error:
                print(f"take response id={order_id} status={response.status_code} error={short_text(response.error)}", flush=True)

    def _response_record(self, response: TakeHTTPResult) -> dict[str, Any]:
        timing = (
            {
                "send": round((response.finished_ns - response.started_ns) / 1_000_000, 3),
                "total": round((response.finished_ns - response.started_ns) / 1_000_000, 3),
            }
            if response.headers.get("mode") == "raw-h2"
            else {
                "headers": round((response.headers_ns - response.started_ns) / 1_000_000, 3),
                "body": round((response.finished_ns - response.headers_ns) / 1_000_000, 3),
                "total": round((response.finished_ns - response.started_ns) / 1_000_000, 3),
            }
        )
        return {
            "status_code": response.status_code,
            "headers": response.headers,
            "error": response.error,
            "response_json": response.response_json,
            "response_text": short_text(response.response_text) if response.response_text and response.response_json is None else "",
            "timing_ms": timing,
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connections", "-c", type=int, default=3, help="parallel websocket connections")
    parser.add_argument("--take-pool-size", type=int, default=16, help="preconnected HTTPS take connections")
    parser.add_argument("--take-workers", type=int, default=4, help="parallel order take workers")
    parser.add_argument("--take-attempts", type=int, default=1, help="parallel take POST requests per matching order")
    parser.add_argument("--socket-timeout", type=int, default=120, help="websocket socket timeout seconds")
    parser.add_argument("--reconnect-delay", type=float, default=0.2, help="delay before reconnect seconds")
    parser.add_argument("--start-delay", type=float, default=0.05, help="delay between worker starts seconds")
    parser.add_argument("--log-skips", action="store_true", help="log skipped orders")
    args = parser.parse_args()
    if args.connections < 1:
        parser.error("--connections must be greater than 0")
    if args.take_pool_size < 1:
        parser.error("--take-pool-size must be greater than 0")
    if args.take_workers < 1:
        parser.error("--take-workers must be greater than 0")
    if args.take_attempts < 1:
        parser.error("--take-attempts must be greater than 0")
    if args.take_attempts > args.take_pool_size:
        parser.error("--take-attempts cannot be greater than --take-pool-size")
    if args.socket_timeout < 1:
        parser.error("--socket-timeout must be greater than 0")
    if args.reconnect_delay < 0:
        parser.error("--reconnect-delay cannot be negative")
    if args.start_delay < 0:
        parser.error("--start-delay cannot be negative")

    load_env_file(ROOT / ".env.parametrs")
    cookie = env("COOKIE_HEADER")
    min_limit = parse_limit("MIN_LIMIT_RUB")
    max_limit = parse_limit("MAX_LIMIT_RUB")
    ensure_limits(min_limit, max_limit)
    wait_take_response = env_bool("WAIT_TAKE_RESPONSE", True)
    rate_limiter = TakeRateLimiter(
        parse_int("TAKE_LIMIT_PER_SECOND", 5),
        parse_int("TAKE_LIMIT_5M_PRIMARY", 200),
        parse_int("TAKE_LIMIT_5M_SECONDARY", 500),
    )
    taker = ParallelTaker(
        cookie,
        min_limit,
        max_limit,
        wait_take_response,
        args.socket_timeout,
        args.reconnect_delay,
        args.take_pool_size,
        args.take_attempts,
        rate_limiter,
        args.log_skips,
    )
    take_threads = [Thread(target=taker.run_take_worker, args=(worker_id,), daemon=True) for worker_id in range(1, args.take_workers + 1)]
    response_thread = Thread(target=taker.run_response_logger, daemon=True)
    threads = [Thread(target=taker.run_worker, args=(worker_id,), daemon=False) for worker_id in range(1, args.connections + 1)]
    try:
        response_thread.start()
        for thread in take_threads:
            thread.start()
        for thread in threads:
            thread.start()
            sleep(args.start_delay)
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        taker.stop_event.set()
        print("stopping workers", flush=True)
    finally:
        taker.take_pool.close()


if __name__ == "__main__":
    main()
