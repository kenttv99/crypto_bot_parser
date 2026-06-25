from __future__ import annotations

import argparse
import json
import os
import sys
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from threading import Event, Lock, Thread
from time import perf_counter_ns, sleep
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cryptobot_api import CryptoBotAPI
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


class TakePool:
    def __init__(self, cookie: str, wait_take_response: bool, size: int) -> None:
        self.cookie = cookie
        self.wait_take_response = wait_take_response
        self.size = size
        self.lock = Lock()
        self.clients: list[CryptoBotAPI] = []
        self.closed = False
        for _ in range(size):
            self._add_ready_client()

    def take(self, order_id: str) -> dict[str, Any] | None:
        api = self._pop()
        try:
            return api.take_payment(order_id)
        finally:
            api.close()
            if not self.wait_take_response:
                Thread(target=self._add_ready_client, daemon=True).start()

    def take_burst(self, order_id: str, attempts: int) -> list[dict[str, Any] | None]:
        if attempts == 1:
            return [self.take(order_id)]
        results: list[dict[str, Any] | None] = [None] * attempts
        errors: list[BaseException] = []
        error_lock = Lock()

        def run(index: int) -> None:
            try:
                results[index] = self.take(order_id)
            except BaseException as exc:
                with error_lock:
                    errors.append(exc)

        threads = [Thread(target=run, args=(index,), daemon=True) for index in range(attempts)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if len(errors) == attempts:
            raise RuntimeError(short_text(str(errors[0])))
        return results

    def close(self) -> None:
        with self.lock:
            self.closed = True
            clients, self.clients = self.clients, []
        for api in clients:
            api.close()

    def _pop(self) -> CryptoBotAPI:
        with self.lock:
            if self.clients:
                return self.clients.pop()
        return self._new_ready_client()

    def _add_ready_client(self) -> None:
        try:
            api = self._new_ready_client()
        except OSError:
            return
        with self.lock:
            if self.closed:
                api.close()
            else:
                self.clients.append(api)

    def _new_ready_client(self) -> CryptoBotAPI:
        api = CryptoBotAPI(self.cookie, wait_take_response=self.wait_take_response)
        api.open()
        return api


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
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.wait_take_response = wait_take_response
        self.socket_timeout = socket_timeout
        self.reconnect_delay = reconnect_delay
        self.take_pool = TakePool(cookie, wait_take_response, take_pool_size)
        self.take_attempts = take_attempts
        self.rate_limiter = rate_limiter
        self.log_skips = log_skips
        self.seen_ids: set[str] = set()
        self.seen_lock = Lock()
        self.file_lock = Lock()
        self.stop_event = Event()

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
        def on_record(record: dict[str, Any]) -> bool | None:
            if self.stop_event.is_set():
                return False
            if record.get("type") == "socketio_connect":
                payload = record.get("payload")
                sid = payload.get("sid", "") if isinstance(payload, dict) else ""
                edge_headers = record.get("edge_headers")
                cf_ray = edge_headers.get("cf-ray", "") if isinstance(edge_headers, dict) else ""
                cf_colo = edge_headers.get("cf-colo", "") if isinstance(edge_headers, dict) else ""
                print(f"worker={worker_id} socket connected sid={sid} cf_ray={cf_ray} colo={cf_colo}", flush=True)
                return None
            if record.get("type") != "socketio_event":
                return None
            received_at = perf_counter_ns()
            for order in extract_candidates(record):
                self._try_take(worker_id, order, received_at)
            return None

        return on_record

    def _try_take(self, worker_id: int, order: dict[str, Any], received_at: int) -> None:
        order_id = str(order.get("id", ""))
        if not order_id:
            return
        with self.seen_lock:
            if order_id in self.seen_ids:
                return
            self.seen_ids.add(order_id)
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
        take_started_at = perf_counter_ns()
        try:
            responses = self.take_pool.take_burst(order_id, allowed_attempts)
        except RuntimeError as exc:
            take_finished_at = perf_counter_ns()
            print(
                f"worker={worker_id} take failed id={order_id} amount={raw_amount} "
                f"decision={ms(take_started_at - received_at)} take={ms(take_finished_at - take_started_at)} error={short_text(str(exc))}",
                flush=True,
            )
            return
        take_finished_at = perf_counter_ns()
        result = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "worker_id": worker_id,
            "type": "taken_order" if any(response is not None for response in responses) else "take_request_sent",
            "order": order,
            "take_responses": responses,
            "take_attempts": allowed_attempts,
        }
        append_record(SAVE_PATH, result, self.file_lock)
        print(
            f"worker={worker_id} take {'order' if any(response is not None for response in responses) else 'request sent'} "
            f"id={order_id} amount={raw_amount} attempts={allowed_attempts} "
            f"decision={ms(take_started_at - received_at)} take={ms(take_finished_at - take_started_at)} total={ms(take_finished_at - received_at)}",
            flush=True,
        )
        for response in responses:
            if response is not None:
                print(json.dumps(response, ensure_ascii=False, indent=2), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connections", "-c", type=int, default=3, help="parallel websocket connections")
    parser.add_argument("--take-pool-size", type=int, default=16, help="preconnected HTTPS take connections")
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
    threads = [Thread(target=taker.run_worker, args=(worker_id,), daemon=False) for worker_id in range(1, args.connections + 1)]
    try:
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
