from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from threading import Event, Lock, Thread
from time import perf_counter_ns
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cryptobot_api import CryptoBotAPI
from cryptobot_socket import CryptoBotSocketClient
from runtime_config import env, load_env_file

SAVE_PATH = ROOT / "data" / "taken_orders_parallel.json"
LOG_LIMIT = 180


def parse_limit(name: str) -> Decimal | None:
    value = os.getenv(name, "").strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise RuntimeError(f"Invalid decimal value in {name}: {value}") from exc


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
        items: list[dict[str, Any]] = []
        if path.exists():
            try:
                items = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                items = []
            if not isinstance(items, list):
                items = []
        path.parent.mkdir(parents=True, exist_ok=True)
        items.append(record)
        path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def short_text(value: Any, limit: int = LOG_LIMIT) -> str:
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":")) if isinstance(value, (dict, list)) else str(value)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def ms(ns: int) -> str:
    return f"{ns / 1_000_000:.3f}ms"


class ParallelTaker:
    def __init__(self, cookie: str, min_limit: Decimal | None, max_limit: Decimal | None, wait_take_response: bool) -> None:
        self.cookie = cookie
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.wait_take_response = wait_take_response
        self.seen_ids: set[str] = set()
        self.seen_lock = Lock()
        self.file_lock = Lock()
        self.stop_event = Event()

    def run_worker(self, worker_id: int) -> None:
        api = CryptoBotAPI(
            self.cookie,
            wait_take_response=self.wait_take_response,
            preconnect_after_send=not self.wait_take_response,
        )
        try:
            api.open()
            CryptoBotSocketClient(self.cookie).run(on_record=self._handler(worker_id, api))
        except Exception as exc:
            print(f"worker={worker_id} stopped error={short_text(str(exc))}", flush=True)
        finally:
            api.close()

    def _handler(self, worker_id: int, api: CryptoBotAPI):
        def on_record(record: dict[str, Any]) -> bool | None:
            if self.stop_event.is_set():
                return False
            if record.get("type") == "socketio_connect":
                payload = record.get("payload")
                sid = payload.get("sid", "") if isinstance(payload, dict) else ""
                print(f"worker={worker_id} socket connected sid={sid}", flush=True)
                return None
            if record.get("type") != "socketio_event":
                return None
            received_at = perf_counter_ns()
            for order in extract_candidates(record):
                self._try_take(worker_id, api, order, received_at)
            return None

        return on_record

    def _try_take(self, worker_id: int, api: CryptoBotAPI, order: dict[str, Any], received_at: int) -> None:
        order_id = str(order.get("id", ""))
        if not order_id:
            return
        with self.seen_lock:
            if order_id in self.seen_ids:
                return
            self.seen_ids.add(order_id)
        raw_amount = str(order.get("in_amount", "")).strip()
        if not raw_amount:
            print(f"worker={worker_id} skip order id={order_id} reason=no_amount", flush=True)
            return
        try:
            if not amount_in_range(raw_amount, self.min_limit, self.max_limit):
                print(f"worker={worker_id} skip order id={order_id} amount={raw_amount} payload={short_text(order)}", flush=True)
                return
        except InvalidOperation:
            print(f"worker={worker_id} skip order id={order_id} amount={raw_amount} payload={short_text(order)}", flush=True)
            return
        take_started_at = perf_counter_ns()
        try:
            response = api.take_payment(order_id)
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
            "type": "taken_order" if response is not None else "take_request_sent",
            "order": order,
            "take_response": response,
        }
        append_record(SAVE_PATH, result, self.file_lock)
        print(
            f"worker={worker_id} take {'order' if response is not None else 'request sent'} id={order_id} amount={raw_amount} "
            f"decision={ms(take_started_at - received_at)} take={ms(take_finished_at - take_started_at)} total={ms(take_finished_at - received_at)}",
            flush=True,
        )
        if response is not None:
            print(json.dumps(response, ensure_ascii=False, indent=2), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connections", "-c", type=int, default=3, help="parallel websocket connections")
    args = parser.parse_args()
    if args.connections < 1:
        parser.error("--connections must be greater than 0")

    load_env_file(ROOT / ".env.parametrs")
    cookie = env("COOKIE_HEADER")
    min_limit = parse_limit("MIN_LIMIT_RUB")
    max_limit = parse_limit("MAX_LIMIT_RUB")
    ensure_limits(min_limit, max_limit)
    wait_take_response = env_bool("WAIT_TAKE_RESPONSE", True)
    taker = ParallelTaker(cookie, min_limit, max_limit, wait_take_response)
    threads = [Thread(target=taker.run_worker, args=(worker_id,), daemon=False) for worker_id in range(1, args.connections + 1)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        taker.stop_event.set()
        print("stopping workers", flush=True)


if __name__ == "__main__":
    main()
