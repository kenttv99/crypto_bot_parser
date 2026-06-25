from __future__ import annotations

import os
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from time import perf_counter_ns
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cryptobot_api import CryptoBotAPI
from cryptobot_socket import CryptoBotSocketClient
from runtime_config import env, load_env_file


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


def ms(ns: int) -> str:
    return f"{ns / 1_000_000:.3f}ms"


def main() -> None:
    load_env_file(ROOT / ".env.parametrs")
    cookie = env("COOKIE_HEADER")
    min_limit = parse_limit("MIN_LIMIT_RUB")
    max_limit = parse_limit("MAX_LIMIT_RUB")
    ensure_limits(min_limit, max_limit)
    started_at = perf_counter_ns()
    api = CryptoBotAPI(cookie)
    api.open()

    def on_record(record: dict[str, Any]) -> bool | None:
        now = perf_counter_ns()
        if record.get("type") == "socketio_connect":
            print(f"connect={ms(now - started_at)}", flush=True)
            return None
        if record.get("type") != "socketio_event":
            return None
        for order in extract_candidates(record):
            order_id = str(order.get("id", ""))
            if not order_id:
                continue
            raw_amount = str(order.get("in_amount", "")).strip()
            if not raw_amount:
                continue
            try:
                if not amount_in_range(raw_amount, min_limit, max_limit):
                    continue
            except InvalidOperation:
                continue
            take_started_at = perf_counter_ns()
            try:
                api.take_payment(order_id)
            except RuntimeError:
                take_finished_at = perf_counter_ns()
                print(
                    f"decision={ms(take_started_at - now)} take={ms(take_finished_at - take_started_at)} total={ms(take_finished_at - now)}",
                    flush=True,
                )
                continue
            take_finished_at = perf_counter_ns()
            print(
                f"decision={ms(take_started_at - now)} take={ms(take_finished_at - take_started_at)} total={ms(take_finished_at - now)}",
                flush=True,
            )
            return False
        return None

    try:
        CryptoBotSocketClient(cookie).run(on_record=on_record)
    finally:
        api.close()


if __name__ == "__main__":
    main()
