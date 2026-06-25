from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cryptobot_api import CryptoBotAPI
from cryptobot_socket import CryptoBotSocketClient
from runtime_config import env, load_env_file

SAVE_PATH = ROOT / "data" / "taken_orders.json"
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


def append_record(path: Path, record: dict[str, Any]) -> None:
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


def main() -> None:
    load_env_file(ROOT / ".env.parametrs")
    cookie = env("COOKIE_HEADER")
    min_limit = parse_limit("MIN_LIMIT_RUB")
    max_limit = parse_limit("MAX_LIMIT_RUB")
    ensure_limits(min_limit, max_limit)
    wait_take_response = env_bool("WAIT_TAKE_RESPONSE", True)
    api = CryptoBotAPI(cookie, wait_take_response=wait_take_response)
    api.open()
    seen_ids: set[str] = set()

    def on_record(record: dict[str, Any]) -> bool | None:
        if record.get("type") == "socketio_connect":
            payload = record.get("payload")
            sid = payload.get("sid", "") if isinstance(payload, dict) else ""
            print(f"socket connected sid={sid}", flush=True)
            return None
        if record.get("type") != "socketio_event":
            return None
        for order in extract_candidates(record):
            order_id = str(order.get("id", ""))
            if not order_id or order_id in seen_ids:
                continue
            seen_ids.add(order_id)
            raw_amount = str(order.get("in_amount", "")).strip()
            if not raw_amount:
                print(f"skip order id={order_id} reason=no_amount", flush=True)
                continue
            try:
                if not amount_in_range(raw_amount, min_limit, max_limit):
                    print(f"skip order id={order_id} amount={raw_amount} payload={short_text(order)}", flush=True)
                    continue
            except InvalidOperation:
                print(f"skip order id={order_id} amount={raw_amount} payload={short_text(order)}", flush=True)
                continue
            try:
                response = api.take_payment(order_id)
            except RuntimeError as exc:
                print(f"take failed id={order_id} amount={raw_amount} error={short_text(str(exc))}", flush=True)
                continue
            result = {
                "received_at": datetime.now(timezone.utc).isoformat(),
                "type": "taken_order" if response is not None else "take_request_sent",
                "order": order,
                "take_response": response,
            }
            append_record(SAVE_PATH, result)
            print(f"take {'order' if response is not None else 'request sent'} id={order_id} amount={raw_amount}", flush=True)
            if response is not None:
                print(json.dumps(response, ensure_ascii=False, indent=2), flush=True)
        return None

    try:
        CryptoBotSocketClient(cookie).run(on_record=on_record)
    finally:
        api.close()


if __name__ == "__main__":
    main()
