from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cryptobot_api import CryptoBotAPI
from cryptobot_socket import CryptoBotSocketClient


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        item = line.strip()
        if not item or item.startswith("#") or "=" not in item:
            continue
        key, value = item.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def env(name: str) -> str:
    value = os.getenv(name, "")
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


load_env_file(ROOT / ".env.parametrs")
COOKIE_HEADER = env("COOKIE_HEADER")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", action="store_true", help="fetch onboarding state")
    parser.add_argument("--take", action="store_true", help="take payment order")
    parser.add_argument("--socket", action="store_true", help="listen p2c websocket")
    parser.add_argument("--save-json", default="", help="save websocket events to json")
    parser.add_argument("--order-id", default="", help="order id for --take")
    args = parser.parse_args()
    if sum((args.state, args.take, args.socket)) != 1:
        parser.error("use exactly one of --state, --take or --socket")
    if args.take and not args.order_id:
        parser.error("missing --order-id")
    if args.state:
        print(json.dumps(CryptoBotAPI(COOKIE_HEADER).get_onboarding_state(), ensure_ascii=False, indent=2))
    elif args.take:
        print(json.dumps(CryptoBotAPI(COOKIE_HEADER).take_payment(args.order_id), ensure_ascii=False, indent=2))
    else:
        CryptoBotSocketClient(COOKIE_HEADER, save_json_path=args.save_json or None).run()


if __name__ == "__main__":
    main()
