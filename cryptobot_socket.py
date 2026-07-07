from __future__ import annotations

import base64
import json
import os
import socket
import ssl
import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter_ns, time_ns
from typing import Any, Callable
from urllib.parse import urlparse

SOCKET_ORIGIN = "https://app.send.tg"
SOCKET_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"


@dataclass(slots=True)
class CryptoBotSocketClient:
    cookie: str
    save_json_path: str | None = None
    initialized: bool = False
    url: str = "wss://app.send.tg/internal/v1/p2c-socket/?EIO=4&transport=websocket"
    timeout: int = 30
    edge_headers: dict[str, str] | None = None

    def run(self, on_record: Callable[[dict[str, Any]], bool | None] | None = None) -> None:
        conn = self._connect()
        try:
            self._negotiate(conn)
            while True:
                message = self._read_message(conn)
                if message is None:
                    return
                if self._handle_message(conn, message, on_record) is False:
                    return
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _connect(self) -> ssl.SSLSocket:
        parsed = urlparse(self.url)
        if parsed.scheme != "wss":
            raise ValueError("url must use wss://")
        conn = ssl.create_default_context().wrap_socket(
            socket.create_connection((parsed.hostname, parsed.port or 443), timeout=self.timeout),
            server_hostname=parsed.hostname,
        )
        try:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        return conn

    def _negotiate(self, conn: ssl.SSLSocket) -> None:
        parsed = urlparse(self.url)
        key = base64.b64encode(os.urandom(16)).decode()
        headers = [
            f"GET {parsed.path}?{parsed.query} HTTP/1.1",
            f"Host: {parsed.hostname}",
            "Connection: Upgrade",
            "Upgrade: websocket",
            f"Origin: {SOCKET_ORIGIN}",
            "Sec-WebSocket-Version: 13",
            f"User-Agent: {SOCKET_USER_AGENT}",
            f"Cookie: {self.cookie}",
            f"Sec-WebSocket-Key: {key}",
        ]
        conn.sendall(("\r\n".join(headers) + "\r\n\r\n").encode())
        response = self._read_http_headers(conn)
        self.edge_headers = self._parse_upgrade_headers(response)
        if b" 101 " not in response:
            raise RuntimeError("websocket upgrade failed")
        conn.sendall(self._ws_frame("40"))

    def _read_message(self, conn: ssl.SSLSocket) -> str | None:
        opcode, payload = self._read_frame(conn)
        if opcode == 8:
            return None
        if opcode == 9:
            conn.sendall(self._frame(payload, 10))
            return ""
        return payload.decode("utf-8", "replace") if opcode == 1 else ""

    def _handle_message(self, conn: ssl.SSLSocket, message: str, on_record: Callable[[dict[str, Any]], bool | None] | None = None) -> bool | None:
        if not message:
            return
        received_perf_ns = perf_counter_ns()
        received_wall_ns = time_ns()
        fast_record = self._fast_record(message, received_perf_ns, received_wall_ns)
        fast_result = on_record(fast_record) if fast_record is not None and on_record is not None else None
        if fast_result is False:
            return False
        if fast_record is not None and not self.save_json_path:
            return None
        parsed = self._parse_message(message)
        parsed["received_perf_ns"] = received_perf_ns
        parsed["received_wall_ns"] = received_wall_ns
        if parsed["type"] == "socketio_connect" and not self.initialized:
            conn.sendall(self._ws_frame('42["list:initialize"]'))
            self.initialized = True
        if parsed["type"] == "ping":
            conn.sendall(self._ws_frame("3"))
        result = None if fast_record is not None else on_record(parsed) if on_record is not None else None
        self._append_json(parsed)
        if result is False:
            return False
        return None

    def _fast_record(self, message: str, received_perf_ns: int, received_wall_ns: int) -> dict[str, Any] | None:
        fast_orders = self._fast_orders(message)
        if not fast_orders:
            return None
        return {
            "received_at": self._now(),
            "received_perf_ns": received_perf_ns,
            "received_wall_ns": received_wall_ns,
            "type": "socketio_event",
            "event": "list:update",
            "fast_orders": fast_orders,
            "raw": message,
        }

    def _parse_message(self, message: str) -> dict[str, Any]:
        if message.startswith("0{"):
            return {"received_at": self._now(), "type": "engine_open", **json.loads(message[1:]), "raw": message}
        if message.startswith("40{"):
            record = {"received_at": self._now(), "type": "socketio_connect", "payload": json.loads(message[2:]), "raw": message}
            if self.edge_headers:
                record["edge_headers"] = self.edge_headers
            return record
        if message == "2":
            return {"received_at": self._now(), "type": "ping"}
        if message == "3":
            return {"received_at": self._now(), "type": "pong"}
        if message.startswith("44{"):
            return {"received_at": self._now(), "type": "socketio_error", "payload": json.loads(message[2:]), "raw": message}
        if message.startswith("42"):
            event, payload = self._parse_socketio_event(message)
            record = {"received_at": self._now(), "type": "socketio_event", "event": event, "payload": payload, "raw": message}
            fast_orders = self._fast_orders(message)
            if fast_orders:
                record["fast_orders"] = fast_orders
            return record
        return {"received_at": self._now(), "type": "raw", "raw": message}

    def _parse_socketio_event(self, message: str) -> tuple[str, Any]:
        payload = json.loads(message[2:])
        if isinstance(payload, list) and payload:
            return str(payload[0]), payload[1] if len(payload) == 2 else payload[1:]
        return "message", payload

    def _fast_orders(self, message: str) -> list[dict[str, str]]:
        if not message.startswith('42["list:update"') or '"op":"add"' not in message:
            return []
        orders: list[dict[str, str]] = []
        start = 0
        while True:
            marker = message.find('"op":"add"', start)
            if marker < 0:
                return orders
            data_marker = message.find('"data":{', marker)
            if data_marker < 0:
                start = marker + 8
                continue
            end = self._object_end(message, data_marker + 7)
            if end < 0:
                start = data_marker + 7
                continue
            chunk = message[data_marker:end]
            order_id = self._json_string_field(chunk, "id")
            amount = self._json_string_field(chunk, "in_amount")
            if order_id and amount:
                orders.append({"id": order_id, "in_amount": amount})
            start = end

    def _object_end(self, text: str, start: int) -> int:
        depth = 0
        in_string = False
        escaped = False
        for index in range(start, len(text)):
            char = text[index]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue
            if char == '"':
                in_string = True
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return index + 1
        return -1

    def _json_string_field(self, text: str, name: str) -> str:
        marker = f'"{name}":"'
        start = text.find(marker)
        if start < 0:
            return ""
        start += len(marker)
        value = []
        escaped = False
        for char in text[start:]:
            if escaped:
                value.append(char)
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                return "".join(value)
            else:
                value.append(char)
        return ""

    def _parse_upgrade_headers(self, response: bytes) -> dict[str, str]:
        headers: dict[str, str] = {}
        for line in response.decode("iso-8859-1", "replace").split("\r\n")[1:]:
            if not line or ":" not in line:
                continue
            name, value = line.split(":", 1)
            headers[name.strip().lower()] = value.strip()
        cf_ray = headers.get("cf-ray", "")
        if cf_ray and "-" in cf_ray:
            headers["cf-colo"] = cf_ray.rsplit("-", 1)[1]
        return headers

    def _append_json(self, record: dict[str, Any]) -> None:
        if not self.save_json_path:
            return
        path = Path(self.save_json_path)
        existing: list[dict[str, Any]] = []
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                existing = []
            if not isinstance(existing, list):
                existing = []
        path.parent.mkdir(parents=True, exist_ok=True)
        existing.append(record)
        path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")

    def _read_frame(self, conn: ssl.SSLSocket) -> tuple[int, bytes]:
        first = self._recv_exact(conn, 2)
        if first is None:
            return 8, b""
        b1, b2 = first
        opcode = b1 & 0x0F
        masked = b2 & 0x80
        length = b2 & 0x7F
        if length == 126:
            extended = self._recv_exact(conn, 2)
            if extended is None:
                return 8, b""
            length = struct.unpack("!H", extended)[0]
        elif length == 127:
            extended = self._recv_exact(conn, 8)
            if extended is None:
                return 8, b""
            length = struct.unpack("!Q", extended)[0]
        mask = self._recv_exact(conn, 4) if masked else b""
        payload = self._recv_exact(conn, length)
        if payload is None:
            return 8, b""
        if masked and mask:
            payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
        return opcode, payload

    def _read_http_headers(self, conn: ssl.SSLSocket) -> bytes:
        response = bytearray()
        while b"\r\n\r\n" not in response:
            chunk = conn.recv(4096)
            if not chunk:
                break
            response.extend(chunk)
            if len(response) > 65536:
                raise RuntimeError("websocket upgrade response too large")
        return bytes(response)

    def _recv_exact(self, conn: ssl.SSLSocket, size: int) -> bytes | None:
        data = bytearray()
        while len(data) < size:
            chunk = conn.recv(size - len(data))
            if not chunk:
                return None
            data.extend(chunk)
        return bytes(data)

    def _ws_frame(self, payload: str) -> bytes:
        return self._frame(payload.encode("utf-8"), 1)

    def _frame(self, payload: bytes, opcode: int) -> bytes:
        length = len(payload)
        head = bytearray([0x80 | opcode, 0x80 | (126 if length > 125 and length < 65536 else 127 if length >= 65536 else length)])
        if length > 125 and length < 65536:
            head.extend(struct.pack("!H", length))
        elif length >= 65536:
            head.extend(struct.pack("!Q", length))
        mask = os.urandom(4)
        head.extend(mask)
        return bytes(head) + bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()
