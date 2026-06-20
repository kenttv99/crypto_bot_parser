from __future__ import annotations

import base64
import json
import os
import socket
import ssl
import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


@dataclass(slots=True)
class CryptoBotSocketClient:
    cookie: str
    url: str = "wss://app.send.tg/internal/v1/p2c-socket/?EIO=4&transport=websocket"
    origin: str = "https://app.send.tg"
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
    accept_language: str = "ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7"
    accept_encoding: str = "gzip, deflate, br, zstd"
    timeout: int = 30
    save_json_path: str | None = None
    extra_headers: dict[str, str] = field(default_factory=dict)
    initialized: bool = False

    def run(self) -> None:
        conn = self._connect()
        try:
            self._negotiate(conn)
            while True:
                message = self._read_message(conn)
                if message is None:
                    return
                self._handle_message(conn, message)
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _connect(self) -> ssl.SSLSocket:
        parsed = urlparse(self.url)
        if parsed.scheme != "wss":
            raise ValueError("url must use wss://")
        raw = socket.create_connection((parsed.hostname, parsed.port or 443), timeout=self.timeout)
        return ssl.create_default_context().wrap_socket(raw, server_hostname=parsed.hostname)

    def _negotiate(self, conn: ssl.SSLSocket) -> None:
        parsed = urlparse(self.url)
        key = base64.b64encode(os.urandom(16)).decode()
        headers = [
            f"GET {parsed.path}?{parsed.query} HTTP/1.1",
            f"Host: {parsed.hostname}",
            "Connection: Upgrade",
            "Pragma: no-cache",
            "Cache-Control: no-cache",
            f"User-Agent: {self.user_agent}",
            "Upgrade: websocket",
            f"Origin: {self.origin}",
            "Sec-WebSocket-Version: 13",
            f"Accept-Encoding: {self.accept_encoding}",
            f"Accept-Language: {self.accept_language}",
            f"Cookie: {self.cookie}",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits",
        ]
        for key, value in self.extra_headers.items():
            headers.append(f"{key}: {value}")
        conn.sendall(("\r\n".join(headers) + "\r\n\r\n").encode())
        response = conn.recv(4096)
        if b" 101 " not in response:
            raise RuntimeError(response.decode("utf-8", "replace"))
        conn.sendall(self._ws_frame("40"))

    def _read_message(self, conn: ssl.SSLSocket) -> str | None:
        opcode, payload = self._read_frame(conn)
        if opcode == 8:
            return None
        if opcode == 9:
            conn.sendall(self._frame(payload, 10))
            return ""
        if opcode != 1:
            return ""
        return payload.decode("utf-8", "replace")

    def _handle_message(self, conn: ssl.SSLSocket, message: str) -> None:
        if not message:
            return
        print(message)
        parsed = self._parse_message(message)
        if parsed["type"] == "socketio_connect" and not self.initialized:
            conn.sendall(self._ws_frame('42["list:initialize"]'))
            self.initialized = True
        if parsed["type"] == "ping":
            conn.sendall(self._ws_frame("3"))
        self._append_json(parsed)
        if parsed["type"] == "socketio_event" and parsed.get("event") == "list:update":
            print(json.dumps(parsed["payload"], ensure_ascii=False))

    def _parse_message(self, message: str) -> dict[str, Any]:
        if message.startswith("0{"):
            return {"received_at": self._now(), "type": "engine_open", **json.loads(message[1:]), "raw": message}
        if message.startswith("40{"):
            return {"received_at": self._now(), "type": "socketio_connect", "payload": json.loads(message[2:]), "raw": message}
        if message == "2":
            return {"received_at": self._now(), "type": "ping"}
        if message == "3":
            return {"received_at": self._now(), "type": "pong"}
        if message.startswith("44{"):
            return {"received_at": self._now(), "type": "socketio_error", "payload": json.loads(message[2:]), "raw": message}
        if message.startswith("42"):
            event, payload = self._parse_socketio_event(message)
            return {"received_at": self._now(), "type": "socketio_event", "event": event, "payload": payload, "raw": message}
        return {"received_at": self._now(), "type": "raw", "raw": message}

    def _parse_socketio_event(self, message: str) -> tuple[str, Any]:
        payload = json.loads(message[2:])
        if isinstance(payload, list) and payload:
            event = str(payload[0])
            data = payload[1] if len(payload) == 2 else payload[1:]
            return event, data
        return "message", payload

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
        first = conn.recv(2)
        if len(first) < 2:
            return 8, b""
        b1, b2 = first
        opcode = b1 & 0x0F
        masked = b2 & 0x80
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack("!H", conn.recv(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", conn.recv(8))[0]
        mask = conn.recv(4) if masked else b""
        payload = b""
        while len(payload) < length:
            chunk = conn.recv(length - len(payload))
            if not chunk:
                break
            payload += chunk
        if masked and mask:
            payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
        return opcode, payload

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
