# `tests/request_to_cryptobot_api.py`

CLI для:
- `GET /internal/v1/p2c/onboarding/state`
- `POST /internal/v1/p2c/payments/take/{order_id}`
- websocket `wss://app.send.tg/internal/v1/p2c-socket/?EIO=4&transport=websocket`

## Запуск

```bash
./venv/Scripts/python.exe tests/request_to_cryptobot_api.py --state
./venv/Scripts/python.exe tests/request_to_cryptobot_api.py --take --order-id 6a34f726006dee5c896884f8
./venv/Scripts/python.exe tests/request_to_cryptobot_api.py --socket --save-json data/events.json
```

## Конфиг

- все cookie задаются одной строкой в `COOKIE_HEADER`
- значение читается из `.env.parametrs`
- пример без секретов лежит в `env.example`

## Что осталось в коде

- в `CryptoBotAPI` зашиты постоянные заголовки для API/WebView-профиля
- `CryptoBotAPI` использует HTTP/2 transport через `httpx`
- для каждого `take` генерируются свежие `baggage`/`sentry-trace`
- в `CryptoBotSocketClient` оставлены только обязательные для handshake поля и `Origin`/`User-Agent`
- для `take` добавлены browser headers из DevTools: `accept-language`, `priority`, `sec-ch-ua*`, `sec-fetch-*`
- `accept-encoding` не задается вручную, распаковкой ответа занимается HTTP-клиент

## Поведение

- `--state` вызывает `CryptoBotAPI.get_onboarding_state()`
- `--take` вызывает `CryptoBotAPI.take_payment(order_id)`
- `--socket` открывает websocket, печатает сообщения и может сохранять их в JSON через `--save-json`
