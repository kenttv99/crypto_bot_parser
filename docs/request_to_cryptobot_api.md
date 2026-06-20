# `tests/request_to_cryptobot_api.py`

CLI-тест для `GET /internal/v1/p2c/onboarding/state`, `POST /internal/v1/p2c/payments/take/{order_id}` и websocket `p2c-socket`.

## Запуск

```bash
./venv/Scripts/python.exe tests/request_to_cryptobot_api.py --state
./venv/Scripts/python.exe tests/request_to_cryptobot_api.py --take --order-id 6a34f726006dee5c896884f8
./venv/Scripts/python.exe tests/request_to_cryptobot_api.py --socket --save-json data/events.json
```

## Что делает

- создает `CryptoBotAPI`
- собирает `Cookie` header из `COOKIE_VALUES`
- вызывает `get_onboarding_state()`
- вызывает `take_payment(order_id)` при `--take`
- печатает JSON-ответ в stdout
- при `--socket` открывает websocket и печатает все сообщения в консоль
- при `--save-json` сохраняет websocket события в JSON-файл

## Входные данные

- значения cookie задаются в `COOKIE_VALUES`
- пустые значения не попадают в итоговый `Cookie` header
- `__cf_bm` можно оставить пустым, если он не нужен
- `--state`, `--take` и `--socket` взаимоисключающие
- `--take` требует `--order-id`
- рабочие значения загружаются из `.env.parametrs`
- шаблон без секретов лежит в `env.example`

## Заголовки

Клиент отправляет:

- `accept`
- `accept-language`
- `cookie`
- `priority`
- `referer`
- `sec-ch-ua`
- `sec-ch-ua-mobile`
- `sec-ch-ua-platform`
- `sec-fetch-dest`
- `sec-fetch-mode`
- `sec-fetch-site`
- `user-agent`

Дополнительно можно передать `baggage`, `sentry_trace` и `extra_headers` при создании `CryptoBotAPI`.
