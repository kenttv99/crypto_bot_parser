# `scripts/take_order.py`

Скрипт слушает `p2c-socket`, находит ордер по диапазону суммы в `RUB`, берет его через `POST /internal/v1/p2c/payments/take/{order_id}` и сохраняет результат в JSON.

## Конфиг

Значения читаются из `.env.parametrs`:

- `COOKIE_HEADER` - полный raw `Cookie` header
- `MIN_LIMIT_RUB` - нижняя граница, пусто = без нижней границы
- `MAX_LIMIT_RUB` - верхняя граница, пусто = без верхней границы
- если задан только один лимит, второй край диапазона считается открытым

## Поведение

- если `MIN_LIMIT_RUB > MAX_LIMIT_RUB`, скрипт завершается с ошибкой
- ордеры берутся из событий `list:snapshot` и `list:update`
- первый подходящий ордер останавливает слушание сокета
- ответ endpoint `take_payment` печатается в консоль
- взятый ордер сохраняется в `data/taken_orders.json`

## Запуск

```bash
./venv/Scripts/python.exe scripts/take_order.py
```
