# `scripts/take_order.py` и `scripts/take_order_parallel.py`

Скрипты слушают `p2c-socket`, находят ордера по диапазону суммы в `RUB`, берут их через `POST /internal/v1/p2c/payments/take/{order_id}` и сохраняют результат.

## Конфиг

Значения читаются из `.env.parametrs`:

- `COOKIE_HEADER` - полный raw `Cookie` header
- `MIN_LIMIT_RUB` - нижняя граница, пусто = без нижней границы
- `MAX_LIMIT_RUB` - верхняя граница, пусто = без верхней границы
- `WAIT_TAKE_RESPONSE` - `true` ждет HTTP-ответ `take`, `false` возвращается сразу после отправки POST
- `TAKE_LIMIT_PER_SECOND` - максимум `take` POST за 1 секунду
- `TAKE_LIMIT_5M_PRIMARY` - первый максимум `take` POST за 5 минут
- `TAKE_LIMIT_5M_SECONDARY` - второй максимум `take` POST за 5 минут
- если задан только один лимит, второй край диапазона считается открытым

## Поведение

- если `MIN_LIMIT_RUB > MAX_LIMIT_RUB`, скрипт завершается с ошибкой
- ордеры берутся из событий `list:snapshot` и `list:update`
- обработанные `order_id` запоминаются в памяти процесса, чтобы не отправлять повторный `take` на один и тот же ордер
- при `WAIT_TAKE_RESPONSE=true` ответ endpoint `take_payment` печатается в консоль
- при `WAIT_TAKE_RESPONSE=false` в лог пишется факт отправки запроса без подтверждения HTTP-ответом
- `scripts/take_order.py` использует одно websocket-соединение и сохраняет записи в `data/taken_orders.json`
- `scripts/take_order_parallel.py` использует несколько websocket-соединений и сохраняет записи в `data/taken_orders_parallel.jsonl`
- в `take`-запросе генерируются свежие `baggage`/`sentry-trace`, добавлены browser headers из WebView-профиля
- `--take-attempts N` отправляет до `N` параллельных `take`-запросов на один подходящий ордер через HTTP/2 transport

## Parallel mode

`scripts/take_order_parallel.py` открывает несколько websocket-соединений с одним cookie. Первый worker, который увидел новый подходящий `order_id`, отправляет `take`; остальные workers игнорируют этот `order_id` через общий `seen_ids` lock.

Для `take` используется общий HTTP/2 client. Burst-запросы идут параллельно через multiplexing, без отдельного HTTP/1.1 соединения на каждую попытку.

Если включен `--take-attempts`, один worker делает burst из нескольких параллельных POST на один и тот же `order_id`. Значение не может быть больше `--take-pool-size`.

Перед отправкой `take` скрипт резервирует квоту по всем лимитам из `.env.parametrs`. Если доступной квоты меньше, чем `--take-attempts`, burst уменьшается до доступного количества. Если квоты нет, ордер пропускается с `reason=take_rate_limit`.

В режиме `WAIT_TAKE_RESPONSE=false` клиент закрывает response stream сразу после получения HTTP headers.

Skip-логи по умолчанию выключены, чтобы не тратить время на `json.dumps` payload и `stdout flush` в горячем пути. Для отладки их можно включить флагом `--log-skips`.

Флаги:

- `--connections`, `-c` - количество параллельных websocket-соединений, по умолчанию `3`
- `--take-pool-size` - максимум HTTP-соединений transport для `take`, по умолчанию `16`
- `--take-attempts` - количество параллельных `take` POST на один подходящий ордер, по умолчанию `1`
- `--socket-timeout` - timeout чтения websocket в секундах, по умолчанию `120`
- `--reconnect-delay` - пауза перед переподключением websocket, по умолчанию `0.2`
- `--start-delay` - пауза между стартом workers, по умолчанию `0.05`
- `--log-skips` - печатать пропущенные ордера

При timeout, закрытии websocket или сетевой ошибке worker не завершает процесс, а переподключается.

## Запуск

```bash
./venv/Scripts/python.exe scripts/take_order.py
```

```bash
python3 scripts/take_order_parallel.py -c 5 --take-pool-size 16 --take-attempts 3 --socket-timeout 120 --reconnect-delay 0.2
```
