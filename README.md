# Billing
- FastAPI принимает платежи, PostgreSQL хранит их, а воркер получает задачи из RabbitMQ, запускает эмулятор шлюза и шлёт вебхуки.
- `uv run poe demo` поднимает стенд в Docker, создаёт платёж, выводит статус и вебхук, затем убирает стенд.
- Для `POST /api/v1/payments` и `GET /api/v1/payments/{payment_id}` нужен `X-API-Key`. Для `POST` также нужен `Idempotency-Key`.
- `uv run poe check` запускает Ruff и тесты с Docker. `uv run poe lint` запускает линтер.
