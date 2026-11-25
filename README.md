# Billing
- FastAPI принимает платежи, PostgreSQL хранит их, а воркер получает задачи из RabbitMQ, запускает эмулятор шлюза и шлёт вебхуки.
- Запустите демо командой `export BILLING_API_KEY="$(openssl rand -hex 32)" BILLING_WEBHOOK_PRIVATE_HOSTS='["webhook-demo"]' && docker compose --profile demo up -d --build --wait`.
- Создайте платёж командой `curl -fsS "http://127.0.0.1:${BILLING_HTTP_PORT:-8000}/api/v1/payments" -H "X-API-Key: $BILLING_API_KEY" -H "Idempotency-Key: demo-$(openssl rand -hex 8)" -H 'Content-Type: application/json' -d '{"amount":"1250.00","currency":"RUB","description":"Заказ №42","webhook_url":"http://webhook-demo:8080/webhook"}'`.
- Статус смотрите через `GET /api/v1/payments/{payment_id}` с `X-API-Key`, вебхуки через `docker compose --profile demo logs webhook-demo`. `uv run poe check` запускает Ruff и тесты с Docker. Для линтинга можно отдельно запустить `uv run poe lint`.
