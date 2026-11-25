#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")/.."

uv sync --locked
uv run --locked ruff check .
uv run --locked ruff format --check .

export BILLING_TEST_COMPOSE_PROJECT="billing-test-$$-$RANDOM"
compose() {
    docker compose -p "$BILLING_TEST_COMPOSE_PROJECT" -f compose.yaml -f compose.test.yaml "$@"
}
cleanup() {
    compose down --volumes
}
trap cleanup EXIT

compose up -d --wait postgres rabbitmq
postgres_address=$(compose port postgres 5432)
rabbitmq_address=$(compose port rabbitmq 5672)
export BILLING_TEST_DATABASE_URL="postgresql+asyncpg://billing:local-db-password@$postgres_address/billing"
export BILLING_TEST_RABBITMQ_URL="amqp://billing:local-broker-password@$rabbitmq_address/"

uv run --locked pytest
