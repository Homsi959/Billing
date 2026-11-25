#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")/.."

project="billing-demo-$$-$RANDOM"
BILLING_API_KEY=$(openssl rand -hex 32)
export BILLING_API_KEY BILLING_HTTP_PORT=0 BILLING_WEBHOOK_PRIVATE_HOSTS='["webhook-demo"]'

compose() {
    docker compose -p "$project" --profile demo "$@"
}
trap 'compose down --volumes' EXIT

compose up -d --build --wait
payments_url="http://$(compose port api 8000)/api/v1/payments"
accepted=$(curl -fsS --max-time 5 "$payments_url" \
    -H "X-API-Key: $BILLING_API_KEY" \
    -H "Idempotency-Key: demo-$(openssl rand -hex 8)" \
    -H 'Content-Type: application/json' \
    -d '{"amount":"1250.00","currency":"RUB","description":"Заказ №42","webhook_url":"http://webhook-demo:8080/webhook"}')
payment_id=$(python -c 'import json, sys; print(json.load(sys.stdin)["payment_id"])' <<< "$accepted")
printf 'Payment accepted\n%s\n' "$accepted"

for attempt in {1..30}; do
    status=$(curl -fsS --max-time 5 "$payments_url/$payment_id" -H "X-API-Key: $BILLING_API_KEY")
    webhook_status=$(python -c 'import json, sys; print(json.load(sys.stdin)["webhook_status"])' <<< "$status")
    case "$webhook_status" in
        delivered)
            printf 'Payment processed\n%s\n' "$status"
            compose logs --no-color webhook-demo
            exit 0
            ;;
        exhausted)
            printf 'Webhook delivery failed.\n%s\n' "$status" >&2
            exit 1
            ;;
    esac
    sleep 1
done

printf '%s\n' 'Timed out waiting for the demo webhook.' >&2
exit 1
