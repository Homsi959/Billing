import socket
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import ValidationError

from billing.domain import PaymentCreate, PaymentJob
from billing.webhooks import PublicResolver, WebhookDeliveryError, is_public

PAYLOAD = {
    "amount": "42.10",
    "currency": "USD",
    "description": "Order 42",
    "metadata": {"shop": 7, "order": 42},
    "webhook_url": "https://example.com/hooks",
}


@pytest.mark.parametrize("amount", ["0", "-1", "1.001", "NaN", "Infinity", "10000000000000000"])
def test_reject_invalid_amounts(amount):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(PAYLOAD | {"amount": amount})


@pytest.mark.parametrize(
    "overrides",
    [
        {"description": "\x00"},
        {"metadata": {"x": "\ud800"}},
        {"metadata": {"x": float("nan")}},
        {"metadata": {"x": "a" * 16_384}},
        {"webhook_url": "https://user:pass@example.com/"},
        {"webhook_url": "https://example.com/#fragment"},
        {"webhook_url": "file:///etc/hosts"},
    ],
)
def test_reject_invalid_payment_fields(overrides):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(PAYLOAD | overrides)


def test_canonical_idempotency_fingerprint():
    first = PaymentCreate.model_validate(PAYLOAD)
    equivalent = PaymentCreate.model_validate(
        PAYLOAD | {"amount": 42.1, "metadata": {"order": 42, "shop": 7}}
    )
    changed = PaymentCreate.model_validate(PAYLOAD | {"amount": "42.11"})
    assert first.amount == Decimal("42.10")
    assert first.model_dump(mode="json")["amount"] == "42.10"
    assert first.fingerprint() == equivalent.fingerprint() != changed.fingerprint()


@pytest.mark.parametrize("attempt", [0, 4, "1", True])
def test_broker_attempt_must_be_an_integer_in_range(attempt):
    with pytest.raises(ValidationError):
        PaymentJob(payment_id=uuid4(), attempt=attempt)


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        ("8.8.8.8", True),
        ("2606:4700::1111", True),
        ("127.0.0.1", False),
        ("169.254.169.254", False),
        ("224.0.0.1", False),
        ("100.64.0.1", False),
        ("::ffff:127.0.0.1", False),
        ("64:ff9b::7f00:1", False),
    ],
)
def test_webhook_address_policy(address, expected):
    assert is_public(address) is expected


async def test_webhook_resolver_filters_actual_dns_answers():
    resolver = PublicResolver(frozenset({"dev-receiver"}))
    private = {"host": "127.0.0.1"}
    public = {"host": "8.8.8.8"}
    resolver.resolver = AsyncMock()
    resolver.resolver.resolve.side_effect = [[private, public], [private], [private]]
    assert await resolver.resolve("public.example", 443, socket.AF_INET) == [public]
    with pytest.raises(WebhookDeliveryError, match="destination_blocked"):
        await resolver.resolve("private.example", 443, socket.AF_INET)
    assert await resolver.resolve("dev-receiver", 80, socket.AF_INET) == [private]
