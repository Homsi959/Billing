from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from billing.api import create_app
from billing.config import ApiSettings

KEY = "local-api-key-for-unit-tests"
SETTINGS = ApiSettings(database_url="postgresql+asyncpg://unused/unused", api_key=KEY)


@pytest.fixture
async def client():
    app = create_app(SETTINGS)
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://billing"
        ) as client:
            yield client


@pytest.mark.parametrize("target", [f"/api/v1/payments/{uuid4()}", "/openapi.json"])
@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
async def test_all_read_endpoints_require_api_key(client, target, headers):
    assert (await client.get(target, headers=headers)).status_code == 401


async def test_create_requires_key_and_idempotency_header(client):
    payload = {
        "amount": "42.10",
        "currency": "RUB",
        "description": "test",
        "webhook_url": "https://example.com/hooks",
    }
    assert (await client.post("/api/v1/payments", json=payload)).status_code == 401
    response = await client.post("/api/v1/payments", json=payload, headers={"X-API-Key": KEY})
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "idempotency-key"]


async def test_nonfinite_json_does_not_break_error_rendering(client):
    response = await client.post(
        "/api/v1/payments",
        content='{"amount": NaN, "currency":"USD", "description":"order",'
        '"webhook_url":"https://example.com"}',
        headers={"X-API-Key": KEY, "Idempotency-Key": "order", "Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "amount"]
