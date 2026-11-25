import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from aiohttp import web
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from billing.api import create_app
from billing.broker import (
    dead_queue,
    declare_topology,
    make_broker,
    payments_exchange,
    payments_queue,
)
from billing.config import ApiSettings, WorkerSettings
from billing.db import make_engine
from billing.domain import PaymentJob, PaymentStatus
from billing.models import OutboxMessage, Payment
from billing.outbox import OutboxRelay
from billing.processing import PaymentProcessor
from billing.webhooks import WebhookDeliveryError
from billing.worker import open_worker

pytestmark = pytest.mark.integration
API_KEY = "integration-api-key-just-for-tests"
PAYMENTS = "/api/v1/payments"


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def settings():
    database = os.getenv("BILLING_TEST_DATABASE_URL")
    rabbitmq = os.getenv("BILLING_TEST_RABBITMQ_URL")
    if not database or not rabbitmq:
        pytest.skip("Set BILLING_TEST_DATABASE_URL and BILLING_TEST_RABBITMQ_URL")
    command = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "alembic",
        "upgrade",
        "head",
        env=os.environ | {"BILLING_DATABASE_URL": database},
    )
    assert await command.wait() == 0
    return WorkerSettings(
        database_url=database,
        rabbitmq_url=rabbitmq,
        webhook_private_hosts={"127.0.0.1"},
        webhook_timeout=2.0,
        retry_base_seconds=0.15,
        outbox_poll_seconds=0.02,
    )


@pytest.fixture
async def sessions(settings):
    engine = make_engine(settings)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


@pytest.fixture
async def clean_broker(settings):
    broker = make_broker(settings.rabbitmq_url.get_secret_value())
    async with broker:
        await declare_topology(broker)
        for definition in (payments_queue, dead_queue):
            queue = await broker.declare_queue(definition)
            await queue.purge()


@pytest.fixture
async def client(settings, sessions):
    app = create_app(ApiSettings(database_url=settings.database_url, api_key=API_KEY))
    created_ids = set()

    async def record_payment(response):
        if response.status_code == 202:
            await response.aread()
            created_ids.add(UUID(response.json()["payment_id"]))

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://billing",
            headers={"X-API-Key": API_KEY},
            event_hooks={"response": [record_payment]},
        ) as client:
            try:
                yield client
            finally:
                if created_ids:
                    async with sessions.begin() as session:
                        await session.execute(
                            delete(OutboxMessage).where(OutboxMessage.payment_id.in_(created_ids))
                        )
                        await session.execute(delete(Payment).where(Payment.id.in_(created_ids)))


def new_payment(url="https://example.com/webhook"):
    return {
        "amount": "1250.00",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": "42"},
        "webhook_url": url,
    }


async def wait_for_webhook_status(client, payment_id, expected, *, max_wait=10):
    async with asyncio.timeout(max_wait):
        while True:
            response = await client.get(f"{PAYMENTS}/{payment_id}")
            assert response.status_code == 200
            details = response.json()
            if details["webhook_status"] == expected:
                return details
            await asyncio.sleep(0.02)


async def wait_for_dead_letter(broker):
    queue = await broker.declare_queue(dead_queue)
    async with queue.iterator() as messages, asyncio.timeout(5):
        return await anext(messages)


@pytest.fixture
def test_compose():
    project = os.getenv("BILLING_TEST_COMPOSE_PROJECT")
    if not project:
        pytest.skip("Run bash scripts/check.sh to test a RabbitMQ shutdown")
    assert project.startswith("billing-test-")
    base = ("docker", "compose", "-p", project, "-f", "compose.yaml", "-f", "compose.test.yaml")

    async def compose(*args):
        command = await asyncio.create_subprocess_exec(*base, *args)
        assert await command.wait() == 0

    return compose


@asynccontextmanager
async def webhook_receiver(responses):
    received = []

    async def handler(request):
        received.append((request.headers["Idempotency-Key"], await request.json()))
        return web.Response(status=responses[min(len(received), len(responses)) - 1])

    app = web.Application()
    app.router.add_post("/webhook", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        yield f"http://127.0.0.1:{port}/webhook", received
    finally:
        await runner.cleanup()


async def test_concurrent_idempotency_and_durable_enqueue(client, sessions):
    key = {"Idempotency-Key": str(uuid4())}
    responses = await asyncio.gather(
        *(client.post(PAYMENTS, json=new_payment(), headers=key) for _attempt in range(12))
    )
    created = responses[0].json()
    assert all(response.status_code == 202 and response.json() == created for response in responses)
    assert responses[0].headers["location"] == f"{PAYMENTS}/{created['payment_id']}"
    assert created["status"] == "pending"
    conflict = await client.post(PAYMENTS, json=new_payment() | {"amount": "10.00"}, headers=key)
    assert conflict.status_code == 409
    assert (await client.get(f"{PAYMENTS}/{uuid4()}")).status_code == 404
    payment_id = UUID(created["payment_id"])
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.amount == Decimal("1250.00")
        outgoing = (
            await session.scalars(
                select(OutboxMessage).where(OutboxMessage.payment_id == payment_id)
            )
        ).one()
        assert outgoing.published_at is None
        assert outgoing.attempt == 1


@pytest.mark.usefixtures("clean_broker")
async def test_outbox_requires_a_routable_confirm(settings, client, sessions):
    response = await client.post(
        PAYMENTS, json=new_payment(), headers={"Idempotency-Key": str(uuid4())}
    )
    assert response.status_code == 202
    payment_id = UUID(response.json()["payment_id"])
    broker = make_broker(settings.rabbitmq_url.get_secret_value())
    async with broker:
        await declare_topology(broker)
        queue = await broker.declare_queue(dead_queue)
        payment_queue = await broker.declare_queue(payments_queue)
        exchange = await broker.declare_exchange(payments_exchange)
        await payment_queue.unbind(exchange, routing_key="payments.new")
        relay = OutboxRelay(sessions, broker, poll_seconds=0.02)
        try:
            assert await relay.publish_one() is True
            async with sessions.begin() as session:
                outgoing = (
                    await session.scalars(
                        select(OutboxMessage).where(OutboxMessage.payment_id == payment_id)
                    )
                ).one()
                assert outgoing.published_at is None
                assert outgoing.available_at > await session.scalar(select(func.now()))
                outgoing.available_at = func.now()
        finally:
            await payment_queue.bind(exchange, routing_key="payments.new")
        assert await relay.publish_one() is True
        async with payment_queue.iterator() as messages:
            async with asyncio.timeout(5):
                message = await anext(messages)
            assert json.loads(message.body) == {"payment_id": str(payment_id), "attempt": 1}
            await message.ack()
        async with sessions() as session:
            published_at = await session.scalar(
                select(OutboxMessage.published_at).where(OutboxMessage.payment_id == payment_id)
            )
            assert published_at is not None
        assert await queue.get(fail=False) is None


@pytest.mark.usefixtures("clean_broker")
async def test_accepts_payment_while_rabbitmq_is_down_and_delivers_after_restart(
    settings, client, sessions, test_compose
):
    async with webhook_receiver([204]) as (url, received), open_worker(settings):
        await test_compose("exec", "-T", "rabbitmq", "rabbitmqctl", "stop_app")
        try:
            key = {"Idempotency-Key": str(uuid4())}
            payload = new_payment(url)
            created = await client.post(PAYMENTS, json=payload, headers=key)
            replayed = await client.post(PAYMENTS, json=payload, headers=key)
            assert created.status_code == replayed.status_code == 202
            assert created.json() == replayed.json()
            payment_id = UUID(created.json()["payment_id"])
            async with sessions() as session:
                outgoing = (
                    await session.scalars(
                        select(OutboxMessage).where(OutboxMessage.payment_id == payment_id)
                    )
                ).one()
                assert outgoing.attempt == 1 and outgoing.published_at is None
            assert received == []
        finally:
            await test_compose("exec", "-T", "rabbitmq", "rabbitmqctl", "start_app")

        details = await wait_for_webhook_status(client, payment_id, "delivered", max_wait=30)
        assert len(received) == 1
        event_key, event = received[0]
        assert event["payment_id"] == str(payment_id)
        assert event["status"] == details["status"]
        assert event["event_id"] == event_key
        async with sessions() as session:
            published_at = await session.scalar(
                select(OutboxMessage.published_at).where(OutboxMessage.payment_id == payment_id)
            )
            assert published_at is not None


async def test_processing_checkpoints_and_ignores_stale_jobs(settings, client, sessions):
    response = await client.post(
        PAYMENTS, json=new_payment(), headers={"Idempotency-Key": str(uuid4())}
    )
    assert response.status_code == 202
    payment_id = UUID(response.json()["payment_id"])
    gateway_calls = []
    events = []

    async def gateway(called_id):
        gateway_calls.append(called_id)
        return PaymentStatus.SUCCEEDED

    async def fail_delivery(_url, event):
        async with sessions() as session:
            payment = await session.get(Payment, payment_id)
            assert payment.status == PaymentStatus.SUCCEEDED
            assert payment.processed_at == event.processed_at
        events.append(event)
        raise WebhookDeliveryError("timeout")

    processor = PaymentProcessor(sessions, gateway, fail_delivery, settings.retry_base_seconds)
    for attempt in (1, 1, 2, 2, 3, 3):
        await processor.process(PaymentJob(payment_id=payment_id, attempt=attempt))

    assert gateway_calls == [payment_id]
    assert len(events) == 3 and events[0] == events[1] == events[2]
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.webhook_status == "exhausted"
        outgoing = (
            await session.scalars(
                select(OutboxMessage).where(OutboxMessage.payment_id == payment_id)
            )
        ).all()
        assert {(row.attempt, row.route, row.failure_code) for row in outgoing} == {
            (1, "payments.new", None),
            (2, "payments.new", None),
            (3, "payments.new", None),
            (3, "payments.dead", "timeout"),
        }


@pytest.mark.usefixtures("clean_broker")
@pytest.mark.parametrize(
    ("responses", "expected", "outcome"),
    [
        ([503, 204], "delivered", PaymentStatus.FAILED),
        ([503], "exhausted", PaymentStatus.SUCCEEDED),
    ],
)
async def test_consumer_retries_without_reprocessing_payment(
    settings, client, sessions, responses, expected, outcome
):
    gateway_calls = []

    async def gateway(payment_id):
        gateway_calls.append(payment_id)
        return outcome

    async with webhook_receiver(responses) as (url, received):
        key = {"Idempotency-Key": str(uuid4())}
        reply = await client.post(PAYMENTS, json=new_payment(url), headers=key)
        assert reply.status_code == 202
        payment_id = reply.json()["payment_id"]
        async with open_worker(settings, gateway) as broker:
            details = await wait_for_webhook_status(client, payment_id, expected)
            assert details["status"] == outcome
            assert details["processed_at"] is not None
            assert details["amount"] == "1250.00"
            assert gateway_calls == [UUID(payment_id)]
            assert len(received) == (2 if expected == "delivered" else 3)
            event_key, event = received[0]
            assert event["event_id"] == event_key
            assert event["status"] == outcome
            assert all(item == received[0] for item in received)
            replay = await client.post(PAYMENTS, json=new_payment(url), headers=key)
            assert replay.json() == reply.json()
            async with sessions() as session:
                scheduled = (
                    await session.scalars(
                        select(OutboxMessage)
                        .where(
                            OutboxMessage.payment_id == UUID(payment_id),
                            OutboxMessage.route == "payments.new",
                        )
                        .order_by(OutboxMessage.attempt)
                    )
                ).all()
                assert [row.attempt for row in scheduled] == list(range(1, len(received) + 1))
                if expected == "exhausted":
                    delays = [
                        (row.available_at - previous.published_at).total_seconds()
                        for previous, row in zip(scheduled[:-1], scheduled[1:], strict=True)
                    ]
                    assert delays[0] >= settings.retry_base_seconds
                    assert delays[1] >= settings.retry_base_seconds * 2

            if expected == "exhausted":
                message = await wait_for_dead_letter(broker)
                assert json.loads(message.body) == {
                    "payment_id": payment_id,
                    "attempt": 3,
                    "failure_code": "http_503",
                }
                await message.ack()


@pytest.mark.usefixtures("clean_broker")
async def test_worker_error_reaches_broker_dlq_after_three_deliveries(settings, client, caplog):
    gateway_calls = []

    async def broken_gateway(payment_id):
        gateway_calls.append(str(payment_id))
        raise RuntimeError("gateway test failure")

    reply = await client.post(
        PAYMENTS, json=new_payment(), headers={"Idempotency-Key": str(uuid4())}
    )
    assert reply.status_code == 202
    payment_id = reply.json()["payment_id"]
    async with open_worker(settings, broken_gateway) as broker:
        message = await wait_for_dead_letter(broker)
        assert gateway_calls == [payment_id] * 3
        assert json.loads(message.body) == {"payment_id": payment_id, "attempt": 1}
        assert message.correlation_id == payment_id
        assert message.headers["x-first-death-reason"] == "delivery_limit"
        assert f"payment_id={payment_id} message_id={message.message_id} attempt=1" in caplog.text
        await message.ack()

    details = await client.get(f"{PAYMENTS}/{payment_id}")
    assert details.status_code == 200
    assert details.json()["status"] == details.json()["webhook_status"] == "pending"


@pytest.mark.usefixtures("clean_broker")
async def test_consumer_dead_letters_invalid_payload(settings):
    async with open_worker(settings) as broker:
        await broker.publish(
            b"{broken",
            content_type="application/json",
            exchange=payments_exchange,
            routing_key="payments.new",
        )
        message = await wait_for_dead_letter(broker)
        assert message.body == b"{broken"
        assert message.headers["x-first-death-reason"] == "rejected"
        await message.ack()
