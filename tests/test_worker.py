from unittest.mock import AsyncMock
from uuid import uuid4

from faststream.rabbit import TestRabbitBroker

from billing.broker import make_broker, payments_exchange
from billing.domain import PaymentJob
from billing.worker import register_consumer


async def test_subscriber_validates_the_raw_amqp_body():
    broker = make_broker("amqp://guest:guest@localhost/")
    processor = AsyncMock()
    register_consumer(broker, processor)
    job = PaymentJob(payment_id=uuid4(), attempt=1)
    async with TestRabbitBroker(broker) as connected:
        await connected.publish(
            job.model_dump(mode="json"), exchange=payments_exchange, routing_key="payments.new"
        )
        await connected.publish(
            b"invalid json",
            content_type="application/json",
            exchange=payments_exchange,
            routing_key="payments.new",
        )
    processor.process.assert_awaited_once_with(job)
