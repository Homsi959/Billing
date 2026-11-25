import asyncio
import logging
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker

from billing.broker import declare_topology, make_broker, payments_exchange, payments_queue
from billing.config import WorkerSettings
from billing.db import make_engine
from billing.domain import PaymentJob
from billing.outbox import OutboxRelay
from billing.processing import Gateway, InvalidJob, PaymentProcessor, simulate_gateway
from billing.webhooks import open_webhook_client

logger = logging.getLogger(__name__)


def register_consumer(broker: RabbitBroker, processor: PaymentProcessor) -> None:
    async def raw_body(message: RabbitMessage) -> bytes:
        return message.body

    @broker.subscriber(
        payments_queue, payments_exchange, ack_policy=AckPolicy.MANUAL, decoder=raw_body
    )
    async def consume(message: RabbitMessage) -> None:
        try:
            job = PaymentJob.model_validate_json(message.body)
        except ValidationError:
            logger.warning(
                "Rejected a message with an invalid payment job. "
                "payment_id=%s message_id=%s attempt=unknown",
                message.correlation_id,
                message.message_id,
            )
            await message.reject()
            return
        try:
            failure_code = await processor.process(job)
        except InvalidJob:
            logger.warning(
                "Rejected a job with an unknown payment or a future attempt. "
                "payment_id=%s message_id=%s attempt=%d",
                job.payment_id,
                message.message_id,
                job.attempt,
            )
            await message.reject()
        except Exception as error:
            logger.error(
                "Could not process the payment job. payment_id=%s message_id=%s attempt=%d "
                "redeliveries=%s reason=%s",
                job.payment_id,
                message.message_id,
                job.attempt,
                message.headers.get("x-delivery-count", 0),
                type(error).__name__,
            )
            await asyncio.sleep(1)
            await message.reject(requeue=True)
        else:
            if failure_code is not None:
                logger.warning(
                    "Could not deliver the webhook. "
                    "payment_id=%s message_id=%s attempt=%d reason=%s",
                    job.payment_id,
                    message.message_id,
                    job.attempt,
                    failure_code,
                )
            await message.ack()


@asynccontextmanager
async def open_worker(
    settings: WorkerSettings, gateway: Gateway = simulate_gateway
) -> AsyncIterator[RabbitBroker]:
    engine = make_engine(settings)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    broker = make_broker(settings.rabbitmq_url.get_secret_value())
    try:
        async with open_webhook_client(
            settings.webhook_timeout, settings.webhook_private_hosts
        ) as client:
            register_consumer(
                broker,
                PaymentProcessor(sessions, gateway, client.send, settings.retry_base_seconds),
            )
            async with broker:
                await declare_topology(broker)
                await broker.start()
                relay = asyncio.create_task(
                    OutboxRelay(sessions, broker, settings.outbox_poll_seconds).run()
                )
                try:
                    yield broker
                finally:
                    relay.cancel()
                    with suppress(asyncio.CancelledError):
                        await relay
    finally:
        await engine.dispose()


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    async with open_worker(WorkerSettings()):
        logger.info("Billing worker is ready to process payments.")
        await stop.wait()


if __name__ == "__main__":
    asyncio.run(main())
