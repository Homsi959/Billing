import asyncio
import logging
from datetime import timedelta

from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from billing.broker import dead_exchange, payments_exchange
from billing.models import OutboxMessage
from billing.payments import DEAD_ROUTE

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self, sessions: async_sessionmaker[AsyncSession], broker: RabbitBroker, poll_seconds: float
    ) -> None:
        self.sessions = sessions
        self.broker = broker
        self.poll_seconds = poll_seconds

    async def publish_one(self) -> bool:
        async with self.sessions.begin() as session:
            statement = (
                select(OutboxMessage)
                .where(
                    OutboxMessage.published_at.is_(None), OutboxMessage.available_at <= func.now()
                )
                .order_by(OutboxMessage.available_at, OutboxMessage.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            message = (await session.scalars(statement)).one_or_none()
            if message is None:
                return False

            payload = {"payment_id": str(message.payment_id), "attempt": message.attempt}
            exchange = payments_exchange
            if message.route == DEAD_ROUTE:
                exchange = dead_exchange
                payload["failure_code"] = message.failure_code

            try:
                async with asyncio.timeout(3):
                    confirmation = await self.broker.publish(
                        payload,
                        exchange=exchange,
                        routing_key=message.route,
                        persist=True,
                        mandatory=True,
                        message_id=str(message.id),
                        correlation_id=str(message.payment_id),
                    )
                    if not isinstance(confirmation, Basic.Ack):
                        raise RuntimeError("RabbitMQ did not confirm the message.")
            except Exception as error:
                message.available_at = func.now() + timedelta(seconds=5)
                logger.warning(
                    "Could not publish the outbox message. Will retry. "
                    "payment_id=%s message_id=%s attempt=%d "
                    "route=%s reason=%s",
                    message.payment_id,
                    message.id,
                    message.attempt,
                    message.route,
                    type(error).__name__,
                )
            else:
                message.published_at = func.now()
                log_confirmation = logger.info if message.route == DEAD_ROUTE else logger.debug
                log_confirmation(
                    "RabbitMQ confirmed the outbox message. "
                    "payment_id=%s message_id=%s attempt=%d route=%s",
                    message.payment_id,
                    message.id,
                    message.attempt,
                    message.route,
                )
            return True

    async def run(self) -> None:
        while True:
            try:
                if await self.publish_one():
                    continue
                delay = self.poll_seconds
            except Exception as error:
                logger.error(
                    "Could not run the outbox relay. Will retry. reason=%s", type(error).__name__
                )
                delay = 2.0
            await asyncio.sleep(delay)
