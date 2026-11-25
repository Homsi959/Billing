import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from billing.domain import MAX_ATTEMPTS, PaymentJob, PaymentProcessed, PaymentStatus, WebhookStatus
from billing.models import OutboxMessage, Payment
from billing.payments import DEAD_ROUTE, NEW_ROUTE
from billing.webhooks import WebhookDeliveryError

GatewayOutcome = Literal[PaymentStatus.SUCCEEDED, PaymentStatus.FAILED]
Gateway = Callable[[UUID], Awaitable[GatewayOutcome]]
DeliverWebhook = Callable[[str, PaymentProcessed], Awaitable[None]]


class InvalidJob(Exception):
    pass


async def simulate_gateway(payment_id: UUID) -> GatewayOutcome:
    await asyncio.sleep(2 + (payment_id.time_low / 0xFFFFFFFF) * 3)
    return PaymentStatus.FAILED if payment_id.int % 10 == 0 else PaymentStatus.SUCCEEDED


class PaymentProcessor:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        gateway: Gateway,
        deliver: DeliverWebhook,
        retry_base: float,
    ) -> None:
        self.sessions = sessions
        self.gateway = gateway
        self.deliver = deliver
        self.retry_base = retry_base

    async def process(self, job: PaymentJob) -> str | None:
        payment = await self._current_payment(job)
        if payment is None:
            return None
        if payment.status == PaymentStatus.PENDING:
            outcome = await self.gateway(payment.id)
            async with self.sessions.begin() as session:
                await session.execute(
                    update(Payment)
                    .where(Payment.id == payment.id, Payment.status == PaymentStatus.PENDING)
                    .values(status=outcome, processed_at=func.now())
                )
            payment = await self._current_payment(job)
            if payment is None:
                return None

        event = PaymentProcessed(
            event_id=uuid5(NAMESPACE_URL, f"billing:payment.processed:{payment.id}"),
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            metadata=payment.payment_metadata,
            processed_at=payment.processed_at,
        )
        failure_code = None
        try:
            await self.deliver(payment.webhook_url, event)
        except WebhookDeliveryError as error:
            failure_code = error.code
        await self._finish_attempt(job, failure_code)
        return failure_code

    async def _current_payment(self, job: PaymentJob) -> Payment | None:
        async with self.sessions() as session:
            payment = await session.get(Payment, job.payment_id)
            if payment is None or job.attempt > payment.attempt:
                raise InvalidJob
            if job.attempt < payment.attempt or payment.webhook_status != WebhookStatus.PENDING:
                return None
            return payment

    async def _finish_attempt(self, job: PaymentJob, failure_code: str | None) -> None:
        async with self.sessions.begin() as session:
            payment = await session.get(Payment, job.payment_id, with_for_update=True)
            if (
                payment is None
                or payment.attempt != job.attempt
                or payment.webhook_status != WebhookStatus.PENDING
            ):
                return
            if failure_code is None:
                payment.webhook_status = WebhookStatus.DELIVERED
            elif job.attempt < MAX_ATTEMPTS:
                payment.attempt += 1
                delay = timedelta(seconds=self.retry_base * 2 ** (job.attempt - 1))
                session.add(
                    OutboxMessage(
                        payment_id=payment.id,
                        route=NEW_ROUTE,
                        attempt=payment.attempt,
                        available_at=func.now() + delay,
                    )
                )
            else:
                payment.webhook_status = WebhookStatus.EXHAUSTED
                session.add(
                    OutboxMessage(
                        payment_id=payment.id,
                        route=DEAD_ROUTE,
                        attempt=job.attempt,
                        failure_code=failure_code,
                    )
                )
