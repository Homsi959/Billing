from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from billing.domain import PaymentAccepted, PaymentCreate, PaymentDetails
from billing.models import OutboxMessage, Payment

NEW_ROUTE = "payments.new"
DEAD_ROUTE = "payments.dead"


class IdempotencyConflict(Exception):
    pass


async def accept_payment(
    sessions: async_sessionmaker[AsyncSession], data: PaymentCreate, key: str
) -> PaymentAccepted:
    request_hash = data.fingerprint()
    async with sessions.begin() as session:
        statement = (
            insert(Payment)
            .values(
                idempotency_key=key,
                request_hash=request_hash,
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                payment_metadata=data.metadata,
                webhook_url=str(data.webhook_url),
            )
            .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
            .returning(Payment)
        )
        payment = (await session.scalars(statement)).one_or_none()
        if payment is None:
            payment = (
                await session.scalars(select(Payment).where(Payment.idempotency_key == key))
            ).one()
            if payment.request_hash != request_hash:
                raise IdempotencyConflict
        else:
            session.add(OutboxMessage(payment_id=payment.id, route=NEW_ROUTE, attempt=1))
        return PaymentAccepted(payment_id=payment.id, created_at=payment.created_at)


async def read_payment(
    sessions: async_sessionmaker[AsyncSession], payment_id: UUID
) -> PaymentDetails | None:
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        return None if payment is None else PaymentDetails.model_validate(payment)
