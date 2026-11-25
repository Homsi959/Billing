from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from billing.domain import MAX_ATTEMPTS


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        CheckConstraint("amount > 0 AND amount != 'NaN'::numeric", name="ck_payments_amount"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint("jsonb_typeof(metadata) = 'object'", name="ck_payments_metadata"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"),
        CheckConstraint("(status = 'pending') = (processed_at IS NULL)", name="ck_payments_time"),
        CheckConstraint(
            "webhook_status IN ('pending', 'delivered', 'exhausted') AND "
            "(status != 'pending' OR webhook_status = 'pending')",
            name="ck_payments_webhook_status",
        ),
        CheckConstraint(f"attempt BETWEEN 1 AND {MAX_ATTEMPTS}", name="ck_payments_attempt"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    idempotency_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(500))
    payment_metadata: Mapped[dict[str, JsonValue]] = mapped_column("metadata", JSONB)
    webhook_url: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16), server_default="pending")
    webhook_status: Mapped[str] = mapped_column(String(16), server_default="pending")
    attempt: Mapped[int] = mapped_column(SmallInteger, server_default="1")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxMessage(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        UniqueConstraint("payment_id", "route", "attempt", name="uq_outbox_payment_route_attempt"),
        CheckConstraint(f"attempt BETWEEN 1 AND {MAX_ATTEMPTS}", name="ck_outbox_attempt"),
        CheckConstraint("route IN ('payments.new', 'payments.dead')", name="ck_outbox_route"),
        CheckConstraint(
            "(route = 'payments.dead') = (failure_code IS NOT NULL)", name="ck_outbox_failure"
        ),
        Index("ix_outbox_due", "available_at", "id", postgresql_where=text("published_at IS NULL")),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    route: Mapped[str] = mapped_column(String(32))
    attempt: Mapped[int] = mapped_column(SmallInteger)
    failure_code: Mapped[str | None] = mapped_column(String(64))
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
