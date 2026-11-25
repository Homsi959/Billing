"""Create payments and transactional outbox."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(500), nullable=False),
        sa.Column("metadata", JSONB(), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("webhook_status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("attempt", sa.SmallInteger(), nullable=False, server_default="1"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        sa.CheckConstraint("amount > 0 AND amount != 'NaN'::numeric", name="ck_payments_amount"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        sa.CheckConstraint("jsonb_typeof(metadata) = 'object'", name="ck_payments_metadata"),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"
        ),
        sa.CheckConstraint(
            "(status = 'pending') = (processed_at IS NULL)", name="ck_payments_time"
        ),
        sa.CheckConstraint(
            "webhook_status IN ('pending', 'delivered', 'exhausted') AND "
            "(status != 'pending' OR webhook_status = 'pending')",
            name="ck_payments_webhook_status",
        ),
        sa.CheckConstraint("attempt BETWEEN 1 AND 3", name="ck_payments_attempt"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("route", sa.String(32), nullable=False),
        sa.Column("attempt", sa.SmallInteger(), nullable=False),
        sa.Column("failure_code", sa.String(64)),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint(
            "payment_id", "route", "attempt", name="uq_outbox_payment_route_attempt"
        ),
        sa.CheckConstraint("attempt BETWEEN 1 AND 3", name="ck_outbox_attempt"),
        sa.CheckConstraint("route IN ('payments.new', 'payments.dead')", name="ck_outbox_route"),
        sa.CheckConstraint(
            "(route = 'payments.dead') = (failure_code IS NOT NULL)", name="ck_outbox_failure"
        ),
    )
    op.create_index(
        "ix_outbox_due",
        "outbox",
        ["available_at", "id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_due", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payments")
