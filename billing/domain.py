import json
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    field_validator,
)

MAX_ATTEMPTS = 3
CENT = Decimal("0.01")
IdempotencyKey = Annotated[
    str, StringConstraints(min_length=1, max_length=128, pattern=r"^[!-~]+$")
]


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class WebhookStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"
    EXHAUSTED = "exhausted"


def validate_database_text(value: JsonValue) -> None:
    if isinstance(value, str):
        if "\x00" in value:
            raise ValueError("Remove NUL (U+0000) characters from the text.")
        value.encode("utf-8")
    elif isinstance(value, dict):
        for key, item in value.items():
            validate_database_text(key)
            validate_database_text(item)
    elif isinstance(value, list):
        for item in value:
            validate_database_text(item)


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: Annotated[str, StringConstraints(min_length=1, max_length=500)]
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: Annotated[AnyHttpUrl, Field(max_length=2048)]

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        return value.quantize(CENT)

    @field_validator("description", "metadata")
    @classmethod
    def check_database_values(cls, value: str | dict[str, JsonValue]) -> str | dict[str, JsonValue]:
        validate_database_text(value)
        if isinstance(value, dict):
            encoded = json.dumps(value, allow_nan=False, ensure_ascii=False, separators=(",", ":"))
            if len(encoded.encode()) > 16_384:
                raise ValueError("Send at most 16 KiB of metadata.")
        return value

    @field_validator("webhook_url")
    @classmethod
    def check_webhook_url(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if value.username is not None or value.password is not None or value.fragment is not None:
            raise ValueError("Remove the username, password and fragment from webhook_url.")
        return value

    def fingerprint(self) -> str:
        normalized = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return sha256(normalized.encode()).hexdigest()


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Literal[PaymentStatus.PENDING] = PaymentStatus.PENDING
    created_at: datetime


class PaymentDetails(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue] = Field(validation_alias="payment_metadata")
    webhook_url: str
    status: PaymentStatus
    webhook_status: WebhookStatus
    created_at: datetime
    processed_at: datetime | None


class PaymentJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_id: UUID
    attempt: Annotated[int, Field(strict=True, ge=1, le=MAX_ATTEMPTS)]


class PaymentProcessed(BaseModel):
    event: Literal["payment.processed"] = "payment.processed"
    event_id: UUID
    payment_id: UUID
    status: Literal[PaymentStatus.SUCCEEDED, PaymentStatus.FAILED]
    amount: Decimal
    currency: Currency
    metadata: dict[str, JsonValue]
    processed_at: datetime
