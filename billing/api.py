from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from secrets import compare_digest
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request, Response, Security
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import async_sessionmaker

from billing.config import ApiSettings
from billing.db import make_engine
from billing.domain import IdempotencyKey, PaymentAccepted, PaymentCreate, PaymentDetails
from billing.payments import IdempotencyConflict, accept_payment, read_payment

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    settings = settings if settings is not None else ApiSettings()
    engine = make_engine(settings)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    expected_key = settings.api_key.get_secret_value().encode()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await engine.dispose()

    async def authorize(key: Annotated[str | None, Security(api_key_header)]) -> None:
        if key is None or not compare_digest(key.encode(), expected_key):
            raise HTTPException(
                401,
                "Send a valid API key in the X-API-Key header.",
                headers={"WWW-Authenticate": "ApiKey"},
            )

    app = FastAPI(
        title="Billing",
        version="1.0.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_request: Request, error: RequestValidationError) -> JSONResponse:
        details = [{key: item[key] for key in ("loc", "msg", "type")} for item in error.errors()]
        return JSONResponse(status_code=422, content={"detail": details})

    router = APIRouter(dependencies=[Depends(authorize)])

    @router.get("/openapi.json", include_in_schema=False)
    async def openapi_schema() -> dict:
        return app.openapi()

    @router.post(
        "/api/v1/payments",
        status_code=202,
        responses={
            401: {"description": "The X-API-Key header is missing or invalid."},
            409: {"description": "This Idempotency-Key was used for a different request."},
        },
    )
    async def create_payment(
        data: PaymentCreate,
        response: Response,
        idempotency_key: Annotated[IdempotencyKey, Header()],
    ) -> PaymentAccepted:
        try:
            accepted = await accept_payment(sessions, data, idempotency_key)
        except IdempotencyConflict:
            raise HTTPException(
                409, "This Idempotency-Key was already used with different payment details."
            ) from None
        response.headers["Location"] = f"/api/v1/payments/{accepted.payment_id}"
        return accepted

    @router.get(
        "/api/v1/payments/{payment_id}",
        responses={
            401: {"description": "The X-API-Key header is missing or invalid."},
            404: {"description": "No payment matches this ID."},
        },
    )
    async def get_payment(payment_id: UUID) -> PaymentDetails:
        payment = await read_payment(sessions, payment_id)
        if payment is None:
            raise HTTPException(404, "No payment matches this ID.")
        return payment

    app.include_router(router)
    return app
