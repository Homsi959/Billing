import asyncio

from alembic import context
from sqlalchemy.engine import Connection

from billing.config import DatabaseSettings
from billing.db import make_engine
from billing.models import Base


def configure(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()


async def migrate() -> None:
    engine = make_engine(DatabaseSettings())
    try:
        async with engine.connect() as connection:
            await connection.run_sync(configure)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    context.configure(dialect_name="postgresql", target_metadata=Base.metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(migrate())
