from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from billing.config import DatabaseSettings


def make_engine(settings: DatabaseSettings) -> AsyncEngine:
    return create_async_engine(
        settings.database_url.get_secret_value(),
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={"timeout": 5, "command_timeout": 10},
    )
