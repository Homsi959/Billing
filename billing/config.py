from typing import Annotated

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BILLING_")

    database_url: SecretStr


class ApiSettings(DatabaseSettings):
    api_key: Annotated[SecretStr, Field(min_length=24)]


class WorkerSettings(DatabaseSettings):
    rabbitmq_url: SecretStr
    webhook_private_hosts: frozenset[str] = frozenset()
    webhook_timeout: Annotated[float, Field(gt=0, le=10)] = 5.0
    retry_base_seconds: Annotated[float, Field(gt=0, le=300)] = 1.0
    outbox_poll_seconds: Annotated[float, Field(gt=0, le=10)] = 0.25
