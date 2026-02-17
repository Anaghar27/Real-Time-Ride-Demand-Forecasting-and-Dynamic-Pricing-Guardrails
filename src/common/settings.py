"""
Application settings loaded from environment variables.
It centralizes cross-cutting concerns like settings, logging, and database access used by the pipelines.
Keeping these helpers isolated reduces duplication and keeps domain modules focused on business logic.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Final

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, ValidationError

REQUIRED_ENV_VARS: Final[tuple[str, ...]] = (
    "PROJECT_NAME",
    "ENV",
    "LOG_LEVEL",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "DATABASE_URL",
    "MLFLOW_TRACKING_URI",
    "API_HOST",
    "API_PORT",
    "PREFECT_API_URL",
    "PROMETHEUS_PORT",
    "GRAFANA_PORT",
)


class Settings(BaseModel):
    """Typed runtime configuration."""

    model_config = ConfigDict(extra="ignore")

    PROJECT_NAME: str
    ENV: str
    LOG_LEVEL: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    DATABASE_URL: str
    MLFLOW_TRACKING_URI: str
    API_HOST: str
    API_PORT: int
    PREFECT_API_URL: str
    PROMETHEUS_PORT: int
    GRAFANA_PORT: int


def load_settings(*, load_env: bool = True) -> Settings:
    """Load and validate environment settings from `.env` and process environment."""

    if load_env:
        load_dotenv()

    missing = [key for key in REQUIRED_ENV_VARS if not os.getenv(key)]
    if missing:
        missing_values = ", ".join(sorted(missing))
        raise RuntimeError(
            f"Missing required environment variables: {missing_values}. "
            "Populate these values in `.env` before starting the application."
        )

    try:
        return Settings.model_validate(dict(os.environ))
    except ValidationError as exc:
        raise RuntimeError(f"Invalid environment configuration: {exc}") from exc


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached accessor for application settings."""

    return load_settings()
