# This file provides dependency factories for FastAPI routes and middleware.
# It exists so services are created once and shared through dependency injection.
# The setup keeps routers thin and makes endpoint tests easy to override.
# Centralized construction also ensures one consistent API configuration is used.

from __future__ import annotations

from functools import lru_cache

from src.api.api_config import ApiConfig, get_api_config
from src.api.db_access import DatabaseClient
from src.api.services.diagnostics_service import DiagnosticsService
from src.api.services.forecast_service import ForecastService
from src.api.services.metadata_service import MetadataService
from src.api.services.pricing_service import PricingService


@lru_cache(maxsize=1)
def get_database_client() -> DatabaseClient:
    config = get_api_config()
    return DatabaseClient(database_url=config.database_url)


@lru_cache(maxsize=1)
def get_pricing_service() -> PricingService:
    config = get_api_config()
    db_client = get_database_client()
    return PricingService(config=config, db=db_client)


@lru_cache(maxsize=1)
def get_forecast_service() -> ForecastService:
    config = get_api_config()
    db_client = get_database_client()
    return ForecastService(config=config, db=db_client)


@lru_cache(maxsize=1)
def get_metadata_service() -> MetadataService:
    config = get_api_config()
    db_client = get_database_client()
    return MetadataService(config=config, db=db_client)


@lru_cache(maxsize=1)
def get_diagnostics_service() -> DiagnosticsService:
    config = get_api_config()
    db_client = get_database_client()
    return DiagnosticsService(config=config, db=db_client)


def get_config() -> ApiConfig:
    return get_api_config()
