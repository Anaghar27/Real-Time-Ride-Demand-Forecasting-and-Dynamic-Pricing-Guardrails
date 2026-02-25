# This file defines liveness, readiness, and version endpoints for API operations.
# It exists so orchestration and monitoring systems can verify service health quickly.
# The readiness check confirms database connectivity and source table availability.
# Version details here help clients track API and schema compatibility over time.

from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Request

from src.api.api_config import ApiConfig
from src.api.db_access import DatabaseClient
from src.api.dependencies import get_config, get_database_client
from src.api.schema_versions import build_version_fields
from src.api.schemas.health_schemas import HealthResponse, ReadinessResponse, VersionResponse

router = APIRouter(tags=["health"])
ConfigDep = Annotated[ApiConfig, Depends(get_config)]
DBDep = Annotated[DatabaseClient, Depends(get_database_client)]


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _git_commit() -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        value = completed.stdout.strip()
        return value or None
    except Exception:
        return None


@router.get("/health", response_model=HealthResponse)
def health(
    request: Request,
    config: ConfigDep,
) -> dict[str, object]:
    return {
        **build_version_fields(
            api_version_path=config.api_version_path,
            schema_version=config.schema_version,
        ),
        "request_id": request.state.request_id,
        "status": "ok",
        "environment": config.environment,
        "service_name": config.api_name,
        "timestamp": _utc_now(),
    }


@router.get("/ready", response_model=ReadinessResponse)
def ready(
    request: Request,
    config: ConfigDep,
    db: DBDep,
) -> dict[str, object]:
    db_connected = db.can_connect()
    pricing_source_ready = db_connected and db.table_exists(config.pricing_table_name)
    forecast_source_ready = db_connected and db.table_exists(config.forecast_table_name)
    is_ready = db_connected and pricing_source_ready and forecast_source_ready

    return {
        **build_version_fields(
            api_version_path=config.api_version_path,
            schema_version=config.schema_version,
        ),
        "request_id": request.state.request_id,
        "db_connected": db_connected,
        "pricing_source_ready": pricing_source_ready,
        "forecast_source_ready": forecast_source_ready,
        "ready": is_ready,
        "database": "reachable" if db_connected else "unreachable",
        "timestamp": _utc_now(),
    }


@router.get("/version", response_model=VersionResponse)
def version(
    request: Request,
    config: ConfigDep,
) -> dict[str, object]:
    return {
        **build_version_fields(
            api_version_path=config.api_version_path,
            schema_version=config.schema_version,
        ),
        "request_id": request.state.request_id,
        "api_version_path": config.api_version_path,
        "app_version": config.app_version,
        "git_commit": _git_commit(),
        "project": config.api_name,
        "version": config.app_version,
        "timestamp": _utc_now(),
    }
