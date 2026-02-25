# This file defines response schemas for health, readiness, and version endpoints.
# It exists to keep operational status contracts explicit for platform consumers.
# The models include request tracing and version metadata for observability.
# Stable health schemas make monitoring checks straightforward to automate.

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class HealthResponse(BaseModel):
    api_version: str
    schema_version: str
    request_id: str
    status: str
    environment: str
    service_name: str
    timestamp: datetime


class ReadinessResponse(BaseModel):
    api_version: str
    schema_version: str
    request_id: str
    db_connected: bool
    pricing_source_ready: bool
    forecast_source_ready: bool
    ready: bool
    database: str
    timestamp: datetime


class VersionResponse(BaseModel):
    api_version: str
    schema_version: str
    request_id: str
    api_version_path: str
    app_version: str
    git_commit: str | None = None
    project: str
    version: str
    timestamp: datetime
