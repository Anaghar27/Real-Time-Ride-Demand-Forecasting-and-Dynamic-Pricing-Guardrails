# This file defines shared schema pieces reused by multiple API endpoints.
# It exists so envelope metadata, pagination, and error payloads stay consistent.
# Shared models reduce duplication and keep contract changes easier to review.
# These classes are also used by tests to validate response shape stability.

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class PaginationMetadata(BaseModel):
    page: int = Field(ge=1)
    page_size: int = Field(ge=1)
    total_count: int = Field(ge=0)
    total_pages: int = Field(ge=0)
    sort: str


class EnvelopeFields(BaseModel):
    api_version: str
    schema_version: str
    request_id: str
    generated_at: datetime
    warnings: list[str] | None = None


class ErrorResponse(BaseModel):
    error_code: str
    message: str
    details: Any | None = None
    request_id: str
    timestamp: datetime


class RunSummaryV1(BaseModel):
    run_id: str
    status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    row_count: int | None = None
    zone_count: int | None = None
    latency_ms: float | None = None
