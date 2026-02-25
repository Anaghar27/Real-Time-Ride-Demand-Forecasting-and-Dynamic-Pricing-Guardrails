# This file defines forecast endpoint schemas for forecast rows and run summaries.
# It exists so demand forecast responses remain explicit and stable for downstream tools.
# The row model carries confidence data and optional plain-language interpretation fields.
# Typed contracts here make schema-version checks practical and reliable.

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from src.api.schemas.common import EnvelopeFields, PaginationMetadata


class ForecastRowV1(BaseModel):
    zone_id: int
    bucket_start_ts: datetime
    forecast_run_key: str
    run_id: str
    horizon_index: int

    zone_name: str | None = None
    borough: str | None = None
    service_zone: str | None = None

    y_pred: float
    y_pred_lower: float
    y_pred_upper: float
    confidence_score: float
    uncertainty_band: str
    used_recursive_features: bool

    model_name: str
    model_version: str
    model_stage: str
    feature_version: str

    demand_outlook_label: str | None = None
    confidence_note: str | None = None
    forecast_range_summary: str | None = None


class ForecastListResponseV1(EnvelopeFields):
    data: list[ForecastRowV1]
    pagination: PaginationMetadata


class ForecastRunSummaryV1(BaseModel):
    run_id: str
    status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    failure_reason: str | None = None
    model_name: str | None = None
    model_version: str | None = None
    model_stage: str | None = None
    feature_version: str | None = None
    forecast_run_key: str | None = None
    forecast_start_ts: datetime | None = None
    forecast_end_ts: datetime | None = None
    horizon_buckets: int | None = None
    bucket_minutes: int | None = None
    zone_count: int | None = None
    row_count: int | None = None
    latency_ms: float | None = None


class ForecastRunSummaryResponseV1(EnvelopeFields):
    data: ForecastRunSummaryV1
