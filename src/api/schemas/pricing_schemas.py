# This file defines pricing endpoint schemas for rows, list envelopes, and run summaries.
# It exists so pricing responses are strongly typed and backward-compatible for clients.
# The row model includes machine fields and optional plain-language fields in one contract.
# Keeping these models explicit helps catch accidental payload drift during development.

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from src.api.schemas.common import EnvelopeFields, PaginationMetadata


class PricingDecisionRowV1(BaseModel):
    zone_id: int
    bucket_start_ts: datetime
    pricing_run_key: str
    run_id: str
    forecast_run_id: str
    zone_name: str | None = None
    borough: str | None = None
    service_zone: str | None = None

    final_multiplier: float
    raw_multiplier: float
    pre_cap_multiplier: float
    post_cap_multiplier: float
    confidence_score: float
    uncertainty_band: str

    y_pred: float
    y_pred_lower: float
    y_pred_upper: float

    cap_applied: bool
    cap_type: str | None = None
    cap_reason: str | None = None
    rate_limit_applied: bool
    rate_limit_direction: str
    smoothing_applied: bool

    primary_reason_code: str
    reason_codes: list[str]
    reason_summary: str
    pricing_policy_version: str

    recommended_price_action: str | None = None
    why_this_price: str | None = None
    guardrail_note: str | None = None
    confidence_note: str | None = None


class PricingDecisionListResponseV1(EnvelopeFields):
    data: list[PricingDecisionRowV1]
    pagination: PaginationMetadata


class PricingRunSummaryV1(BaseModel):
    run_id: str
    status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    failure_reason: str | None = None
    pricing_policy_version: str | None = None
    forecast_run_id: str | None = None
    target_bucket_start: datetime | None = None
    target_bucket_end: datetime | None = None
    zone_count: int | None = None
    row_count: int | None = None
    cap_applied_count: int | None = None
    rate_limited_count: int | None = None
    low_confidence_count: int | None = None
    latency_ms: float | None = None


class PricingRunSummaryResponseV1(EnvelopeFields):
    data: PricingRunSummaryV1
