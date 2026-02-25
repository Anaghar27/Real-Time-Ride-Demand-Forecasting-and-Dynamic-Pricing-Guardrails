# This file defines lightweight diagnostics response schemas for API consumers.
# It exists so coverage, guardrail, and confidence summaries are consistently shaped.
# The models provide quick operational insight without exposing internal pipeline details.
# Explicit schemas make diagnostics endpoints easier to validate in contract tests.

from __future__ import annotations

from pydantic import BaseModel

from src.api.schemas.common import EnvelopeFields


class CoverageSummaryV1(BaseModel):
    pricing_run_id: str | None = None
    forecast_run_id: str | None = None
    pricing_zone_count: int
    forecast_zone_count: int
    pricing_row_count: int
    forecast_row_count: int


class CoverageSummaryResponseV1(EnvelopeFields):
    data: CoverageSummaryV1


class GuardrailUsageSummaryV1(BaseModel):
    pricing_run_id: str | None = None
    total_rows: int
    cap_applied_rows: int
    rate_limited_rows: int
    smoothing_applied_rows: int
    cap_applied_rate: float
    rate_limited_rate: float


class GuardrailUsageSummaryResponseV1(EnvelopeFields):
    data: GuardrailUsageSummaryV1


class ConfidenceBandSummaryRowV1(BaseModel):
    uncertainty_band: str
    row_count: int
    avg_confidence_score: float


class ConfidenceSummaryV1(BaseModel):
    forecast_run_id: str | None = None
    bands: list[ConfidenceBandSummaryRowV1]


class ConfidenceSummaryResponseV1(EnvelopeFields):
    data: ConfidenceSummaryV1
