# This module persists pricing decisions and pricing run logs into Postgres.
# It exists to make pricing output idempotent and safe to rerun for the same forecast window.
# A deterministic pricing run key and upsert semantics prevent duplicate logical rows.
# The writer enforces contract columns so downstream dashboards get stable fields.

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _safe_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return identifier


def pricing_run_key(
    *,
    pricing_policy_version: str,
    forecast_run_id: str,
    target_bucket_start: datetime,
    target_bucket_end: datetime,
) -> str:
    raw = (
        f"{pricing_policy_version}|{forecast_run_id}|"
        f"{target_bucket_start.isoformat()}|{target_bucket_end.isoformat()}"
    ).encode()
    return hashlib.sha256(raw).hexdigest()[:24]


@dataclass(frozen=True)
class PricingRunLogRow:
    run_id: str
    pricing_run_key: str | None
    started_at: datetime
    ended_at: datetime | None
    status: str
    failure_reason: str | None
    pricing_policy_version: str
    forecast_run_id: str | None
    target_bucket_start: datetime | None
    target_bucket_end: datetime | None
    zone_count: int | None
    row_count: int | None
    cap_applied_count: int | None
    rate_limited_count: int | None
    low_confidence_count: int | None
    latency_ms: float | None
    config_snapshot: dict[str, Any]
    check_summary: dict[str, Any] | None
    artifacts_path: str | None

    def to_params(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pricing_run_key": self.pricing_run_key,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "status": self.status,
            "failure_reason": self.failure_reason,
            "pricing_policy_version": self.pricing_policy_version,
            "forecast_run_id": self.forecast_run_id,
            "target_bucket_start": self.target_bucket_start,
            "target_bucket_end": self.target_bucket_end,
            "zone_count": self.zone_count,
            "row_count": self.row_count,
            "cap_applied_count": self.cap_applied_count,
            "rate_limited_count": self.rate_limited_count,
            "low_confidence_count": self.low_confidence_count,
            "latency_ms": self.latency_ms,
            "config_snapshot": json.dumps(self.config_snapshot, default=str),
            "check_summary": json.dumps(self.check_summary, default=str) if self.check_summary is not None else None,
            "artifacts_path": self.artifacts_path,
        }


def upsert_pricing_run_log(*, engine: Engine, row: PricingRunLogRow) -> None:
    statement = text(
        """
        INSERT INTO pricing_run_log (
            run_id,
            pricing_run_key,
            started_at,
            ended_at,
            status,
            failure_reason,
            pricing_policy_version,
            forecast_run_id,
            target_bucket_start,
            target_bucket_end,
            zone_count,
            row_count,
            cap_applied_count,
            rate_limited_count,
            low_confidence_count,
            latency_ms,
            config_snapshot,
            check_summary,
            artifacts_path
        ) VALUES (
            :run_id,
            :pricing_run_key,
            :started_at,
            :ended_at,
            :status,
            :failure_reason,
            :pricing_policy_version,
            :forecast_run_id,
            :target_bucket_start,
            :target_bucket_end,
            :zone_count,
            :row_count,
            :cap_applied_count,
            :rate_limited_count,
            :low_confidence_count,
            :latency_ms,
            CAST(:config_snapshot AS JSONB),
            CAST(:check_summary AS JSONB),
            :artifacts_path
        )
        ON CONFLICT (run_id) DO UPDATE SET
            pricing_run_key = EXCLUDED.pricing_run_key,
            ended_at = EXCLUDED.ended_at,
            status = EXCLUDED.status,
            failure_reason = EXCLUDED.failure_reason,
            forecast_run_id = EXCLUDED.forecast_run_id,
            target_bucket_start = EXCLUDED.target_bucket_start,
            target_bucket_end = EXCLUDED.target_bucket_end,
            zone_count = EXCLUDED.zone_count,
            row_count = EXCLUDED.row_count,
            cap_applied_count = EXCLUDED.cap_applied_count,
            rate_limited_count = EXCLUDED.rate_limited_count,
            low_confidence_count = EXCLUDED.low_confidence_count,
            latency_ms = EXCLUDED.latency_ms,
            config_snapshot = EXCLUDED.config_snapshot,
            check_summary = EXCLUDED.check_summary,
            artifacts_path = EXCLUDED.artifacts_path
        """
    )
    with engine.begin() as connection:
        connection.execute(statement, row.to_params())


def upsert_pricing_decisions(
    *,
    engine: Engine,
    pricing_output_table_name: str,
    pricing_frame: pd.DataFrame,
) -> int:
    table_name = _safe_identifier(pricing_output_table_name)
    required = {
        "zone_id",
        "bucket_start_ts",
        "pricing_created_at",
        "pricing_run_key",
        "horizon_index",
        "forecast_run_id",
        "forecast_created_at",
        "y_pred",
        "y_pred_lower",
        "y_pred_upper",
        "confidence_score",
        "uncertainty_band",
        "model_name",
        "model_version",
        "model_stage",
        "feature_version",
        "baseline_expected_demand",
        "baseline_reference_level",
        "demand_ratio",
        "raw_multiplier",
        "pre_cap_multiplier",
        "post_cap_multiplier",
        "candidate_multiplier_before_rate_limit",
        "final_multiplier",
        "cap_applied",
        "cap_type",
        "cap_reason",
        "cap_value",
        "rate_limit_applied",
        "rate_limit_direction",
        "previous_final_multiplier",
        "smoothing_applied",
        "fallback_applied",
        "primary_reason_code",
        "reason_codes_json",
        "reason_summary",
        "pricing_policy_version",
        "run_id",
        "status",
    }
    missing = sorted(required.difference(pricing_frame.columns))
    if missing:
        raise ValueError(f"pricing dataframe missing required columns: {missing}")

    payload_frame = pricing_frame[list(required)].copy()
    payload_frame["reason_codes_json"] = payload_frame["reason_codes_json"].apply(lambda value: json.dumps(value))
    payload: list[dict[str, Any]] = payload_frame.to_dict(orient="records")

    statement = text(
        f"""
        INSERT INTO {table_name} (
            zone_id,
            bucket_start_ts,
            pricing_created_at,
            pricing_run_key,
            horizon_index,
            forecast_run_id,
            forecast_created_at,
            y_pred,
            y_pred_lower,
            y_pred_upper,
            confidence_score,
            uncertainty_band,
            model_name,
            model_version,
            model_stage,
            feature_version,
            baseline_expected_demand,
            baseline_reference_level,
            demand_ratio,
            raw_multiplier,
            pre_cap_multiplier,
            post_cap_multiplier,
            candidate_multiplier_before_rate_limit,
            final_multiplier,
            cap_applied,
            cap_type,
            cap_reason,
            cap_value,
            rate_limit_applied,
            rate_limit_direction,
            previous_final_multiplier,
            smoothing_applied,
            fallback_applied,
            primary_reason_code,
            reason_codes_json,
            reason_summary,
            pricing_policy_version,
            run_id,
            status,
            created_at
        ) VALUES (
            :zone_id,
            :bucket_start_ts,
            :pricing_created_at,
            :pricing_run_key,
            :horizon_index,
            :forecast_run_id,
            :forecast_created_at,
            :y_pred,
            :y_pred_lower,
            :y_pred_upper,
            :confidence_score,
            :uncertainty_band,
            :model_name,
            :model_version,
            :model_stage,
            :feature_version,
            :baseline_expected_demand,
            :baseline_reference_level,
            :demand_ratio,
            :raw_multiplier,
            :pre_cap_multiplier,
            :post_cap_multiplier,
            :candidate_multiplier_before_rate_limit,
            :final_multiplier,
            :cap_applied,
            :cap_type,
            :cap_reason,
            :cap_value,
            :rate_limit_applied,
            :rate_limit_direction,
            :previous_final_multiplier,
            :smoothing_applied,
            :fallback_applied,
            :primary_reason_code,
            CAST(:reason_codes_json AS JSONB),
            :reason_summary,
            :pricing_policy_version,
            :run_id,
            :status,
            NOW()
        )
        ON CONFLICT (pricing_run_key, zone_id, bucket_start_ts) DO UPDATE SET
            pricing_created_at = EXCLUDED.pricing_created_at,
            horizon_index = EXCLUDED.horizon_index,
            forecast_run_id = EXCLUDED.forecast_run_id,
            forecast_created_at = EXCLUDED.forecast_created_at,
            y_pred = EXCLUDED.y_pred,
            y_pred_lower = EXCLUDED.y_pred_lower,
            y_pred_upper = EXCLUDED.y_pred_upper,
            confidence_score = EXCLUDED.confidence_score,
            uncertainty_band = EXCLUDED.uncertainty_band,
            model_name = EXCLUDED.model_name,
            model_version = EXCLUDED.model_version,
            model_stage = EXCLUDED.model_stage,
            feature_version = EXCLUDED.feature_version,
            baseline_expected_demand = EXCLUDED.baseline_expected_demand,
            baseline_reference_level = EXCLUDED.baseline_reference_level,
            demand_ratio = EXCLUDED.demand_ratio,
            raw_multiplier = EXCLUDED.raw_multiplier,
            pre_cap_multiplier = EXCLUDED.pre_cap_multiplier,
            post_cap_multiplier = EXCLUDED.post_cap_multiplier,
            candidate_multiplier_before_rate_limit = EXCLUDED.candidate_multiplier_before_rate_limit,
            final_multiplier = EXCLUDED.final_multiplier,
            cap_applied = EXCLUDED.cap_applied,
            cap_type = EXCLUDED.cap_type,
            cap_reason = EXCLUDED.cap_reason,
            cap_value = EXCLUDED.cap_value,
            rate_limit_applied = EXCLUDED.rate_limit_applied,
            rate_limit_direction = EXCLUDED.rate_limit_direction,
            previous_final_multiplier = EXCLUDED.previous_final_multiplier,
            smoothing_applied = EXCLUDED.smoothing_applied,
            fallback_applied = EXCLUDED.fallback_applied,
            primary_reason_code = EXCLUDED.primary_reason_code,
            reason_codes_json = EXCLUDED.reason_codes_json,
            reason_summary = EXCLUDED.reason_summary,
            pricing_policy_version = EXCLUDED.pricing_policy_version,
            run_id = EXCLUDED.run_id,
            status = EXCLUDED.status,
            created_at = NOW()
        """
    )
    with engine.begin() as connection:
        connection.execute(statement, payload)
    return len(payload)


def utc_now() -> datetime:
    return datetime.now(tz=UTC)
