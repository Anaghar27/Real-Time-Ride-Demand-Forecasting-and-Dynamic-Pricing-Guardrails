# This module persists scoring outputs and run metadata into Postgres.
# It exists to make scoring idempotent and to provide a stable contract table for downstream pricing consumers.
# Forecast writes use an upsert on a deterministic run key so reruns update rows instead of duplicating them.
# Run logs are stored alongside forecasts so operators can audit success, failure reasons, and basic performance stats.

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class ScoringRunLogRow:
    run_id: str
    started_at: datetime
    ended_at: datetime | None
    status: str
    failure_reason: str | None
    model_name: str
    model_version: str | None
    model_stage: str
    feature_version: str
    forecast_run_key: str | None
    scoring_created_at: datetime
    forecast_start_ts: datetime
    forecast_end_ts: datetime
    horizon_buckets: int
    bucket_minutes: int
    zone_count: int | None
    row_count: int | None
    latency_ms: float | None
    confidence_reference_updated_at: datetime | None
    config_snapshot: dict[str, Any]

    def to_params(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "status": self.status,
            "failure_reason": self.failure_reason,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "model_stage": self.model_stage,
            "feature_version": self.feature_version,
            "forecast_run_key": self.forecast_run_key,
            "scoring_created_at": self.scoring_created_at,
            "forecast_start_ts": self.forecast_start_ts,
            "forecast_end_ts": self.forecast_end_ts,
            "horizon_buckets": self.horizon_buckets,
            "bucket_minutes": self.bucket_minutes,
            "zone_count": self.zone_count,
            "row_count": self.row_count,
            "latency_ms": self.latency_ms,
            "confidence_reference_updated_at": self.confidence_reference_updated_at,
            "config_snapshot": json.dumps(self.config_snapshot, default=str),
        }


def upsert_scoring_run_log(*, engine: Engine, row: ScoringRunLogRow) -> None:
    statement = text(
        """
        INSERT INTO scoring_run_log (
            run_id,
            started_at,
            ended_at,
            status,
            failure_reason,
            model_name,
            model_version,
            model_stage,
            feature_version,
            forecast_run_key,
            scoring_created_at,
            forecast_start_ts,
            forecast_end_ts,
            horizon_buckets,
            bucket_minutes,
            zone_count,
            row_count,
            latency_ms,
            confidence_reference_updated_at,
            config_snapshot
        ) VALUES (
            :run_id,
            :started_at,
            :ended_at,
            :status,
            :failure_reason,
            :model_name,
            :model_version,
            :model_stage,
            :feature_version,
            :forecast_run_key,
            :scoring_created_at,
            :forecast_start_ts,
            :forecast_end_ts,
            :horizon_buckets,
            :bucket_minutes,
            :zone_count,
            :row_count,
            :latency_ms,
            :confidence_reference_updated_at,
            CAST(:config_snapshot AS JSONB)
        )
        ON CONFLICT (run_id) DO UPDATE SET
            ended_at = EXCLUDED.ended_at,
            status = EXCLUDED.status,
            failure_reason = EXCLUDED.failure_reason,
            model_version = EXCLUDED.model_version,
            forecast_run_key = EXCLUDED.forecast_run_key,
            zone_count = EXCLUDED.zone_count,
            row_count = EXCLUDED.row_count,
            latency_ms = EXCLUDED.latency_ms,
            confidence_reference_updated_at = EXCLUDED.confidence_reference_updated_at,
            config_snapshot = EXCLUDED.config_snapshot
        """
    )
    with engine.begin() as connection:
        connection.execute(statement, row.to_params())


def upsert_demand_forecast(*, engine: Engine, forecasts: pd.DataFrame) -> int:
    required = {
        "forecast_run_key",
        "zone_id",
        "bucket_start_ts",
        "forecast_created_at",
        "horizon_index",
        "y_pred",
        "y_pred_lower",
        "y_pred_upper",
        "confidence_score",
        "uncertainty_band",
        "used_recursive_features",
        "model_name",
        "model_version",
        "model_stage",
        "feature_version",
        "run_id",
        "scoring_window_start",
        "scoring_window_end",
    }
    missing = required.difference(forecasts.columns)
    if missing:
        raise ValueError(f"forecast dataframe missing required columns: {sorted(missing)}")

    payload: list[dict[str, Any]] = forecasts[list(required)].to_dict(orient="records")
    statement = text(
        """
        INSERT INTO demand_forecast (
            forecast_run_key,
            zone_id,
            bucket_start_ts,
            forecast_created_at,
            horizon_index,
            y_pred,
            y_pred_lower,
            y_pred_upper,
            confidence_score,
            uncertainty_band,
            used_recursive_features,
            model_name,
            model_version,
            model_stage,
            feature_version,
            run_id,
            scoring_window_start,
            scoring_window_end,
            created_at
        ) VALUES (
            :forecast_run_key,
            :zone_id,
            :bucket_start_ts,
            :forecast_created_at,
            :horizon_index,
            :y_pred,
            :y_pred_lower,
            :y_pred_upper,
            :confidence_score,
            :uncertainty_band,
            :used_recursive_features,
            :model_name,
            :model_version,
            :model_stage,
            :feature_version,
            :run_id,
            :scoring_window_start,
            :scoring_window_end,
            NOW()
        )
        ON CONFLICT (forecast_run_key, zone_id, bucket_start_ts) DO UPDATE SET
            forecast_created_at = EXCLUDED.forecast_created_at,
            horizon_index = EXCLUDED.horizon_index,
            y_pred = EXCLUDED.y_pred,
            y_pred_lower = EXCLUDED.y_pred_lower,
            y_pred_upper = EXCLUDED.y_pred_upper,
            confidence_score = EXCLUDED.confidence_score,
            uncertainty_band = EXCLUDED.uncertainty_band,
            used_recursive_features = EXCLUDED.used_recursive_features,
            model_name = EXCLUDED.model_name,
            model_version = EXCLUDED.model_version,
            model_stage = EXCLUDED.model_stage,
            feature_version = EXCLUDED.feature_version,
            run_id = EXCLUDED.run_id,
            scoring_window_start = EXCLUDED.scoring_window_start,
            scoring_window_end = EXCLUDED.scoring_window_end,
            created_at = NOW()
        """
    )

    with engine.begin() as connection:
        connection.execute(statement, payload)
    return int(len(payload))


def utc_now() -> datetime:
    return datetime.now(tz=UTC)

