# This module contains the hard gates that decide whether a scoring run is safe to publish.
# It exists to fail fast when forecasts are malformed, incomplete, or based on stale inputs.
# The checks focus on correctness (no duplicates, valid bounds, sane ranges) and operational readiness (coverage and freshness).
# Keeping checks explicit and testable makes the scoring job production-friendly.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd


class ScoringCheckError(RuntimeError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = details or {}


@dataclass(frozen=True)
class ScoringCheckSummary:
    passed: bool
    failures: list[dict[str, Any]]
    warnings: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {"passed": self.passed, "failures": self.failures, "warnings": self.warnings}


def run_checks(
    *,
    forecasts: pd.DataFrame,
    zone_count: int,
    horizon_buckets: int,
    forecast_start_ts: datetime,
    zone_lineage: pd.DataFrame,
    min_zone_coverage_pct: float,
    max_feature_staleness_minutes: int,
) -> ScoringCheckSummary:
    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    expected_rows = int(zone_count * horizon_buckets)
    actual_rows = int(len(forecasts))
    if actual_rows != expected_rows:
        failures.append(
            {
                "check": "row_count",
                "expected_rows": expected_rows,
                "actual_rows": actual_rows,
            }
        )

    if forecasts.duplicated(subset=["zone_id", "bucket_start_ts"]).any():
        dupe_count = int(forecasts.duplicated(subset=["zone_id", "bucket_start_ts"]).sum())
        failures.append({"check": "duplicate_keys", "duplicate_rows": dupe_count})

    if (forecasts["y_pred"].astype(float) < 0).any():
        failures.append({"check": "nonnegative_pred", "message": "y_pred contains negative values"})

    if ((forecasts["confidence_score"].astype(float) < 0) | (forecasts["confidence_score"].astype(float) > 1)).any():
        failures.append({"check": "confidence_score_range", "message": "confidence_score outside [0,1]"})

    invalid_bounds = (
        (forecasts["y_pred_lower"].astype(float) > forecasts["y_pred_upper"].astype(float))
        | (forecasts["y_pred"].astype(float) < forecasts["y_pred_lower"].astype(float))
        | (forecasts["y_pred"].astype(float) > forecasts["y_pred_upper"].astype(float))
    )
    if invalid_bounds.any():
        failures.append({"check": "interval_bounds", "invalid_rows": int(invalid_bounds.sum())})

    if not zone_lineage.empty and "coverage_ratio" in zone_lineage.columns:
        coverage_ok = float((zone_lineage["coverage_ratio"].astype(float) >= 0.99).mean())
        if coverage_ok < min_zone_coverage_pct:
            failures.append(
                {
                    "check": "zone_history_coverage",
                    "min_required_pct": min_zone_coverage_pct,
                    "actual_pct": coverage_ok,
                }
            )

    if not zone_lineage.empty and "last_observed_bucket_ts" in zone_lineage.columns:
        last_ts = pd.to_datetime(zone_lineage["last_observed_bucket_ts"], utc=True).max()
        if pd.isna(last_ts):
            warnings.append({"check": "freshness", "message": "missing last_observed_bucket_ts"})
        else:
            staleness = forecast_start_ts - last_ts.to_pydatetime()
            if staleness > timedelta(minutes=max_feature_staleness_minutes):
                failures.append(
                    {
                        "check": "freshness",
                        "max_staleness_minutes": max_feature_staleness_minutes,
                        "actual_staleness_minutes": staleness.total_seconds() / 60.0,
                    }
                )

    if np.isnan(forecasts["y_pred"].astype(float)).any():
        failures.append({"check": "nan_predictions", "message": "y_pred contains NaNs"})

    passed = len(failures) == 0
    return ScoringCheckSummary(passed=passed, failures=failures, warnings=warnings)


def enforce_checks(summary: ScoringCheckSummary) -> None:
    if not summary.passed:
        raise ScoringCheckError("Scoring checks failed; forecasts not published.", details=summary.to_dict())

