# This test module validates Phase 5 confidence interval and scoring logic.
# It exists to ensure every forecast row gets nonnegative bounds, a confidence score in [0,1], and a stable uncertainty band.
# The tests run purely in memory using small pandas frames so CI does not need Postgres or MLflow.
# This provides a safety net for future refactors of the confidence method.

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from src.scoring.confidence import ConfidenceReference, apply_confidence
from src.scoring.scoring_config import ScoringConfig


def _config() -> ScoringConfig:
    return ScoringConfig(
        model_name="ride-demand-forecast-model",
        model_stage="Staging",
        feature_version="v1",
        policy_version="p1",
        horizon_buckets=4,
        bucket_minutes=15,
        run_timezone="UTC",
        scoring_frequency_minutes=15,
        forecast_start_override=None,
        forecast_end_override=None,
        max_zones=None,
        lag_null_policy="zero",
        history_days=7,
        history_extra_hours=4,
        max_feature_staleness_minutes=60,
        min_zone_coverage_pct=0.95,
        confidence_backtest_days=14,
        confidence_interval_quantile=0.95,
        confidence_refresh_hours=24,
        reports_dir="reports/scoring",
        mlflow_experiment_name="ride-demand-scoring",
        prefect_work_pool="scoring-process",
        prefect_work_queue="scoring",
    )


def test_confidence_bounds_and_score_ranges() -> None:
    cfg = _config()
    forecasts = pd.DataFrame(
        {
            "zone_id": [1, 2],
            "bucket_start_ts": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC), datetime(2025, 1, 1, 0, 0, tzinfo=UTC)],
            "hour_of_day": [0, 0],
            "quarter_hour_index": [0, 0],
            "day_of_week": [3, 3],
            "is_weekend": [False, False],
            "week_of_year": [1, 1],
            "month": [1, 1],
            "is_holiday": [False, False],
            "lag_1": [10.0, 5.0],
            "lag_2": [10.0, 5.0],
            "lag_4": [10.0, 5.0],
            "lag_96": [10.0, 5.0],
            "lag_672": [10.0, 5.0],
            "roll_mean_4": [10.0, 5.0],
            "roll_mean_8": [10.0, 5.0],
            "roll_std_8": [1.0, 1.0],
            "roll_max_16": [12.0, 6.0],
            "used_recursive_features": [False, False],
            "horizon_index": [1, 1],
            "y_pred": [8.0, 2.0],
        }
    )

    reference_table = pd.DataFrame(
        {
            "segment_key": ["all"],
            "hour_of_day": [0],
            "q50_abs_error": [1.0],
            "q90_abs_error": [2.0],
            "q95_abs_error": [3.0],
            "updated_at": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC)],
            "source_window": ["2024-12-01..2024-12-15"],
        }
    )
    reference = ConfidenceReference(table=reference_table, updated_at=reference_table["updated_at"].iloc[0], source_window="x")

    scored = apply_confidence(forecasts=forecasts, reference=reference, zone_policy=pd.DataFrame(), config=cfg)

    assert (scored["y_pred_lower"] >= 0).all()
    assert (scored["y_pred_upper"] >= scored["y_pred"]).all()
    assert (scored["y_pred"] >= scored["y_pred_lower"]).all()
    assert ((scored["confidence_score"] >= 0) & (scored["confidence_score"] <= 1)).all()
    assert set(scored["uncertainty_band"].unique()).issubset({"low", "medium", "high"})


def test_sparse_segment_reduces_confidence() -> None:
    cfg = _config()
    forecasts = pd.DataFrame(
        {
            "zone_id": [1, 2],
            "bucket_start_ts": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC)] * 2,
            "hour_of_day": [0, 0],
            "quarter_hour_index": [0, 0],
            "day_of_week": [3, 3],
            "is_weekend": [False, False],
            "week_of_year": [1, 1],
            "month": [1, 1],
            "is_holiday": [False, False],
            "lag_1": [10.0, 10.0],
            "lag_2": [10.0, 10.0],
            "lag_4": [10.0, 10.0],
            "lag_96": [10.0, 10.0],
            "lag_672": [10.0, 10.0],
            "roll_mean_4": [10.0, 10.0],
            "roll_mean_8": [10.0, 10.0],
            "roll_std_8": [1.0, 1.0],
            "roll_max_16": [12.0, 12.0],
            "used_recursive_features": [False, False],
            "horizon_index": [1, 1],
            "y_pred": [10.0, 10.0],
        }
    )
    reference_table = pd.DataFrame(
        {
            "segment_key": ["robust", "sparse"],
            "hour_of_day": [0, 0],
            "q50_abs_error": [1.0, 1.0],
            "q90_abs_error": [2.0, 2.0],
            "q95_abs_error": [3.0, 3.0],
            "updated_at": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC)] * 2,
            "source_window": ["x", "x"],
        }
    )
    reference = ConfidenceReference(table=reference_table, updated_at=reference_table["updated_at"].iloc[0], source_window="x")
    zone_policy = pd.DataFrame({"zone_id": [1, 2], "segment_key": ["robust", "sparse"]})

    scored = apply_confidence(forecasts=forecasts, reference=reference, zone_policy=zone_policy, config=cfg)
    robust_conf = float(scored.loc[scored["zone_id"] == 1, "confidence_score"].iloc[0])
    sparse_conf = float(scored.loc[scored["zone_id"] == 2, "confidence_score"].iloc[0])
    assert sparse_conf < robust_conf

