# This test module validates stale-data forecast window fallback behavior for scheduled scoring runs.
# It exists to ensure scoring can temporarily run from available historical boundaries and then return to normal once data catches up.

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest

from src.scoring.scoring_config import ScoringConfig, load_scoring_config
from src.scoring.scoring_orchestrator import _maybe_apply_stale_data_fallback


def _config(*, enabled: bool, floor_start: datetime | None = None) -> ScoringConfig:
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
        stale_data_fallback_enabled=enabled,
        stale_data_floor_start_ts=floor_start,
    )


def test_stale_data_fallback_uses_latest_observed_end_when_enabled() -> None:
    forecast_start = datetime(2026, 2, 24, 8, 0, tzinfo=UTC)
    cfg = _config(enabled=True, floor_start=datetime(2025, 11, 2, 0, 0, tzinfo=UTC))
    last_observed_end = pd.Timestamp("2025-11-03T00:00:00+00:00")

    adjusted_start, adjusted_end, used = _maybe_apply_stale_data_fallback(
        config=cfg,
        forecast_start_ts=forecast_start,
        horizon_buckets=4,
        explicit_window_override=False,
        staleness=pd.Timedelta(minutes=10_000),
        last_observed_end=last_observed_end,
    )

    assert used is True
    assert adjusted_start == datetime(2025, 11, 3, 0, 0, tzinfo=UTC)
    assert adjusted_end == datetime(2025, 11, 3, 1, 0, tzinfo=UTC)


def test_stale_data_fallback_respects_floor_start_when_it_is_later() -> None:
    forecast_start = datetime(2026, 2, 24, 8, 0, tzinfo=UTC)
    cfg = _config(enabled=True, floor_start=datetime(2025, 11, 2, 0, 15, tzinfo=UTC))
    last_observed_end = pd.Timestamp("2025-11-02T00:00:00+00:00")

    adjusted_start, adjusted_end, used = _maybe_apply_stale_data_fallback(
        config=cfg,
        forecast_start_ts=forecast_start,
        horizon_buckets=4,
        explicit_window_override=False,
        staleness=pd.Timedelta(minutes=10_000),
        last_observed_end=last_observed_end,
    )

    assert used is True
    assert adjusted_start == datetime(2025, 11, 2, 0, 15, tzinfo=UTC)
    assert adjusted_end == datetime(2025, 11, 2, 1, 15, tzinfo=UTC)


def test_stale_data_fallback_skips_when_override_is_explicit() -> None:
    forecast_start = datetime(2026, 2, 24, 8, 0, tzinfo=UTC)
    cfg = _config(enabled=True, floor_start=datetime(2025, 11, 2, 0, 0, tzinfo=UTC))
    last_observed_end = pd.Timestamp("2025-11-03T00:00:00+00:00")

    adjusted_start, adjusted_end, used = _maybe_apply_stale_data_fallback(
        config=cfg,
        forecast_start_ts=forecast_start,
        horizon_buckets=4,
        explicit_window_override=True,
        staleness=pd.Timedelta(minutes=10_000),
        last_observed_end=last_observed_end,
    )

    assert used is False
    assert adjusted_start == forecast_start
    assert adjusted_end == datetime(2026, 2, 24, 9, 0, tzinfo=UTC)


def test_load_scoring_config_reads_stale_data_fallback_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCORING_STALE_DATA_FALLBACK_ENABLED", "true")
    monkeypatch.setenv("SCORING_STALE_DATA_FLOOR_START_TS", "2025-11-02T00:00:00+00:00")

    cfg = load_scoring_config()

    assert cfg.stale_data_fallback_enabled is True
    assert cfg.stale_data_floor_start_ts == datetime(2025, 11, 2, 0, 0, tzinfo=UTC)
