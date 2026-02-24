# This module defines the runtime configuration for the scoring pipeline in Phase 5.
# It exists so scheduled jobs, ad-hoc runs, and backfills all share the same defaults and safety limits.
# The config is resolved from repo YAML defaults plus environment overrides to keep operations reproducible.
# Keeping these knobs centralized makes scoring behavior easier to audit and troubleshoot.

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import yaml

FEATURE_COLUMNS: list[str] = [
    "hour_of_day",
    "quarter_hour_index",
    "day_of_week",
    "is_weekend",
    "week_of_year",
    "month",
    "is_holiday",
    "lag_1",
    "lag_2",
    "lag_4",
    "lag_96",
    "lag_672",
    "roll_mean_4",
    "roll_mean_8",
    "roll_std_8",
    "roll_max_16",
]


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Config at {path} must be a mapping, got: {type(loaded).__name__}")
    return dict(loaded)


def _env_int(name: str, default: int | None = None) -> int | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _env_float(name: str, default: float | None = None) -> float | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def _env_str(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _env_bool(name: str, default: bool | None = None) -> bool | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value (true/false), got: {value!r}")


def _env_iso_dt(name: str) -> datetime | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware ISO8601, got naive datetime: {value!r}")
    return parsed


@dataclass(frozen=True)
class ScoringConfig:
    model_name: str
    model_stage: str
    feature_version: str
    policy_version: str
    horizon_buckets: int
    bucket_minutes: int
    run_timezone: str
    scoring_frequency_minutes: int
    forecast_start_override: datetime | None
    forecast_end_override: datetime | None
    max_zones: int | None
    lag_null_policy: str

    history_days: int
    history_extra_hours: int
    max_feature_staleness_minutes: int
    min_zone_coverage_pct: float

    confidence_backtest_days: int
    confidence_interval_quantile: float
    confidence_refresh_hours: int

    reports_dir: str
    mlflow_experiment_name: str

    prefect_work_pool: str
    prefect_work_queue: str
    stale_data_fallback_enabled: bool = False
    stale_data_floor_start_ts: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "model_stage": self.model_stage,
            "feature_version": self.feature_version,
            "policy_version": self.policy_version,
            "horizon_buckets": self.horizon_buckets,
            "bucket_minutes": self.bucket_minutes,
            "run_timezone": self.run_timezone,
            "scoring_frequency_minutes": self.scoring_frequency_minutes,
            "forecast_start_override": self.forecast_start_override.isoformat() if self.forecast_start_override else None,
            "forecast_end_override": self.forecast_end_override.isoformat() if self.forecast_end_override else None,
            "max_zones": self.max_zones,
            "lag_null_policy": self.lag_null_policy,
            "history_days": self.history_days,
            "history_extra_hours": self.history_extra_hours,
            "max_feature_staleness_minutes": self.max_feature_staleness_minutes,
            "min_zone_coverage_pct": self.min_zone_coverage_pct,
            "confidence_backtest_days": self.confidence_backtest_days,
            "confidence_interval_quantile": self.confidence_interval_quantile,
            "confidence_refresh_hours": self.confidence_refresh_hours,
            "reports_dir": self.reports_dir,
            "mlflow_experiment_name": self.mlflow_experiment_name,
            "prefect_work_pool": self.prefect_work_pool,
            "prefect_work_queue": self.prefect_work_queue,
            "stale_data_fallback_enabled": self.stale_data_fallback_enabled,
            "stale_data_floor_start_ts": (
                self.stale_data_floor_start_ts.isoformat() if self.stale_data_floor_start_ts else None
            ),
        }


def load_scoring_config(*, training_config_path: str = "configs/training.yaml") -> ScoringConfig:
    training_cfg = _load_yaml(training_config_path)
    data_cfg = dict(training_cfg.get("data", {}))
    registry_cfg = dict(training_cfg.get("registry", {}))

    default_model_name = str(registry_cfg.get("model_name", "ride-demand-forecast-model"))
    default_feature_version = str(data_cfg.get("feature_version", "v1"))
    default_policy_version = str(data_cfg.get("policy_version", "p1"))
    default_timezone = str(data_cfg.get("feature_timezone", os.getenv("FEATURE_TIMEZONE", "UTC")))

    model_name = str(_env_str("RIDE_DEMAND_MODEL_NAME", _env_str("SCORING_MODEL_NAME", default_model_name)))
    model_stage = str(_env_str("RIDE_DEMAND_MODEL_STAGE", _env_str("SCORING_MODEL_STAGE", "Staging")))

    feature_version = str(_env_str("SCORING_FEATURE_VERSION", default_feature_version))
    policy_version = str(_env_str("SCORING_POLICY_VERSION", default_policy_version))

    horizon_buckets = int(_env_int("SCORING_HORIZON_BUCKETS", 4) or 4)
    scoring_frequency_minutes = int(_env_int("SCORING_FREQUENCY_MINUTES", 15) or 15)
    run_timezone = str(_env_str("SCORING_TIMEZONE", default_timezone))

    forecast_start_override = _env_iso_dt("SCORING_FORECAST_START_TS")
    forecast_end_override = _env_iso_dt("SCORING_FORECAST_END_TS")

    max_zones = _env_int("SCORING_MAX_ZONES", None)
    lag_null_policy = str(_env_str("FEATURE_LAG_NULL_POLICY", "zero"))
    if lag_null_policy not in {"zero", "keep_nulls"}:
        raise ValueError("FEATURE_LAG_NULL_POLICY must be one of: zero, keep_nulls")

    history_days = int(_env_int("SCORING_HISTORY_DAYS", 7) or 7)
    history_extra_hours = int(_env_int("SCORING_HISTORY_EXTRA_HOURS", 4) or 4)
    max_feature_staleness_minutes = int(_env_int("SCORING_MAX_FEATURE_STALENESS_MINUTES", 60) or 60)
    min_zone_coverage_pct = float(_env_float("SCORING_MIN_ZONE_COVERAGE_PCT", 0.95) or 0.95)

    confidence_backtest_days = int(_env_int("SCORING_CONFIDENCE_BACKTEST_DAYS", 14) or 14)
    confidence_interval_quantile = float(_env_float("SCORING_CONFIDENCE_QUANTILE", 0.95) or 0.95)
    confidence_refresh_hours = int(_env_int("SCORING_CONFIDENCE_REFRESH_HOURS", 24) or 24)

    reports_dir = str(_env_str("SCORING_REPORTS_DIR", "reports/scoring"))
    mlflow_experiment_name = str(_env_str("SCORING_MLFLOW_EXPERIMENT", "ride-demand-scoring"))

    prefect_work_pool = str(_env_str("SCORING_PREFECT_WORK_POOL", "scoring-process"))
    prefect_work_queue = str(_env_str("SCORING_PREFECT_WORK_QUEUE", "scoring"))
    stale_data_fallback_enabled = bool(_env_bool("SCORING_STALE_DATA_FALLBACK_ENABLED", False))
    stale_data_floor_start_ts = _env_iso_dt("SCORING_STALE_DATA_FLOOR_START_TS")

    if horizon_buckets <= 0:
        raise ValueError(f"SCORING_HORIZON_BUCKETS must be > 0, got {horizon_buckets}")
    if scoring_frequency_minutes <= 0:
        raise ValueError(f"SCORING_FREQUENCY_MINUTES must be > 0, got {scoring_frequency_minutes}")
    bucket_minutes = 15

    return ScoringConfig(
        model_name=model_name,
        model_stage=model_stage,
        feature_version=feature_version,
        policy_version=policy_version,
        horizon_buckets=horizon_buckets,
        bucket_minutes=bucket_minutes,
        run_timezone=run_timezone,
        scoring_frequency_minutes=scoring_frequency_minutes,
        forecast_start_override=forecast_start_override,
        forecast_end_override=forecast_end_override,
        max_zones=max_zones,
        lag_null_policy=lag_null_policy,
        history_days=history_days,
        history_extra_hours=history_extra_hours,
        max_feature_staleness_minutes=max_feature_staleness_minutes,
        min_zone_coverage_pct=min_zone_coverage_pct,
        confidence_backtest_days=confidence_backtest_days,
        confidence_interval_quantile=confidence_interval_quantile,
        confidence_refresh_hours=confidence_refresh_hours,
        reports_dir=reports_dir,
        mlflow_experiment_name=mlflow_experiment_name,
        prefect_work_pool=prefect_work_pool,
        prefect_work_queue=prefect_work_queue,
        stale_data_fallback_enabled=stale_data_fallback_enabled,
        stale_data_floor_start_ts=stale_data_floor_start_ts,
    )
