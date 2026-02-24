# This file defines runtime configuration for the pricing guardrails pipeline.
# It exists so ad hoc runs, backfills, and scheduled runs all use one consistent policy surface.
# The loader merges YAML defaults with environment overrides and validates required safety controls.
# Keeping these settings in one place makes pricing decisions reproducible and easier to audit.

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import yaml

VALID_SELECTION_MODES = {"latest_run", "explicit_run_id", "explicit_window"}
VALID_CREATED_AT_MODES = {"current_time", "override"}


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Config at {path} must be a mapping, got: {type(loaded).__name__}")
    return dict(loaded)


def _env_str(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value


def _env_float(name: str, default: float | None = None) -> float | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return float(value)


def _env_int(name: str, default: int | None = None) -> int | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


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


def _env_iso_ts(name: str) -> datetime | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware ISO8601, got: {value!r}")
    return parsed


def _as_float_mapping(value: Any, field_name: str) -> dict[str, float]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping of string->float")
    mapped: dict[str, float] = {}
    for key, raw in value.items():
        mapped[str(key)] = float(raw)
    return mapped


@dataclass(frozen=True)
class PricingConfig:
    pricing_policy_version: str
    forecast_table_name: str
    pricing_output_table_name: str
    forecast_selection_mode: str
    explicit_forecast_run_id: str | None
    explicit_window_start: datetime | None
    explicit_window_end: datetime | None

    pricing_created_at_mode: str
    pricing_created_at_override: datetime | None
    run_timezone: str

    default_floor_multiplier: float
    global_cap_multiplier: float
    cap_by_confidence_band: dict[str, float]
    cap_by_zone_class: dict[str, float]
    cap_by_time_category: dict[str, float]

    max_increase_per_bucket: float
    max_decrease_per_bucket: float

    smoothing_enabled: bool
    smoothing_alpha: float

    low_confidence_adjustment_enabled: bool
    low_confidence_threshold: float
    low_confidence_dampening_factor: float
    low_confidence_uncertainty_bands: list[str]

    baseline_reference_mode: str
    baseline_lookback_days: int
    baseline_min_value: float

    allow_discounting: bool
    discount_floor_multiplier: float
    cold_start_multiplier: float
    max_zones: int | None

    strict_checks: bool
    coverage_threshold_pct: float
    row_count_tolerance_pct: float

    policy_snapshot_enabled: bool
    report_sample_size: int

    prefect_schedule_minutes: int
    prefect_work_pool: str
    prefect_work_queue: str

    def effective_floor_multiplier(self) -> float:
        if self.allow_discounting:
            return min(self.default_floor_multiplier, self.discount_floor_multiplier)
        return self.default_floor_multiplier

    def to_dict(self) -> dict[str, Any]:
        return {
            "pricing_policy_version": self.pricing_policy_version,
            "forecast_table_name": self.forecast_table_name,
            "pricing_output_table_name": self.pricing_output_table_name,
            "forecast_selection_mode": self.forecast_selection_mode,
            "explicit_forecast_run_id": self.explicit_forecast_run_id,
            "explicit_window_start": self.explicit_window_start.isoformat() if self.explicit_window_start else None,
            "explicit_window_end": self.explicit_window_end.isoformat() if self.explicit_window_end else None,
            "pricing_created_at_mode": self.pricing_created_at_mode,
            "pricing_created_at_override": (
                self.pricing_created_at_override.isoformat() if self.pricing_created_at_override else None
            ),
            "run_timezone": self.run_timezone,
            "default_floor_multiplier": self.default_floor_multiplier,
            "global_cap_multiplier": self.global_cap_multiplier,
            "cap_by_confidence_band": dict(self.cap_by_confidence_band),
            "cap_by_zone_class": dict(self.cap_by_zone_class),
            "cap_by_time_category": dict(self.cap_by_time_category),
            "max_increase_per_bucket": self.max_increase_per_bucket,
            "max_decrease_per_bucket": self.max_decrease_per_bucket,
            "smoothing_enabled": self.smoothing_enabled,
            "smoothing_alpha": self.smoothing_alpha,
            "low_confidence_adjustment_enabled": self.low_confidence_adjustment_enabled,
            "low_confidence_threshold": self.low_confidence_threshold,
            "low_confidence_dampening_factor": self.low_confidence_dampening_factor,
            "low_confidence_uncertainty_bands": list(self.low_confidence_uncertainty_bands),
            "baseline_reference_mode": self.baseline_reference_mode,
            "baseline_lookback_days": self.baseline_lookback_days,
            "baseline_min_value": self.baseline_min_value,
            "allow_discounting": self.allow_discounting,
            "discount_floor_multiplier": self.discount_floor_multiplier,
            "cold_start_multiplier": self.cold_start_multiplier,
            "max_zones": self.max_zones,
            "strict_checks": self.strict_checks,
            "coverage_threshold_pct": self.coverage_threshold_pct,
            "row_count_tolerance_pct": self.row_count_tolerance_pct,
            "policy_snapshot_enabled": self.policy_snapshot_enabled,
            "report_sample_size": self.report_sample_size,
            "prefect_schedule_minutes": self.prefect_schedule_minutes,
            "prefect_work_pool": self.prefect_work_pool,
            "prefect_work_queue": self.prefect_work_queue,
        }


def load_pricing_config(*, config_path: str = "configs/pricing_policy.yaml") -> PricingConfig:
    cfg = _load_yaml(config_path)
    pricing_created_at_cfg = dict(cfg.get("pricing_created_at", {}))
    low_conf_cfg = dict(cfg.get("low_confidence_adjustment", {}))
    prefect_cfg = dict(cfg.get("prefect", {}))

    pricing_policy_version = str(_env_str("PRICING_POLICY_VERSION", str(cfg.get("pricing_policy_version", "pr1"))))
    forecast_table_name = str(_env_str("PRICING_FORECAST_TABLE_NAME", str(cfg.get("forecast_table_name", "demand_forecast"))))
    pricing_output_table_name = str(
        _env_str("PRICING_OUTPUT_TABLE_NAME", str(cfg.get("pricing_output_table_name", "pricing_decisions")))
    )

    forecast_selection_mode = str(
        _env_str("PRICING_FORECAST_SELECTION_MODE", str(cfg.get("forecast_selection_mode", "latest_run")))
    )
    explicit_forecast_run_id = _env_str("PRICING_FORECAST_RUN_ID", cfg.get("explicit_forecast_run_id"))
    explicit_window_start = _env_iso_ts("PRICING_FORECAST_START_TS")
    explicit_window_end = _env_iso_ts("PRICING_FORECAST_END_TS")
    if explicit_window_start is None and cfg.get("explicit_window_start"):
        explicit_window_start = datetime.fromisoformat(str(cfg["explicit_window_start"]).replace("Z", "+00:00"))
    if explicit_window_end is None and cfg.get("explicit_window_end"):
        explicit_window_end = datetime.fromisoformat(str(cfg["explicit_window_end"]).replace("Z", "+00:00"))

    pricing_created_at_mode = str(
        _env_str("PRICING_CREATED_AT_MODE", str(pricing_created_at_cfg.get("mode", "current_time")))
    )
    pricing_created_at_override = _env_iso_ts("PRICING_CREATED_AT_OVERRIDE_TS")
    if pricing_created_at_override is None and pricing_created_at_cfg.get("override_ts"):
        pricing_created_at_override = datetime.fromisoformat(
            str(pricing_created_at_cfg["override_ts"]).replace("Z", "+00:00")
        )

    run_timezone = str(_env_str("PRICING_RUN_TIMEZONE", str(cfg.get("run_timezone", "UTC"))))
    default_floor_multiplier = float(_env_float("PRICING_DEFAULT_FLOOR_MULTIPLIER", float(cfg.get("default_floor_multiplier", 1.0))) or 1.0)
    global_cap_multiplier = float(_env_float("PRICING_GLOBAL_CAP_MULTIPLIER", float(cfg.get("global_cap_multiplier", 2.5))) or 2.5)

    cap_by_confidence_band = _as_float_mapping(cfg.get("cap_by_confidence_band", {}), "cap_by_confidence_band")
    cap_by_zone_class = _as_float_mapping(cfg.get("cap_by_zone_class", {}), "cap_by_zone_class")
    cap_by_time_category = _as_float_mapping(cfg.get("cap_by_time_category", {}), "cap_by_time_category")

    max_increase_per_bucket = float(_env_float("PRICING_MAX_INCREASE_PER_BUCKET", float(cfg.get("max_increase_per_bucket", 0.2))) or 0.2)
    max_decrease_per_bucket = float(_env_float("PRICING_MAX_DECREASE_PER_BUCKET", float(cfg.get("max_decrease_per_bucket", 0.15))) or 0.15)

    smoothing_enabled = bool(_env_bool("PRICING_SMOOTHING_ENABLED", bool(cfg.get("smoothing_enabled", False))))
    smoothing_alpha = float(_env_float("PRICING_SMOOTHING_ALPHA", float(cfg.get("smoothing_alpha", 0.7))) or 0.7)

    low_confidence_adjustment_enabled = bool(
        _env_bool("PRICING_LOW_CONFIDENCE_ADJUSTMENT_ENABLED", bool(low_conf_cfg.get("enabled", False)))
    )
    low_confidence_threshold = float(
        _env_float("PRICING_LOW_CONFIDENCE_THRESHOLD", float(low_conf_cfg.get("confidence_threshold", 0.45)))
        or 0.45
    )
    low_confidence_dampening_factor = float(
        _env_float(
            "PRICING_LOW_CONFIDENCE_DAMPENING_FACTOR",
            float(low_conf_cfg.get("dampening_factor", 0.6)),
        )
        or 0.6
    )
    low_confidence_uncertainty_bands = [str(item) for item in list(low_conf_cfg.get("uncertainty_bands", []))]

    baseline_reference_mode = str(_env_str("PRICING_BASELINE_REFERENCE_MODE", str(cfg.get("baseline_reference_mode", "fact_feature_average"))))
    baseline_lookback_days = int(_env_int("PRICING_BASELINE_LOOKBACK_DAYS", int(cfg.get("baseline_lookback_days", 28))) or 28)
    baseline_min_value = float(_env_float("PRICING_BASELINE_MIN_VALUE", float(cfg.get("baseline_min_value", 0.5))) or 0.5)

    allow_discounting = bool(_env_bool("PRICING_ALLOW_DISCOUNTING", bool(cfg.get("allow_discounting", False))))
    discount_floor_multiplier = float(
        _env_float("PRICING_DISCOUNT_FLOOR_MULTIPLIER", float(cfg.get("discount_floor_multiplier", 1.0))) or 1.0
    )
    cold_start_multiplier = float(
        _env_float("PRICING_COLD_START_MULTIPLIER", float(cfg.get("cold_start_multiplier", default_floor_multiplier)))
        or default_floor_multiplier
    )
    max_zones = _env_int("PRICING_MAX_ZONES", cfg.get("max_zones"))

    strict_checks = bool(_env_bool("PRICING_STRICT_CHECKS", bool(cfg.get("strict_checks", True))))
    coverage_threshold_pct = float(
        _env_float("PRICING_COVERAGE_THRESHOLD_PCT", float(cfg.get("coverage_threshold_pct", 0.95))) or 0.95
    )
    row_count_tolerance_pct = float(
        _env_float("PRICING_ROW_COUNT_TOLERANCE_PCT", float(cfg.get("row_count_tolerance_pct", 0.02))) or 0.02
    )

    policy_snapshot_enabled = bool(_env_bool("PRICING_POLICY_SNAPSHOT_ENABLED", bool(cfg.get("policy_snapshot_enabled", True))))
    report_sample_size = int(_env_int("PRICING_REPORT_SAMPLE_SIZE", int(cfg.get("report_sample_size", 300))) or 300)

    prefect_schedule_minutes = int(_env_int("PRICING_SCHEDULE_MINUTES", int(prefect_cfg.get("schedule_minutes", 15))) or 15)
    prefect_work_pool = str(_env_str("PRICING_PREFECT_WORK_POOL", str(prefect_cfg.get("work_pool", "pricing-process"))))
    prefect_work_queue = str(_env_str("PRICING_PREFECT_WORK_QUEUE", str(prefect_cfg.get("work_queue", "pricing"))))

    if forecast_selection_mode not in VALID_SELECTION_MODES:
        raise ValueError(
            f"PRICING_FORECAST_SELECTION_MODE must be one of {sorted(VALID_SELECTION_MODES)}, got {forecast_selection_mode}"
        )
    if forecast_selection_mode == "explicit_run_id" and not explicit_forecast_run_id:
        raise ValueError("explicit_run_id mode requires PRICING_FORECAST_RUN_ID or explicit_forecast_run_id")
    if forecast_selection_mode == "explicit_window" and (explicit_window_start is None or explicit_window_end is None):
        raise ValueError("explicit_window mode requires PRICING_FORECAST_START_TS and PRICING_FORECAST_END_TS")
    if explicit_window_start and explicit_window_end and explicit_window_end <= explicit_window_start:
        raise ValueError("PRICING_FORECAST_END_TS must be greater than PRICING_FORECAST_START_TS")

    if pricing_created_at_mode not in VALID_CREATED_AT_MODES:
        raise ValueError(
            f"PRICING_CREATED_AT_MODE must be one of {sorted(VALID_CREATED_AT_MODES)}, got {pricing_created_at_mode}"
        )
    if pricing_created_at_mode == "override" and pricing_created_at_override is None:
        raise ValueError("PRICING_CREATED_AT_MODE=override requires PRICING_CREATED_AT_OVERRIDE_TS")

    if default_floor_multiplier < 0:
        raise ValueError("default_floor_multiplier must be nonnegative")
    if global_cap_multiplier <= 0:
        raise ValueError("global_cap_multiplier must be > 0")
    if global_cap_multiplier < default_floor_multiplier:
        raise ValueError("global_cap_multiplier cannot be below default_floor_multiplier")
    if max_increase_per_bucket < 0 or max_decrease_per_bucket < 0:
        raise ValueError("rate-limit deltas must be nonnegative")
    if not (0 < smoothing_alpha <= 1):
        raise ValueError("smoothing_alpha must be in (0, 1]")
    if not (0 <= low_confidence_threshold <= 1):
        raise ValueError("low_confidence_threshold must be in [0, 1]")
    if not (0 <= low_confidence_dampening_factor <= 1):
        raise ValueError("low_confidence_dampening_factor must be in [0, 1]")
    if baseline_lookback_days <= 0:
        raise ValueError("baseline_lookback_days must be > 0")
    if baseline_min_value <= 0:
        raise ValueError("baseline_min_value must be > 0")
    if coverage_threshold_pct <= 0 or coverage_threshold_pct > 1:
        raise ValueError("coverage_threshold_pct must be in (0, 1]")

    return PricingConfig(
        pricing_policy_version=pricing_policy_version,
        forecast_table_name=forecast_table_name,
        pricing_output_table_name=pricing_output_table_name,
        forecast_selection_mode=forecast_selection_mode,
        explicit_forecast_run_id=explicit_forecast_run_id,
        explicit_window_start=explicit_window_start,
        explicit_window_end=explicit_window_end,
        pricing_created_at_mode=pricing_created_at_mode,
        pricing_created_at_override=pricing_created_at_override,
        run_timezone=run_timezone,
        default_floor_multiplier=default_floor_multiplier,
        global_cap_multiplier=global_cap_multiplier,
        cap_by_confidence_band=cap_by_confidence_band,
        cap_by_zone_class=cap_by_zone_class,
        cap_by_time_category=cap_by_time_category,
        max_increase_per_bucket=max_increase_per_bucket,
        max_decrease_per_bucket=max_decrease_per_bucket,
        smoothing_enabled=smoothing_enabled,
        smoothing_alpha=smoothing_alpha,
        low_confidence_adjustment_enabled=low_confidence_adjustment_enabled,
        low_confidence_threshold=low_confidence_threshold,
        low_confidence_dampening_factor=low_confidence_dampening_factor,
        low_confidence_uncertainty_bands=low_confidence_uncertainty_bands,
        baseline_reference_mode=baseline_reference_mode,
        baseline_lookback_days=baseline_lookback_days,
        baseline_min_value=baseline_min_value,
        allow_discounting=allow_discounting,
        discount_floor_multiplier=discount_floor_multiplier,
        cold_start_multiplier=cold_start_multiplier,
        max_zones=max_zones,
        strict_checks=strict_checks,
        coverage_threshold_pct=coverage_threshold_pct,
        row_count_tolerance_pct=row_count_tolerance_pct,
        policy_snapshot_enabled=policy_snapshot_enabled,
        report_sample_size=report_sample_size,
        prefect_schedule_minutes=prefect_schedule_minutes,
        prefect_work_pool=prefect_work_pool,
        prefect_work_queue=prefect_work_queue,
    )


def resolve_pricing_created_at(config: PricingConfig, *, override_ts: datetime | None = None) -> datetime:
    if override_ts is not None:
        return override_ts
    if config.pricing_created_at_mode == "override":
        if config.pricing_created_at_override is None:
            raise ValueError("pricing_created_at_override is required when pricing_created_at_mode='override'")
        return config.pricing_created_at_override
    return datetime.now(tz=UTC)
