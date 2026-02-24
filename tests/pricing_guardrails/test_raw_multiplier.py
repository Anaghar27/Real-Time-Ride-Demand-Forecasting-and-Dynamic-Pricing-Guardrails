# This test file validates raw multiplier computation behavior for pricing guardrails.
# It exists to ensure both mapping methods are deterministic and safe before caps are applied.
# The tests also cover low-confidence dampening and baseline fallback tiers.
# Keeping these checks in memory makes CI fast and regression-resistant.

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from src.pricing_guardrails.baseline_reference import BaselineTables, merge_baseline_reference
from src.pricing_guardrails.multiplier_engine import compute_raw_multiplier
from src.pricing_guardrails.pricing_config import PricingConfig


def _config() -> PricingConfig:
    return PricingConfig(
        pricing_policy_version="pr1",
        forecast_table_name="demand_forecast",
        pricing_output_table_name="pricing_decisions",
        forecast_selection_mode="latest_run",
        explicit_forecast_run_id=None,
        explicit_window_start=None,
        explicit_window_end=None,
        pricing_created_at_mode="current_time",
        pricing_created_at_override=None,
        run_timezone="UTC",
        default_floor_multiplier=1.0,
        global_cap_multiplier=2.5,
        cap_by_confidence_band={},
        cap_by_zone_class={},
        cap_by_time_category={},
        max_increase_per_bucket=0.2,
        max_decrease_per_bucket=0.15,
        smoothing_enabled=False,
        smoothing_alpha=0.7,
        low_confidence_adjustment_enabled=True,
        low_confidence_threshold=0.5,
        low_confidence_dampening_factor=0.5,
        low_confidence_uncertainty_bands=["high"],
        baseline_reference_mode="fact_feature_average",
        baseline_lookback_days=28,
        baseline_min_value=0.5,
        allow_discounting=False,
        discount_floor_multiplier=1.0,
        cold_start_multiplier=1.0,
        max_zones=None,
        strict_checks=True,
        coverage_threshold_pct=0.95,
        row_count_tolerance_pct=0.02,
        policy_snapshot_enabled=True,
        report_sample_size=100,
        prefect_schedule_minutes=15,
        prefect_work_pool="pricing-process",
        prefect_work_queue="pricing",
    )


def test_piecewise_ratio_multiplier_and_low_confidence_dampening() -> None:
    config = _config()
    frame = pd.DataFrame(
        {
            "zone_id": [1],
            "bucket_start_ts": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC)],
            "y_pred": [20.0],
            "baseline_expected_demand": [10.0],
            "confidence_score": [0.2],
            "uncertainty_band": ["high"],
        }
    )
    rules = {
        "active_method": "demand_ratio_piecewise",
        "methods": {
            "demand_ratio_piecewise": {
                "breakpoints": [
                    {"ratio": 0.0, "multiplier": 1.0},
                    {"ratio": 1.0, "multiplier": 1.1},
                    {"ratio": 2.0, "multiplier": 2.0},
                ]
            }
        },
    }

    result = compute_raw_multiplier(
        forecasts_with_baseline=frame,
        pricing_config=config,
        multiplier_rules=rules,
    )

    assert float(result["demand_ratio"].iloc[0]) == 2.0
    assert float(result["raw_multiplier"].iloc[0]) == 2.0
    assert float(result["pre_guardrail_multiplier"].iloc[0]) == 1.5
    assert bool(result["low_confidence_adjusted"].iloc[0]) is True


def test_threshold_band_multiplier() -> None:
    config = _config()
    config = PricingConfig(**{**config.to_dict(), "low_confidence_adjustment_enabled": False})

    frame = pd.DataFrame(
        {
            "zone_id": [1, 2],
            "bucket_start_ts": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC), datetime(2025, 1, 1, 0, 0, tzinfo=UTC)],
            "y_pred": [8.0, 20.0],
            "baseline_expected_demand": [10.0, 10.0],
            "confidence_score": [0.9, 0.9],
            "uncertainty_band": ["low", "low"],
        }
    )
    rules = {
        "active_method": "threshold_bands",
        "methods": {
            "threshold_bands": {
                "metric": "demand_ratio",
                "bands": [
                    {"min_inclusive": 0.0, "max_exclusive": 1.0, "multiplier": 1.0},
                    {"min_inclusive": 1.0, "max_exclusive": 1.5, "multiplier": 1.3},
                    {"min_inclusive": 1.5, "max_exclusive": None, "multiplier": 1.8},
                ],
            }
        },
    }

    result = compute_raw_multiplier(
        forecasts_with_baseline=frame,
        pricing_config=config,
        multiplier_rules=rules,
    )

    assert list(result["raw_multiplier"].astype(float)) == [1.0, 1.8]


def test_baseline_fallback_hierarchy() -> None:
    config = _config()
    forecasts = pd.DataFrame(
        {
            "zone_id": [1, 2, 3],
            "bucket_start_ts": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC)] * 3,
            "y_pred": [10.0, 10.0, 10.0],
            "day_of_week": [2, 2, 2],
            "quarter_hour_index": [0, 0, 0],
        }
    )

    tables = BaselineTables(
        zone=pd.DataFrame(
            {
                "zone_id": [1],
                "day_of_week": [2],
                "quarter_hour_index": [0],
                "baseline_expected_demand_zone": [12.0],
            }
        ),
        borough=pd.DataFrame(
            {
                "borough": ["Manhattan"],
                "day_of_week": [2],
                "quarter_hour_index": [0],
                "baseline_expected_demand_borough": [9.0],
            }
        ),
        city=pd.DataFrame(
            {
                "day_of_week": [2],
                "quarter_hour_index": [0],
                "baseline_expected_demand_city": [7.0],
            }
        ),
        zone_lookup=pd.DataFrame({"zone_id": [1, 2], "borough": ["Manhattan", "Manhattan"]}),
    )

    merged = merge_baseline_reference(forecasts=forecasts, baseline_tables=tables, pricing_config=config)

    levels = list(merged.sort_values("zone_id")["baseline_reference_level"])
    assert levels == ["zone", "borough", "city"]
