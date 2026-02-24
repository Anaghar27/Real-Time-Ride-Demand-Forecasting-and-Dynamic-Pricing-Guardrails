# This test file validates rate limiter and smoothing behavior in pricing guardrails.
# It exists to ensure multiplier deltas are clamped against configured per-bucket bounds.
# The cases also verify cold-start handling and post-smoothing bound enforcement.
# Pure DataFrame tests keep execution deterministic and quick.

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from src.pricing_guardrails.pricing_config import PricingConfig
from src.pricing_guardrails.rate_limiter import apply_rate_limiter


def _config(*, smoothing_enabled: bool) -> PricingConfig:
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
        global_cap_multiplier=2.0,
        cap_by_confidence_band={},
        cap_by_zone_class={},
        cap_by_time_category={},
        max_increase_per_bucket=0.2,
        max_decrease_per_bucket=0.15,
        smoothing_enabled=smoothing_enabled,
        smoothing_alpha=0.5,
        low_confidence_adjustment_enabled=False,
        low_confidence_threshold=0.5,
        low_confidence_dampening_factor=0.5,
        low_confidence_uncertainty_bands=[],
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


def test_rate_limit_clamps_up_and_down() -> None:
    config = _config(smoothing_enabled=False)
    frame = pd.DataFrame(
        {
            "zone_id": [1, 1],
            "bucket_start_ts": [
                datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 0, 15, tzinfo=UTC),
            ],
            "post_cap_multiplier": [1.6, 0.8],
        }
    )

    out = apply_rate_limiter(capped_frame=frame, pricing_config=config, previous_multiplier_map={1: 1.0})

    first = out.iloc[0]
    second = out.iloc[1]
    assert float(first["final_multiplier"]) == 1.2
    assert str(first["rate_limit_direction"]) == "up"
    assert float(second["final_multiplier"]) == 1.05
    assert str(second["rate_limit_direction"]) == "down"


def test_smoothing_keeps_bounds_and_marks_cold_start() -> None:
    config = _config(smoothing_enabled=True)
    frame = pd.DataFrame(
        {
            "zone_id": [2],
            "bucket_start_ts": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC)],
            "post_cap_multiplier": [1.8],
        }
    )

    out = apply_rate_limiter(capped_frame=frame, pricing_config=config, previous_multiplier_map={})
    row = out.iloc[0]

    assert bool(row["cold_start_used"]) is True
    assert bool(row["smoothing_applied"]) is True
    assert 1.0 <= float(row["final_multiplier"]) <= 1.8
