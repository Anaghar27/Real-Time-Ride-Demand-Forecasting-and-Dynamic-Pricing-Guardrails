# This test file validates cap and floor guardrail behavior for pricing decisions.
# It exists to keep cap precedence deterministic and policy-compliant across refactors.
# The tests check floor application, contextual caps, and global hard-stop behavior.
# In-memory frames are used so the suite remains fast and stable.

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from src.pricing_guardrails.cap_guardrail import apply_cap_guardrail
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
        global_cap_multiplier=2.0,
        cap_by_confidence_band={"high": 1.4},
        cap_by_zone_class={"sparse": 1.3},
        cap_by_time_category={},
        max_increase_per_bucket=0.2,
        max_decrease_per_bucket=0.15,
        smoothing_enabled=False,
        smoothing_alpha=0.7,
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


def test_floor_then_contextual_cap_precedence() -> None:
    config = _config()
    frame = pd.DataFrame(
        {
            "zone_id": [1, 2],
            "bucket_start_ts": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC)] * 2,
            "pre_guardrail_multiplier": [0.8, 1.8],
            "uncertainty_band": ["low", "high"],
            "zone_class": ["robust", "sparse"],
        }
    )

    out = apply_cap_guardrail(raw_frame=frame, pricing_config=config)
    row1 = out[out["zone_id"] == 1].iloc[0]
    row2 = out[out["zone_id"] == 2].iloc[0]

    assert float(row1["post_cap_multiplier"]) == 1.0
    assert bool(row1["cap_applied"]) is True
    assert str(row1["cap_type"]) == "floor"

    assert float(row2["post_cap_multiplier"]) == 1.3
    assert bool(row2["cap_applied"]) is True
    assert str(row2["cap_type"]) == "contextual"
    assert str(row2["cap_reason"]) == "sparse_zone"


def test_global_cap_is_final_hard_stop() -> None:
    config = _config()
    frame = pd.DataFrame(
        {
            "zone_id": [1],
            "bucket_start_ts": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC)],
            "pre_guardrail_multiplier": [5.0],
            "uncertainty_band": ["low"],
            "zone_class": ["robust"],
        }
    )

    out = apply_cap_guardrail(raw_frame=frame, pricing_config=config)
    row = out.iloc[0]

    assert float(row["post_cap_multiplier"]) == 2.0
    assert str(row["cap_type"]) == "global"
