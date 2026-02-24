# This test file validates reason-code generation for pricing guardrail outputs.
# It exists so each decision row remains explainable and policy-auditable.
# The tests check guardrail-triggered codes and deterministic primary reason selection.
# These in-memory assertions protect the reason taxonomy contract used by dashboards.

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from src.pricing_guardrails.reason_codes import apply_reason_codes


def _reason_config() -> dict[str, object]:
    return {
        "priority_order": [
            "CAP_APPLIED_GLOBAL",
            "RATE_LIMIT_INCREASE_CLAMP",
            "LOW_CONFIDENCE_DAMPENING",
            "HIGH_DEMAND_RATIO",
            "NORMAL_DEMAND_BASELINE",
        ],
        "codes": {
            "CAP_APPLIED_GLOBAL": {"description": "global cap"},
            "RATE_LIMIT_INCREASE_CLAMP": {"description": "rate clamp up"},
            "LOW_CONFIDENCE_DAMPENING": {"description": "low confidence"},
            "HIGH_DEMAND_RATIO": {"description": "high demand"},
            "NORMAL_DEMAND_BASELINE": {"description": "normal demand"},
            "BASELINE_FALLBACK_ZONE": {"description": "zone baseline"},
            "NO_PREVIOUS_MULTIPLIER_COLD_START": {"description": "cold start"},
            "SPARSE_ZONE_POLICY_ACTIVE": {"description": "sparse"},
            "SMOOTHING_APPLIED": {"description": "smoothing"},
        },
    }


def test_reason_codes_include_guardrail_and_signal_codes() -> None:
    frame = pd.DataFrame(
        {
            "zone_id": [1],
            "bucket_start_ts": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC)],
            "demand_ratio": [1.5],
            "baseline_reference_level": ["zone"],
            "low_confidence_adjusted": [True],
            "cap_applied": [True],
            "cap_type": ["global"],
            "cap_reason": ["global_cap"],
            "rate_limit_applied": [True],
            "rate_limit_direction": ["up"],
            "smoothing_applied": [True],
            "cold_start_used": [True],
            "zone_class": ["sparse"],
        }
    )

    out = apply_reason_codes(
        priced_frame=frame,
        reason_code_config=_reason_config(),
        high_demand_ratio_threshold=1.25,
    )

    codes = out.iloc[0]["reason_codes_json"]
    assert "HIGH_DEMAND_RATIO" in codes
    assert "CAP_APPLIED_GLOBAL" in codes
    assert "RATE_LIMIT_INCREASE_CLAMP" in codes
    assert out.iloc[0]["primary_reason_code"] == "CAP_APPLIED_GLOBAL"


def test_every_row_has_primary_reason() -> None:
    frame = pd.DataFrame(
        {
            "zone_id": [2],
            "bucket_start_ts": [datetime(2025, 1, 1, 0, 0, tzinfo=UTC)],
            "demand_ratio": [0.9],
            "baseline_reference_level": ["zone"],
            "low_confidence_adjusted": [False],
            "cap_applied": [False],
            "cap_type": [None],
            "cap_reason": [None],
            "rate_limit_applied": [False],
            "rate_limit_direction": ["none"],
            "smoothing_applied": [False],
            "cold_start_used": [False],
            "zone_class": ["robust"],
        }
    )

    out = apply_reason_codes(
        priced_frame=frame,
        reason_code_config=_reason_config(),
        high_demand_ratio_threshold=1.25,
    )

    assert out.iloc[0]["primary_reason_code"] != ""
    assert isinstance(out.iloc[0]["reason_codes_json"], list)
