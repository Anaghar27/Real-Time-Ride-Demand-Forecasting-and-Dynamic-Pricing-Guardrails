# This test file checks that critical tooltip keys exist for dashboard interpretation.
# It exists so narrative explainability does not silently regress during UI changes.
# The assertions target metrics, charts, and guardrail transparency tooltips.
# Ensuring key coverage supports the non-technical decision-support requirement.

from __future__ import annotations

from src.dashboard_user.tooltips import TOOLTIPS


def test_required_tooltip_keys_exist() -> None:
    required_keys = {
        "zones_covered_card",
        "avg_final_multiplier_card",
        "count_capped_card",
        "count_rate_limited_card",
        "low_confidence_share_card",
        "multiplier_distribution_chart",
        "final_vs_raw_multiplier",
        "cap_rate_limit_flags",
        "confidence_score",
        "forecast_interval_band",
        "uncertainty_band",
        "confidence_conservativeness",
        "cap_protection",
        "rate_limit_protection",
        "reason_code_existence",
        "cap_by_borough_chart",
        "cap_by_hour_chart",
        "rate_by_borough_chart",
        "rate_by_hour_chart",
        "reason_code_summary_table",
    }

    missing_keys = required_keys.difference(TOOLTIPS.keys())
    assert not missing_keys

    for key in required_keys:
        assert isinstance(TOOLTIPS[key], str)
        assert TOOLTIPS[key].strip()
