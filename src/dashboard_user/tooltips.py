# This file defines narrative tooltip text for metrics, charts, and guardrail counters.
# It exists so dashboard users can interpret pricing and forecast signals without technical background.
# A single dictionary keeps explanations consistent between pages and tests.
# These tooltips focus on actionability, not model internals.

from __future__ import annotations

TOOLTIPS: dict[str, str] = {
    "latest_run_coverage": "Shows when the latest successful run finished and how many rows/zones it covered.",
    "zones_covered_card": "Number of unique zones represented in the filtered pricing results.",
    "avg_final_multiplier_card": "Average policy-adjusted multiplier after caps and rate limits.",
    "count_capped_card": "Count of rows where cap guardrails prevented excessive multiplier values.",
    "count_rate_limited_card": "Count of rows where change-rate guardrails smoothed abrupt movement.",
    "low_confidence_share_card": "Share of rows below the low-confidence threshold used for conservative interpretation.",
    "multiplier_distribution_chart": "Distribution of final multipliers in the filtered window; right-skew means more aggressive pricing.",
    "final_vs_raw_multiplier": "Raw multiplier is the unconstrained recommendation; final multiplier is what policy allowed to ship.",
    "cap_rate_limit_flags": "Cap and rate-limit flags explain whether policy safety checks overrode the raw multiplier.",
    "confidence_score": "Confidence summarizes forecast reliability; lower confidence supports more cautious pricing decisions.",
    "forecast_interval_band": "The shaded band is the expected forecast range between lower and upper bounds.",
    "uncertainty_band": "Uncertainty band is a bucketed label for forecast stability in this horizon.",
    "confidence_conservativeness": "Lower confidence typically increases conservative behavior through downstream guardrails.",
    "cap_protection": "Caps protect riders and marketplace stability from unusually high short-term price spikes.",
    "rate_limit_protection": "Rate limiting protects against rapid multiplier jumps between adjacent time buckets.",
    "reason_code_existence": "Reason codes provide auditable explanations for why each price recommendation was produced.",
    "cap_by_borough_chart": "Cap-applied rate by borough highlights where demand pressure most often hits cap policy.",
    "cap_by_hour_chart": "Cap-applied rate by hour shows intraday periods where cap protections activate most often.",
    "rate_by_borough_chart": "Rate-limited rate by borough reveals where smoothing is most frequently needed.",
    "rate_by_hour_chart": "Rate-limited rate by hour reveals when abrupt multiplier changes are most common.",
    "reason_code_summary_table": "Reason counts and descriptions summarize recurring pricing drivers in plain language.",
}
