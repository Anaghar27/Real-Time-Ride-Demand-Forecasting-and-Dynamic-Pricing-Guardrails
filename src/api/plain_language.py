# This file translates technical forecast and pricing outputs into plain-language summaries.
# It exists so dashboards and non-technical consumers can quickly understand model decisions.
# The mappings are deterministic and tied directly to numeric thresholds and guardrail flags.
# Keeping this logic in one module prevents contradictory wording across endpoints.

from __future__ import annotations

from typing import Any


def price_action_label(final_multiplier: float) -> str:
    """Map multiplier to deterministic price action labels."""

    if final_multiplier < 1.0:
        return "Price decrease"
    if final_multiplier == 1.0:
        return "No price change"
    if 1.0 < final_multiplier <= 1.08:
        return "Small increase"
    if 1.08 < final_multiplier <= 1.2:
        return "Moderate increase"
    return "Larger increase"


def confidence_note(confidence_score: float, uncertainty_band: str | None) -> str:
    """Translate confidence values into a simple sentence."""

    band = (uncertainty_band or "unknown").lower()
    if confidence_score >= 0.8:
        return f"High confidence forecast with {band} uncertainty band."
    if confidence_score >= 0.6:
        return f"Medium confidence forecast with {band} uncertainty band."
    return f"Lower confidence forecast with {band} uncertainty band, treat changes cautiously."


def guardrail_note(
    *,
    cap_applied: bool,
    rate_limit_applied: bool,
    cap_reason: str | None,
    cap_type: str | None,
) -> str:
    """Describe guardrail effects in plain language."""

    if cap_applied and rate_limit_applied:
        reason = (cap_reason or cap_type or "policy").replace("_", " ")
        return f"Both cap and rate limiting were applied due to {reason}."
    if cap_applied:
        reason = (cap_reason or cap_type or "policy").replace("_", " ")
        return f"A pricing cap was applied due to {reason}."
    if rate_limit_applied:
        return "Rate limiting smoothed the multiplier change between time buckets."
    return "No cap or rate limit guardrail was applied."


def why_this_price(
    *,
    final_multiplier: float,
    reason_summary: str,
    cap_applied: bool,
    rate_limit_applied: bool,
) -> str:
    """Generate concise explanation sentence for pricing rows."""

    action = price_action_label(final_multiplier).lower()
    if cap_applied or rate_limit_applied:
        return f"{action.capitalize()} was recommended and then adjusted by guardrails. {reason_summary}"
    return f"{action.capitalize()} was recommended from forecasted demand signals. {reason_summary}"


def demand_outlook_label(y_pred: float) -> str:
    """Map forecast level into coarse demand categories."""

    if y_pred < 5:
        return "low"
    if y_pred < 15:
        return "normal"
    if y_pred < 30:
        return "elevated"
    return "high"


def forecast_range_summary(y_pred_lower: float, y_pred_upper: float) -> str:
    """Build a human-readable forecast range string."""

    return f"Expected demand range is {y_pred_lower:.2f} to {y_pred_upper:.2f}."


def pricing_plain_fields(row: dict[str, Any]) -> dict[str, str | None]:
    """Return plain-language fields for a pricing row."""

    final_multiplier = float(row.get("final_multiplier", 1.0))
    return {
        "recommended_price_action": price_action_label(final_multiplier),
        "why_this_price": why_this_price(
            final_multiplier=final_multiplier,
            reason_summary=str(row.get("reason_summary", "Pricing decision followed policy defaults.")),
            cap_applied=bool(row.get("cap_applied", False)),
            rate_limit_applied=bool(row.get("rate_limit_applied", False)),
        ),
        "guardrail_note": guardrail_note(
            cap_applied=bool(row.get("cap_applied", False)),
            rate_limit_applied=bool(row.get("rate_limit_applied", False)),
            cap_reason=(str(row.get("cap_reason")) if row.get("cap_reason") is not None else None),
            cap_type=(str(row.get("cap_type")) if row.get("cap_type") is not None else None),
        ),
        "confidence_note": confidence_note(
            confidence_score=float(row.get("confidence_score", 0.0)),
            uncertainty_band=(
                str(row.get("uncertainty_band")) if row.get("uncertainty_band") is not None else None
            ),
        ),
    }


def forecast_plain_fields(row: dict[str, Any]) -> dict[str, str]:
    """Return plain-language fields for a forecast row."""

    return {
        "demand_outlook_label": demand_outlook_label(float(row.get("y_pred", 0.0))),
        "confidence_note": confidence_note(
            confidence_score=float(row.get("confidence_score", 0.0)),
            uncertainty_band=(
                str(row.get("uncertainty_band")) if row.get("uncertainty_band") is not None else None
            ),
        ),
        "forecast_range_summary": forecast_range_summary(
            y_pred_lower=float(row.get("y_pred_lower", 0.0)),
            y_pred_upper=float(row.get("y_pred_upper", 0.0)),
        ),
    }
