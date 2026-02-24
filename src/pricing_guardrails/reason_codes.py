# This module assigns machine-readable reason codes to every pricing decision row.
# It exists so downstream consumers can understand why a multiplier changed without reverse engineering the pipeline.
# Codes are config-driven and grouped by signal, guardrail, and fallback categories.
# A deterministic primary reason is also selected for dashboards and operational triage.

from __future__ import annotations

from typing import Any

import pandas as pd


def _append_code(codes: list[str], candidate: str, valid_codes: set[str]) -> None:
    if candidate in valid_codes and candidate not in codes:
        codes.append(candidate)


def _primary_reason(codes: list[str], priority_order: list[str]) -> str:
    for code in priority_order:
        if code in codes:
            return code
    if codes:
        return codes[0]
    return "NORMAL_DEMAND_BASELINE"


def _reason_summary(codes: list[str], catalog: dict[str, dict[str, Any]]) -> str:
    snippets: list[str] = []
    for code in codes[:3]:
        description = str(catalog.get(code, {}).get("description", "")).strip()
        if description:
            snippets.append(description)
    return " | ".join(snippets) if snippets else "Pricing decision generated with default policy path."


def apply_reason_codes(
    *,
    priced_frame: pd.DataFrame,
    reason_code_config: dict[str, Any],
    high_demand_ratio_threshold: float,
) -> pd.DataFrame:
    frame = priced_frame.copy()
    if frame.empty:
        frame["reason_codes_json"] = []
        frame["reason_summary"] = ""
        frame["primary_reason_code"] = ""
        return frame

    catalog = {
        str(code): dict(payload) if isinstance(payload, dict) else {}
        for code, payload in dict(reason_code_config.get("codes", {})).items()
    }
    priority_order = [str(item) for item in list(reason_code_config.get("priority_order", []))]
    valid_codes = set(catalog.keys())

    code_rows: list[list[str]] = []
    summaries: list[str] = []
    primaries: list[str] = []

    for _, row in frame.iterrows():
        codes: list[str] = []

        demand_ratio = float(row.get("demand_ratio", 1.0))
        if demand_ratio >= high_demand_ratio_threshold:
            _append_code(codes, "HIGH_DEMAND_RATIO", valid_codes)
        else:
            _append_code(codes, "NORMAL_DEMAND_BASELINE", valid_codes)

        baseline_level = str(row.get("baseline_reference_level", "zone"))
        if baseline_level == "zone":
            _append_code(codes, "BASELINE_FALLBACK_ZONE", valid_codes)
        elif baseline_level == "borough":
            _append_code(codes, "BASELINE_FALLBACK_BOROUGH", valid_codes)
        elif baseline_level == "city":
            _append_code(codes, "BASELINE_FALLBACK_CITY", valid_codes)
        elif baseline_level == "global":
            _append_code(codes, "MISSING_BASELINE_REFERENCE_FALLBACK", valid_codes)

        if bool(row.get("low_confidence_adjusted", False)):
            _append_code(codes, "LOW_CONFIDENCE_DAMPENING", valid_codes)

        if bool(row.get("cap_applied", False)):
            cap_type = str(row.get("cap_type") or "")
            cap_reason = str(row.get("cap_reason") or "")
            if cap_type == "floor":
                _append_code(codes, "FLOOR_APPLIED", valid_codes)
            if cap_type == "global":
                _append_code(codes, "CAP_APPLIED_GLOBAL", valid_codes)
            if cap_type == "contextual" and cap_reason == "confidence":
                _append_code(codes, "CAP_APPLIED_CONFIDENCE", valid_codes)
            if cap_type == "contextual" and cap_reason == "sparse_zone":
                _append_code(codes, "CAP_APPLIED_SPARSE_ZONE", valid_codes)

        if bool(row.get("rate_limit_applied", False)):
            direction = str(row.get("rate_limit_direction", "none"))
            if direction == "up":
                _append_code(codes, "RATE_LIMIT_INCREASE_CLAMP", valid_codes)
            elif direction == "down":
                _append_code(codes, "RATE_LIMIT_DECREASE_CLAMP", valid_codes)

        if bool(row.get("smoothing_applied", False)):
            _append_code(codes, "SMOOTHING_APPLIED", valid_codes)

        if bool(row.get("cold_start_used", False)):
            _append_code(codes, "NO_PREVIOUS_MULTIPLIER_COLD_START", valid_codes)

        zone_class = str(row.get("zone_class", ""))
        if zone_class in {"sparse", "ultra_sparse"}:
            _append_code(codes, "SPARSE_ZONE_POLICY_ACTIVE", valid_codes)

        if not codes:
            fallback = "NORMAL_DEMAND_BASELINE"
            if fallback in valid_codes:
                codes = [fallback]
            else:
                codes = sorted(valid_codes)[:1]

        primary = _primary_reason(codes, priority_order)
        summary = _reason_summary(codes, catalog)

        code_rows.append(codes)
        primaries.append(primary)
        summaries.append(summary)

    frame["reason_codes_json"] = code_rows
    frame["primary_reason_code"] = primaries
    frame["reason_summary"] = summaries
    return frame
