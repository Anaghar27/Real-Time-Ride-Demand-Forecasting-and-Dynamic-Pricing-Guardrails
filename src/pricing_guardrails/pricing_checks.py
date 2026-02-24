# This module implements hard quality checks for pricing decision outputs.
# It exists to block invalid pricing writes before they can affect downstream systems.
# The checks enforce key uniqueness, policy bounds, rate-limit integrity, and reason-code completeness.
# Results are structured for run logs and diagnostics so operators can quickly find failures.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.pricing_guardrails.pricing_config import PricingConfig


class PricingCheckError(RuntimeError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = details or {}


@dataclass(frozen=True)
class PricingCheckSummary:
    passed: bool
    failures: list[dict[str, Any]]
    warnings: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {"passed": self.passed, "failures": self.failures, "warnings": self.warnings}


def run_pricing_checks(
    *,
    pricing_frame: pd.DataFrame,
    expected_zones: int,
    expected_buckets: int,
    pricing_config: PricingConfig,
) -> PricingCheckSummary:
    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    expected_rows = int(expected_zones * expected_buckets)
    actual_rows = int(len(pricing_frame))
    tolerance = int(round(expected_rows * pricing_config.row_count_tolerance_pct))
    if abs(actual_rows - expected_rows) > tolerance:
        failures.append(
            {
                "check": "row_count",
                "expected_rows": expected_rows,
                "actual_rows": actual_rows,
                "tolerance_rows": tolerance,
            }
        )

    duplicate_count = int(
        pricing_frame.duplicated(subset=["pricing_run_key", "zone_id", "bucket_start_ts"]).sum()
    )
    if duplicate_count > 0:
        failures.append({"check": "duplicate_keys", "duplicate_rows": duplicate_count})

    final = pricing_frame["final_multiplier"].astype(float)
    if final.isna().any():
        failures.append({"check": "final_multiplier_null", "null_rows": int(final.isna().sum())})
    if (final < 0).any():
        failures.append({"check": "final_multiplier_nonnegative", "invalid_rows": int((final < 0).sum())})

    floor = pricing_config.effective_floor_multiplier()
    if (final < floor).any():
        failures.append({"check": "final_multiplier_floor", "invalid_rows": int((final < floor).sum())})

    cap = pricing_config.global_cap_multiplier
    if (final > cap).any():
        failures.append({"check": "final_multiplier_cap", "invalid_rows": int((final > cap).sum())})

    previous = pricing_frame["previous_final_multiplier"].astype(float)
    has_previous = previous.notna()
    delta = final - previous
    max_up_violation = has_previous & (delta > pricing_config.max_increase_per_bucket + 1e-9)
    max_down_violation = has_previous & (delta < -(pricing_config.max_decrease_per_bucket + 1e-9))
    if max_up_violation.any() or max_down_violation.any():
        failures.append(
            {
                "check": "rate_limit_delta_bounds",
                "up_violations": int(max_up_violation.sum()),
                "down_violations": int(max_down_violation.sum()),
            }
        )

    cap_rows = pricing_frame["cap_applied"].fillna(False).astype(bool)
    if cap_rows.any():
        missing_diag = cap_rows & (
            pricing_frame["cap_type"].isna() | pricing_frame["cap_value"].isna() | pricing_frame["cap_reason"].isna()
        )
        if missing_diag.any():
            failures.append({"check": "cap_diagnostics", "invalid_rows": int(missing_diag.sum())})

    rate_rows = pricing_frame["rate_limit_applied"].fillna(False).astype(bool)
    if rate_rows.any():
        invalid_rate_diag = rate_rows & (
            pricing_frame["rate_limit_direction"].astype(str).isin(["none", "", "nan"])
            | pricing_frame["post_rate_limit_multiplier"].isna()
        )
        if invalid_rate_diag.any():
            failures.append({"check": "rate_limit_diagnostics", "invalid_rows": int(invalid_rate_diag.sum())})

    confidence = pricing_frame["confidence_score"].astype(float)
    if confidence.isna().any() or ((confidence < 0) | (confidence > 1)).any():
        failures.append(
            {
                "check": "confidence_fields",
                "null_rows": int(confidence.isna().sum()),
                "out_of_range_rows": int(((confidence < 0) | (confidence > 1)).sum()),
            }
        )

    invalid_reason_json = pricing_frame["reason_codes_json"].apply(lambda value: not isinstance(value, list))
    if invalid_reason_json.any():
        failures.append({"check": "reason_codes_json_type", "invalid_rows": int(invalid_reason_json.sum())})

    empty_primary = pricing_frame["primary_reason_code"].isna() | (pricing_frame["primary_reason_code"].astype(str) == "")
    if empty_primary.any():
        failures.append({"check": "primary_reason_code_presence", "invalid_rows": int(empty_primary.sum())})

    coverage = 0.0
    if expected_zones > 0:
        coverage = float(pricing_frame["zone_id"].nunique() / expected_zones)
    if coverage < pricing_config.coverage_threshold_pct:
        failures.append(
            {
                "check": "zone_coverage",
                "expected_zones": expected_zones,
                "actual_zones": int(pricing_frame["zone_id"].nunique()),
                "coverage": coverage,
                "required": pricing_config.coverage_threshold_pct,
            }
        )

    if pricing_frame.empty:
        warnings.append({"check": "empty_pricing_frame", "message": "No forecast rows selected for pricing window."})

    return PricingCheckSummary(passed=len(failures) == 0, failures=failures, warnings=warnings)


def enforce_pricing_checks(summary: PricingCheckSummary, *, strict_checks: bool) -> None:
    if summary.passed:
        return
    if strict_checks:
        raise PricingCheckError("Pricing checks failed; pricing decisions were not written.", details=summary.to_dict())
