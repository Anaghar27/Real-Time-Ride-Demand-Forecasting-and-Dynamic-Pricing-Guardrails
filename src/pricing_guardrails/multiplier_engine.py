# This module converts forecast demand signals into raw pricing multipliers.
# It exists to isolate the signal-to-action logic from downstream guardrail logic.
# Two config-driven methods are supported so policy teams can choose ratio interpolation or threshold bands.
# The output keeps intermediate fields so dampening and baseline effects stay explainable.

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from src.pricing_guardrails.pricing_config import PricingConfig


def _compute_piecewise_multiplier(*, demand_ratio: pd.Series, breakpoints: list[dict[str, Any]]) -> pd.Series:
    if not breakpoints:
        raise ValueError("Piecewise multiplier method requires at least one breakpoint")

    sorted_points = sorted(
        ((float(point["ratio"]), float(point["multiplier"])) for point in breakpoints),
        key=lambda item: item[0],
    )
    ratios = np.array([item[0] for item in sorted_points], dtype=float)
    multipliers = np.array([item[1] for item in sorted_points], dtype=float)

    values = demand_ratio.astype(float).to_numpy()
    clipped_values = np.clip(values, ratios.min(), ratios.max())
    interpolated = np.interp(clipped_values, ratios, multipliers)
    return pd.Series(interpolated, index=demand_ratio.index, dtype=float)


def _compute_threshold_multiplier(*, frame: pd.DataFrame, config: dict[str, Any]) -> pd.Series:
    metric_name = str(config.get("metric", "demand_ratio"))
    if metric_name not in frame.columns:
        raise ValueError(f"Threshold method metric {metric_name!r} not found in dataframe columns")

    bands = list(config.get("bands", []))
    if not bands:
        raise ValueError("Threshold multiplier method requires non-empty bands")

    metric_values = frame[metric_name].astype(float)
    multiplier = pd.Series(np.nan, index=frame.index, dtype=float)

    for band in bands:
        minimum = float(band.get("min_inclusive", 0.0))
        max_exclusive = band.get("max_exclusive")
        band_multiplier = float(band["multiplier"])
        condition = metric_values >= minimum
        if max_exclusive is not None:
            condition = condition & (metric_values < float(max_exclusive))
        multiplier = multiplier.where(~condition, band_multiplier)

    if multiplier.isna().any():
        fallback_multiplier = float(bands[-1]["multiplier"])
        multiplier = multiplier.fillna(fallback_multiplier)
    return multiplier


def compute_raw_multiplier(
    *,
    forecasts_with_baseline: pd.DataFrame,
    pricing_config: PricingConfig,
    multiplier_rules: dict[str, Any],
) -> pd.DataFrame:
    frame = forecasts_with_baseline.copy()
    if frame.empty:
        return frame

    if "baseline_expected_demand" not in frame.columns:
        raise ValueError("baseline_expected_demand is required before computing raw multipliers")

    baseline = frame["baseline_expected_demand"].astype(float).clip(lower=pricing_config.baseline_min_value)
    frame["demand_ratio"] = frame["y_pred"].astype(float) / baseline

    active_method = str(multiplier_rules.get("active_method", "demand_ratio_piecewise"))
    methods = dict(multiplier_rules.get("methods", {}))
    if active_method not in methods:
        raise ValueError(f"active multiplier method {active_method!r} not found in methods")

    if active_method == "demand_ratio_piecewise":
        breakpoints = list(methods[active_method].get("breakpoints", []))
        frame["raw_multiplier"] = _compute_piecewise_multiplier(demand_ratio=frame["demand_ratio"], breakpoints=breakpoints)
    elif active_method == "threshold_bands":
        frame["raw_multiplier"] = _compute_threshold_multiplier(frame=frame, config=dict(methods[active_method]))
    else:
        raise ValueError(f"Unsupported multiplier method: {active_method}")

    frame["raw_multiplier_method"] = active_method

    floor = pricing_config.effective_floor_multiplier()
    frame["low_confidence_adjusted"] = False
    frame["pre_guardrail_multiplier"] = frame["raw_multiplier"].astype(float)

    if pricing_config.low_confidence_adjustment_enabled:
        uncertainty_bands = set(pricing_config.low_confidence_uncertainty_bands)
        low_confidence = frame["confidence_score"].astype(float) < pricing_config.low_confidence_threshold
        if uncertainty_bands:
            low_confidence = low_confidence | frame["uncertainty_band"].astype(str).isin(uncertainty_bands)

        pre_guardrail = frame["pre_guardrail_multiplier"].astype(float)
        dampened = floor + (pre_guardrail - floor) * pricing_config.low_confidence_dampening_factor
        frame.loc[low_confidence, "pre_guardrail_multiplier"] = np.maximum(dampened[low_confidence], floor)
        frame.loc[low_confidence, "low_confidence_adjusted"] = True

    frame["pre_guardrail_multiplier"] = frame["pre_guardrail_multiplier"].clip(lower=floor)
    frame["baseline_expected_demand"] = baseline
    return frame


def compute_demand_signal_label(
    *,
    demand_ratio: float,
    multiplier_rules: dict[str, Any],
) -> str:
    high_threshold = float(multiplier_rules.get("high_demand_ratio_threshold", 1.25))
    if demand_ratio >= high_threshold:
        return "high"
    return "normal"
