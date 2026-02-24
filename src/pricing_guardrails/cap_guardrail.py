# This module applies floor and cap guardrails to raw pricing multipliers.
# It exists to enforce policy limits before any temporal rate limiting is considered.
# The cap order is deterministic so audits always reconstruct the same final candidate.
# Diagnostic columns are attached to each row to explain what clamp happened and why.

from __future__ import annotations

import numpy as np
import pandas as pd

from src.pricing_guardrails.pricing_config import PricingConfig


def _derive_time_category(frame: pd.DataFrame, *, run_timezone: str) -> pd.Series:
    if "time_category" in frame.columns:
        return frame["time_category"].astype(str)

    ts = pd.to_datetime(frame["bucket_start_ts"], utc=True).dt.tz_convert(run_timezone)
    peak_hours = set(range(7, 11)) | set(range(16, 21))
    return pd.Series(np.where(ts.dt.hour.isin(peak_hours), "peak", "off_peak"), index=frame.index, dtype="object")


def apply_cap_guardrail(
    *,
    raw_frame: pd.DataFrame,
    pricing_config: PricingConfig,
) -> pd.DataFrame:
    frame = raw_frame.copy()
    if frame.empty:
        return frame

    floor_value = pricing_config.effective_floor_multiplier()
    pre = frame["pre_guardrail_multiplier"].astype(float)
    post = pre.copy()

    frame["pre_cap_multiplier"] = pre
    frame["cap_applied"] = False
    frame["cap_reason"] = None
    frame["cap_type"] = None
    frame["cap_value"] = np.nan

    floor_mask = post < floor_value
    post = np.maximum(post, floor_value)
    frame.loc[floor_mask, "cap_applied"] = True
    frame.loc[floor_mask, "cap_type"] = "floor"
    frame.loc[floor_mask, "cap_reason"] = "floor_policy"
    frame.loc[floor_mask, "cap_value"] = floor_value

    time_category = _derive_time_category(frame, run_timezone=pricing_config.run_timezone)
    frame["time_category"] = time_category

    contextual_cap = pd.Series(np.inf, index=frame.index, dtype=float)
    contextual_reason = pd.Series("", index=frame.index, dtype="object")

    if pricing_config.cap_by_confidence_band:
        conf_caps = frame["uncertainty_band"].astype(str).map(pricing_config.cap_by_confidence_band)
        has_conf = conf_caps.notna()
        lower_conf = conf_caps.fillna(np.inf).astype(float)
        update_mask = lower_conf < contextual_cap
        contextual_cap = contextual_cap.where(~update_mask, lower_conf)
        contextual_reason = contextual_reason.where(~update_mask, "confidence")
        contextual_cap = contextual_cap.where(has_conf | (contextual_reason != "confidence"), contextual_cap)

    if pricing_config.cap_by_zone_class:
        zone_caps = frame.get("zone_class", pd.Series("", index=frame.index)).astype(str).map(
            pricing_config.cap_by_zone_class
        )
        has_zone_cap = zone_caps.notna()
        lower_zone = zone_caps.fillna(np.inf).astype(float)
        update_mask = lower_zone < contextual_cap
        contextual_cap = contextual_cap.where(~update_mask, lower_zone)
        contextual_reason = contextual_reason.where(~update_mask, "sparse_zone")
        contextual_cap = contextual_cap.where(has_zone_cap | (contextual_reason != "sparse_zone"), contextual_cap)

    if pricing_config.cap_by_time_category:
        time_caps = time_category.map(pricing_config.cap_by_time_category)
        has_time_cap = time_caps.notna()
        lower_time = time_caps.fillna(np.inf).astype(float)
        update_mask = lower_time < contextual_cap
        contextual_cap = contextual_cap.where(~update_mask, lower_time)
        contextual_reason = contextual_reason.where(~update_mask, "time_category")
        contextual_cap = contextual_cap.where(has_time_cap | (contextual_reason != "time_category"), contextual_cap)

    contextual_mask = np.isfinite(contextual_cap) & (post > contextual_cap)
    post = post.where(~contextual_mask, contextual_cap)
    frame.loc[contextual_mask, "cap_applied"] = True
    frame.loc[contextual_mask, "cap_type"] = "contextual"
    frame.loc[contextual_mask, "cap_reason"] = contextual_reason[contextual_mask]
    frame.loc[contextual_mask, "cap_value"] = contextual_cap[contextual_mask]

    global_mask = post > pricing_config.global_cap_multiplier
    post = np.minimum(post, pricing_config.global_cap_multiplier)
    frame.loc[global_mask, "cap_applied"] = True
    frame.loc[global_mask, "cap_type"] = "global"
    frame.loc[global_mask, "cap_reason"] = "global_cap"
    frame.loc[global_mask, "cap_value"] = pricing_config.global_cap_multiplier

    frame["post_cap_multiplier"] = post.astype(float)
    frame["cap_applied"] = frame["cap_applied"].astype(bool)
    return frame
