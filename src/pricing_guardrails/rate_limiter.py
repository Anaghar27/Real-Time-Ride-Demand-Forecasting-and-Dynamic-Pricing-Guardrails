# This module rate-limits pricing movement between consecutive buckets for each zone.
# It exists to avoid abrupt user-facing shocks even when demand signals move quickly.
# Previous values are sourced from published pricing outputs so the limiter uses operational truth.
# Optional smoothing is supported, with a final clamp to keep policy bounds intact.

from __future__ import annotations

import re
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.pricing_guardrails.pricing_config import PricingConfig

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _safe_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return identifier


def load_previous_final_multipliers(
    *,
    engine: Engine,
    pricing_output_table_name: str,
    zone_ids: list[int],
    before_bucket_ts: pd.Timestamp,
) -> dict[int, float]:
    if not zone_ids:
        return {}

    table = _safe_identifier(pricing_output_table_name)
    query = text(
        f"""
        SELECT DISTINCT ON (zone_id)
            zone_id,
            final_multiplier
        FROM {table}
        WHERE zone_id = ANY(CAST(:zone_ids AS INTEGER[]))
          AND bucket_start_ts < :before_bucket_ts
        ORDER BY zone_id, bucket_start_ts DESC, pricing_created_at DESC
        """
    )
    frame = pd.read_sql_query(
        query,
        con=engine,
        params={"zone_ids": zone_ids, "before_bucket_ts": before_bucket_ts.to_pydatetime()},
    )
    if frame.empty:
        return {}
    frame["zone_id"] = frame["zone_id"].astype(int)
    frame["final_multiplier"] = frame["final_multiplier"].astype(float)
    return dict(zip(frame["zone_id"], frame["final_multiplier"], strict=False))


def apply_rate_limiter(
    *,
    capped_frame: pd.DataFrame,
    pricing_config: PricingConfig,
    previous_multiplier_map: dict[int, float],
) -> pd.DataFrame:
    frame = capped_frame.copy()
    if frame.empty:
        return frame

    frame["bucket_start_ts"] = pd.to_datetime(frame["bucket_start_ts"], utc=True)
    frame = frame.sort_values(["zone_id", "bucket_start_ts"]).copy()

    floor = pricing_config.effective_floor_multiplier()
    max_cap = pricing_config.global_cap_multiplier

    diagnostics: list[dict[str, Any]] = []
    for zone_id, group in frame.groupby("zone_id", sort=False):
        prev_multiplier = previous_multiplier_map.get(int(zone_id), pricing_config.cold_start_multiplier)
        cold_start = int(zone_id) not in previous_multiplier_map

        for idx in group.index:
            candidate = float(frame.at[idx, "post_cap_multiplier"])
            rate_limited = candidate
            direction = "none"
            rate_limit_applied = False

            delta = candidate - prev_multiplier
            if delta > pricing_config.max_increase_per_bucket:
                rate_limited = prev_multiplier + pricing_config.max_increase_per_bucket
                direction = "up"
                rate_limit_applied = True
            elif delta < -pricing_config.max_decrease_per_bucket:
                rate_limited = prev_multiplier - pricing_config.max_decrease_per_bucket
                direction = "down"
                rate_limit_applied = True

            smoothed = rate_limited
            smoothing_applied = False
            smoothing_reclamped = False
            if pricing_config.smoothing_enabled:
                smoothing_applied = True
                alpha = pricing_config.smoothing_alpha
                smoothed = alpha * rate_limited + (1.0 - alpha) * prev_multiplier

            final_multiplier = smoothed
            # Keep the post-smoothing value within global policy bounds.
            # Contextual caps are applied before this step and may be relaxed here to honor
            # configured max-decrease constraints for rider experience stability.
            final_upper_bound = max_cap
            if final_multiplier < floor:
                final_multiplier = floor
                smoothing_reclamped = smoothing_applied
            if final_multiplier > final_upper_bound:
                final_multiplier = final_upper_bound
                smoothing_reclamped = smoothing_applied

            diagnostics.append(
                {
                    "index": idx,
                    "previous_final_multiplier": float(prev_multiplier),
                    "candidate_multiplier_before_rate_limit": float(candidate),
                    "rate_limit_applied": bool(rate_limit_applied),
                    "rate_limit_direction": direction,
                    "max_up_delta": float(pricing_config.max_increase_per_bucket),
                    "max_down_delta": float(pricing_config.max_decrease_per_bucket),
                    "post_rate_limit_multiplier": float(rate_limited),
                    "smoothing_applied": bool(smoothing_applied),
                    "smoothing_reclamped": bool(smoothing_reclamped),
                    "final_multiplier": float(final_multiplier),
                    "cold_start_used": bool(cold_start),
                }
            )
            prev_multiplier = final_multiplier
            cold_start = False

    diag_frame = pd.DataFrame(diagnostics).set_index("index")
    frame = frame.join(diag_frame, how="left")
    frame["rate_limit_applied"] = frame["rate_limit_applied"].fillna(False).astype(bool)
    frame["smoothing_applied"] = frame["smoothing_applied"].fillna(False).astype(bool)
    frame["smoothing_reclamped"] = frame["smoothing_reclamped"].fillna(False).astype(bool)
    frame["cold_start_used"] = frame["cold_start_used"].fillna(False).astype(bool)
    return frame.sort_index()
