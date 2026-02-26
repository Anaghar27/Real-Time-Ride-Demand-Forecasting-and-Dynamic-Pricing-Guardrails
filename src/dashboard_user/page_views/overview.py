# This file renders the high-level overview tab for decision-support users.
# It exists so operators can quickly assess coverage, pricing intensity, and guardrail activity.
# The page prioritizes simple KPIs, ranked zones, and multiplier distribution context.
# It also handles empty windows without breaking downstream interactions.

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from src.dashboard_user.components.charts import render_multiplier_histogram
from src.dashboard_user.components.summary_cards import render_overview_cards
from src.dashboard_user.components.tables import render_table
from src.dashboard_user.formatting import format_count, format_multiplier, format_percent


def render(
    *,
    pricing_df: pd.DataFrame,
    latest_run_metadata: dict[str, Any],
    low_confidence_threshold: float,
    tooltips: dict[str, str],
) -> None:
    st.header("Overview")

    pricing_run = latest_run_metadata.get("pricing_run") if latest_run_metadata else None
    st.subheader("Latest Run and Coverage", help=tooltips["latest_run_coverage"])
    if pricing_run:
        run_id = pricing_run.get("run_id", "unknown")
        ended_at = pricing_run.get("ended_at") or pricing_run.get("started_at")
        row_count = pricing_run.get("row_count")
        zone_count = pricing_run.get("zone_count")
        st.caption(
            f"Latest pricing run: `{run_id}` | timestamp: `{ended_at}` | rows: {row_count} | zones: {zone_count}"
        )
    else:
        st.caption("No latest pricing run metadata found.")

    if pricing_df.empty:
        st.info("No pricing decisions available for the current filters.")
        return

    low_confidence_share = (
        float((pricing_df["confidence_score"] < low_confidence_threshold).mean())
        if "confidence_score" in pricing_df.columns
        else 0.0
    )

    render_overview_cards(
        zones_covered=format_count(pricing_df["zone_id"].nunique()),
        avg_final_multiplier=format_multiplier(pricing_df["final_multiplier"].mean()),
        count_capped=format_count(pricing_df["cap_applied"].sum()),
        count_rate_limited=format_count(pricing_df["rate_limit_applied"].sum()),
        low_confidence_share=format_percent(low_confidence_share),
        tooltips=tooltips,
    )

    top_zones = (
        pricing_df.groupby(["zone_id", "zone_name", "borough"], dropna=False)
        .agg(
            avg_final_multiplier=("final_multiplier", "mean"),
            rows=("zone_id", "size"),
            capped_rows=("cap_applied", "sum"),
            rate_limited_rows=("rate_limit_applied", "sum"),
        )
        .sort_values("avg_final_multiplier", ascending=False)
        .head(20)
        .reset_index()
    )
    top_zones["avg_final_multiplier"] = top_zones["avg_final_multiplier"].map(lambda x: f"{x:.2f}x")

    render_table(
        top_zones,
        title="Top Zones By Final Multiplier",
        empty_message="No ranked zones available for this window.",
    )

    render_multiplier_histogram(pricing_df, help_text=tooltips["multiplier_distribution_chart"])
