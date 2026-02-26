# This file renders the pricing explorer tab for row-level decision transparency.
# It exists so users can inspect why prices moved and where guardrails intervened.
# The page combines tabular detail with trend and reason-code breakdown charts.
# It is designed for non-technical interpretation with narrative tooltip support.

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard_user.components.charts import (
    render_multiplier_trend,
    render_reason_code_breakdown,
)
from src.dashboard_user.components.tables import render_table


def render(*, pricing_df: pd.DataFrame, tooltips: dict[str, str]) -> None:
    st.header("Pricing Explorer")

    if pricing_df.empty:
        st.info("No pricing decisions match these filters.")
        return

    st.subheader("How to read this page")
    st.markdown(f"- **Final vs raw multiplier:** {tooltips['final_vs_raw_multiplier']}")
    st.markdown(f"- **Guardrail flags:** {tooltips['cap_rate_limit_flags']}")
    st.markdown(f"- **Confidence:** {tooltips['confidence_score']}")

    display_columns = [
        "zone_name",
        "borough",
        "bucket_start_ts",
        "final_multiplier",
        "confidence_score",
        "primary_reason_code",
        "cap_applied",
        "rate_limit_applied",
        "why_this_price",
    ]
    existing_columns = [column for column in display_columns if column in pricing_df.columns]

    table_df = pricing_df[existing_columns].copy()
    if "bucket_start_ts" in table_df.columns:
        table_df["bucket_start_ts"] = (
            pd.to_datetime(table_df["bucket_start_ts"], utc=True, errors="coerce")
            .dt.strftime("%Y-%m-%d %H:%M")
            .fillna("-")
        )
    if "final_multiplier" in table_df.columns:
        table_df["final_multiplier"] = table_df["final_multiplier"].map(lambda x: f"{x:.2f}x")
    if "confidence_score" in table_df.columns:
        table_df["confidence_score"] = table_df["confidence_score"].map(lambda x: f"{x:.2f}")

    render_table(
        table_df,
        title="Pricing Decisions",
        empty_message="No rows available for the pricing table.",
        help_text=tooltips["cap_rate_limit_flags"],
        height=360,
    )

    zone_options = (
        pricing_df[["zone_id", "zone_name"]]
        .drop_duplicates()
        .sort_values(["zone_name", "zone_id"], na_position="last")
    )
    zone_label_lookup = {
        f"{int(row.zone_id)} | {row.zone_name}": int(row.zone_id)
        for row in zone_options.itertuples()
    }
    selected_zone_label = st.selectbox(
        "Select zone for trend", options=list(zone_label_lookup.keys()), index=0
    )
    selected_zone_id = zone_label_lookup[selected_zone_label]

    zone_trend_df = pricing_df[pricing_df["zone_id"] == selected_zone_id].sort_values(
        "bucket_start_ts"
    )
    render_multiplier_trend(zone_trend_df, help_text=tooltips["final_vs_raw_multiplier"])

    reason_breakdown = (
        pricing_df.groupby("primary_reason_code", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(12)
    )
    render_reason_code_breakdown(reason_breakdown, help_text=tooltips["reason_code_existence"])
