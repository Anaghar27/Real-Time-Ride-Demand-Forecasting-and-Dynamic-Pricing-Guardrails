# This file renders the guardrail transparency tab focused on policy interventions.
# It exists so users can understand where caps and rate limits were protecting against unstable prices.
# The page summarizes guardrail usage by geography and hour to support operational decisions.
# Reason-code tables provide an auditable explanation layer for pricing outcomes.

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard_user.components.charts import render_rate_bar
from src.dashboard_user.components.tables import render_table


def render(
    *,
    pricing_df: pd.DataFrame,
    reason_catalog_df: pd.DataFrame,
    tooltips: dict[str, str],
) -> None:
    st.header("Guardrail Transparency")

    if pricing_df.empty:
        st.info("No pricing rows available to compute guardrail transparency metrics.")
        return

    st.subheader("Why guardrails exist")
    st.markdown(f"- **Cap protection:** {tooltips['cap_protection']}")
    st.markdown(f"- **Rate-limit protection:** {tooltips['rate_limit_protection']}")
    st.markdown(f"- **Reason codes:** {tooltips['reason_code_existence']}")

    enriched = pricing_df.copy()
    enriched["hour_of_day"] = pd.to_datetime(
        enriched["bucket_start_ts"], utc=True, errors="coerce"
    ).dt.hour

    cap_by_borough = (
        enriched.groupby("borough", dropna=False)
        .agg(cap_applied_rate=("cap_applied", "mean"))
        .reset_index()
        .sort_values("cap_applied_rate", ascending=False)
    )
    cap_by_hour = (
        enriched.groupby("hour_of_day", dropna=False)
        .agg(cap_applied_rate=("cap_applied", "mean"))
        .reset_index()
        .sort_values("hour_of_day", ascending=True)
    )

    rate_by_borough = (
        enriched.groupby("borough", dropna=False)
        .agg(rate_limited_rate=("rate_limit_applied", "mean"))
        .reset_index()
        .sort_values("rate_limited_rate", ascending=False)
    )
    rate_by_hour = (
        enriched.groupby("hour_of_day", dropna=False)
        .agg(rate_limited_rate=("rate_limit_applied", "mean"))
        .reset_index()
        .sort_values("hour_of_day", ascending=True)
    )

    left_col, right_col = st.columns(2)
    with left_col:
        render_rate_bar(
            cap_by_borough,
            x_field="borough",
            y_field="cap_applied_rate",
            title="Cap Applied Rate by Borough",
            help_text=tooltips["cap_by_borough_chart"],
        )
        render_rate_bar(
            rate_by_borough,
            x_field="borough",
            y_field="rate_limited_rate",
            title="Rate-Limited Rate by Borough",
            help_text=tooltips["rate_by_borough_chart"],
        )
    with right_col:
        render_rate_bar(
            cap_by_hour,
            x_field="hour_of_day",
            y_field="cap_applied_rate",
            title="Cap Applied Rate by Hour",
            help_text=tooltips["cap_by_hour_chart"],
        )
        render_rate_bar(
            rate_by_hour,
            x_field="hour_of_day",
            y_field="rate_limited_rate",
            title="Rate-Limited Rate by Hour",
            help_text=tooltips["rate_by_hour_chart"],
        )

    reason_counts = (
        enriched.groupby("primary_reason_code", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    if not reason_catalog_df.empty:
        reason_counts = reason_counts.merge(
            reason_catalog_df[["reason_code", "description"]],
            left_on="primary_reason_code",
            right_on="reason_code",
            how="left",
        )
        reason_counts = reason_counts.drop(columns=["reason_code"])

    render_table(
        reason_counts,
        title="Reason Code Summary",
        empty_message="No reason code rows available.",
        help_text=tooltips["reason_code_summary_table"],
        height=320,
    )
