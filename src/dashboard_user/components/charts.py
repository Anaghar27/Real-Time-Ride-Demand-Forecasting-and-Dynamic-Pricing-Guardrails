# This file contains reusable chart renderers for pricing, forecast, and guardrail views.
# It exists so chart logic is shared and consistently handles empty datasets.
# Centralizing plotting code makes tooltip coverage easier to manage and test.
# The charts use Altair because it integrates cleanly with Streamlit and supports layered visuals.

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st


def render_multiplier_histogram(dataframe: pd.DataFrame, *, help_text: str) -> None:
    st.subheader("Final Multiplier Distribution", help=help_text)
    if dataframe.empty:
        st.info("No pricing rows available for multiplier distribution.")
        return

    chart = (
        alt.Chart(dataframe)
        .mark_bar()
        .encode(
            x=alt.X("final_multiplier:Q", bin=alt.Bin(maxbins=25), title="Final multiplier"),
            y=alt.Y("count():Q", title="Count"),
            tooltip=[alt.Tooltip("count():Q", title="Rows")],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)


def render_multiplier_trend(dataframe: pd.DataFrame, *, help_text: str) -> None:
    st.subheader("Final Multiplier Over Time", help=help_text)
    if dataframe.empty:
        st.info("No rows available for the selected zone trend.")
        return

    chart = (
        alt.Chart(dataframe)
        .mark_line(point=True)
        .encode(
            x=alt.X("bucket_start_ts:T", title="Time (UTC)"),
            y=alt.Y("final_multiplier:Q", title="Final multiplier"),
            tooltip=[
                alt.Tooltip("bucket_start_ts:T", title="Bucket"),
                alt.Tooltip("final_multiplier:Q", format=".2f"),
                alt.Tooltip("raw_multiplier:Q", format=".2f"),
                alt.Tooltip("confidence_score:Q", format=".2f"),
            ],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)


def render_reason_code_breakdown(dataframe: pd.DataFrame, *, help_text: str) -> None:
    st.subheader("Top Reason Codes", help=help_text)
    if dataframe.empty:
        st.info("No reason-code rows available for the current filters.")
        return

    chart = (
        alt.Chart(dataframe)
        .mark_bar()
        .encode(
            x=alt.X("count:Q", title="Count"),
            y=alt.Y("primary_reason_code:N", sort="-x", title="Reason code"),
            tooltip=["primary_reason_code:N", "count:Q"],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)


def render_forecast_interval_chart(dataframe: pd.DataFrame, *, help_text: str) -> None:
    st.subheader("Forecast With Interval Band", help=help_text)
    if dataframe.empty:
        st.info("No forecast rows available for the selected zone and horizon.")
        return

    base = alt.Chart(dataframe).encode(x=alt.X("bucket_start_ts:T", title="Time (UTC)"))
    band = base.mark_area(opacity=0.25).encode(
        y=alt.Y("y_pred_lower:Q", title="Predicted demand"),
        y2="y_pred_upper:Q",
    )
    line = base.mark_line(color="#0e7490").encode(y=alt.Y("y_pred:Q", title="Predicted demand"))

    chart = (band + line).properties(height=300)
    st.altair_chart(chart, use_container_width=True)


def render_confidence_distribution(dataframe: pd.DataFrame, *, help_text: str) -> None:
    st.subheader("Confidence Distribution", help=help_text)
    if dataframe.empty:
        st.info("No confidence rows available in this selection.")
        return

    chart = (
        alt.Chart(dataframe)
        .mark_bar()
        .encode(
            x=alt.X("uncertainty_band:N", title="Uncertainty band"),
            y=alt.Y("row_count:Q", title="Rows"),
            color="uncertainty_band:N",
            tooltip=[
                "uncertainty_band:N",
                "row_count:Q",
                alt.Tooltip("avg_confidence:Q", format=".2f"),
            ],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)


def render_rate_bar(
    dataframe: pd.DataFrame,
    *,
    x_field: str,
    y_field: str,
    title: str,
    help_text: str,
) -> None:
    st.subheader(title, help=help_text)
    if dataframe.empty:
        st.info("No guardrail rows available in this selection.")
        return

    chart = (
        alt.Chart(dataframe)
        .mark_bar()
        .encode(
            x=alt.X(f"{x_field}:N", title=x_field.replace("_", " ").title()),
            y=alt.Y(f"{y_field}:Q", title="Rate"),
            tooltip=[alt.Tooltip(f"{x_field}:N"), alt.Tooltip(f"{y_field}:Q", format=".3f")],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)
