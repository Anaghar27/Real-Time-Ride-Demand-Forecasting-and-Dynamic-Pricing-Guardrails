# This file renders the forecast explorer tab for interval and confidence interpretation.
# It exists so users can see predicted demand levels and uncertainty over the selected horizon.
# The page makes confidence information explicit to explain conservative pricing behavior.
# It supports focused exploration by zone and horizon bucket.

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard_user.components.charts import (
    render_confidence_distribution,
    render_forecast_interval_chart,
)


def render(*, forecast_df: pd.DataFrame, tooltips: dict[str, str]) -> None:
    st.header("Forecast Explorer")

    if forecast_df.empty:
        st.info("No forecast rows match these filters.")
        return

    st.subheader("How to read this page")
    st.markdown(f"- **Interval meaning:** {tooltips['forecast_interval_band']}")
    st.markdown(f"- **Uncertainty band:** {tooltips['uncertainty_band']}")
    st.markdown(f"- **Pricing conservativeness:** {tooltips['confidence_conservativeness']}")

    zone_options = (
        forecast_df[["zone_id", "zone_name"]]
        .drop_duplicates()
        .sort_values(["zone_name", "zone_id"], na_position="last")
    )
    zone_lookup = {
        f"{int(row.zone_id)} | {row.zone_name}": int(row.zone_id)
        for row in zone_options.itertuples()
    }
    selected_zone_label = st.selectbox(
        "Select forecast zone", options=list(zone_lookup.keys()), index=0
    )
    selected_zone_id = zone_lookup[selected_zone_label]

    horizon_options = sorted(
        [int(value) for value in forecast_df["horizon_index"].dropna().unique().tolist()]
    )
    selected_horizon = st.selectbox("Select horizon index", options=horizon_options, index=0)

    selected_forecast = forecast_df[
        (forecast_df["zone_id"] == selected_zone_id)
        & (forecast_df["horizon_index"].astype(float) == float(selected_horizon))
    ].sort_values("bucket_start_ts")

    render_forecast_interval_chart(selected_forecast, help_text=tooltips["forecast_interval_band"])

    confidence_distribution = (
        forecast_df.groupby("uncertainty_band", dropna=False)
        .agg(
            row_count=("zone_id", "size"),
            avg_confidence=("confidence_score", "mean"),
        )
        .reset_index()
        .sort_values("row_count", ascending=False)
    )
    render_confidence_distribution(confidence_distribution, help_text=tooltips["uncertainty_band"])
