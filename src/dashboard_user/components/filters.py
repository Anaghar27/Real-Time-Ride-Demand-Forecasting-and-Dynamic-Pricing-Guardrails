# This file renders sidebar filters shared across all dashboard pages.
# It exists so every page reads from one consistent set of global controls.
# The component captures run mode, time window, geography, and confidence constraints in one place.
# Returning a typed filter object keeps downstream query logic predictable.

from __future__ import annotations

from datetime import UTC, datetime, time

import pandas as pd
import streamlit as st

from src.dashboard_user.dashboard_config import (
    DashboardConfig,
    DashboardFilters,
    default_time_window,
)


def render_sidebar_filters(
    *,
    config: DashboardConfig,
    zones: pd.DataFrame,
    recent_pricing_runs: pd.DataFrame,
    min_feature_ts: datetime | None = None,
    max_feature_ts: datetime | None = None,
) -> DashboardFilters:
    st.sidebar.header("Global Filters")

    default_start, default_end = default_time_window(config)
    min_feature_date = min_feature_ts.date() if min_feature_ts else None
    max_feature_date = max_feature_ts.date() if max_feature_ts else None
    max_feature_time = (
        max_feature_ts.astimezone(UTC).time().replace(second=0, microsecond=0)
        if max_feature_ts
        else None
    )
    if max_feature_ts:
        st.sidebar.caption(
            "Date constraint: data scraped from the website is currently available up to "
            f"{max_feature_ts.astimezone(UTC).strftime('%Y-%m-%d %H:%M')} UTC."
        )
        st.sidebar.caption(
            "The system will automatically pick up newer dates as soon as fresh source data is ingested."
        )

    default_end_date = default_end.date()
    if max_feature_date and default_end_date > max_feature_date:
        default_end_date = max_feature_date
    default_start_date = default_start.date()
    if default_start_date > default_end_date:
        default_start_date = default_end_date

    default_end_time = default_end.time().replace(second=0, microsecond=0)
    if max_feature_date and max_feature_time and default_end_date == max_feature_date:
        default_end_time = max_feature_time

    if "dashboard_start_date" not in st.session_state:
        st.session_state["dashboard_start_date"] = default_start_date
    if "dashboard_start_time" not in st.session_state:
        st.session_state["dashboard_start_time"] = default_start.time().replace(
            second=0, microsecond=0
        )
    if "dashboard_end_date" not in st.session_state:
        st.session_state["dashboard_end_date"] = default_end_date
    if "dashboard_end_time" not in st.session_state:
        st.session_state["dashboard_end_time"] = default_end_time

    if max_feature_date and st.session_state["dashboard_start_date"] > max_feature_date:
        st.session_state["dashboard_start_date"] = max_feature_date
    if max_feature_date and st.session_state["dashboard_end_date"] > max_feature_date:
        st.session_state["dashboard_end_date"] = max_feature_date
    if min_feature_date and st.session_state["dashboard_start_date"] < min_feature_date:
        st.session_state["dashboard_start_date"] = min_feature_date
    if min_feature_date and st.session_state["dashboard_end_date"] < min_feature_date:
        st.session_state["dashboard_end_date"] = min_feature_date

    date_bounds: dict[str, object] = {}
    if min_feature_date:
        date_bounds["min_value"] = min_feature_date
    if max_feature_date:
        date_bounds["max_value"] = max_feature_date

    start_date = st.sidebar.date_input(
        "Start date (UTC)", key="dashboard_start_date", **date_bounds
    )
    start_time = st.sidebar.time_input(
        "Start time (UTC)",
        key="dashboard_start_time",
        step=60,
    )

    end_date = st.sidebar.date_input("End date (UTC)", key="dashboard_end_date", **date_bounds)
    end_time = st.sidebar.time_input(
        "End time (UTC)",
        key="dashboard_end_time",
        step=60,
    )

    if (
        max_feature_date
        and max_feature_time
        and start_date == max_feature_date
        and start_time > max_feature_time
    ):
        start_time = max_feature_time
        st.session_state["dashboard_start_time"] = max_feature_time
    if (
        max_feature_date
        and max_feature_time
        and end_date == max_feature_date
        and end_time > max_feature_time
    ):
        end_time = max_feature_time
        st.session_state["dashboard_end_time"] = max_feature_time

    start_ts = datetime.combine(start_date, start_time or time(0, 0), tzinfo=UTC)
    end_ts = datetime.combine(end_date, end_time or time(23, 59), tzinfo=UTC)

    if start_ts > end_ts:
        st.sidebar.warning(
            "Start timestamp is after end timestamp. Using end timestamp for both bounds."
        )
        start_ts = end_ts

    boroughs = ["All"]
    if not zones.empty and "borough" in zones.columns:
        boroughs.extend(sorted({str(value) for value in zones["borough"].dropna().unique()}))
    borough_choice = st.sidebar.selectbox("Borough", options=boroughs, index=0)

    zone_lookup: dict[str, int | None] = {"All zones": None}
    if not zones.empty:
        for _, row in zones.iterrows():
            zone_id = int(row["zone_id"])
            zone_label = f"{zone_id} | {row['zone_name']} ({row['borough']})"
            zone_lookup[zone_label] = zone_id
    zone_label = st.sidebar.selectbox("Zone", options=list(zone_lookup.keys()), index=0)

    confidence_options = ["All", "low", "medium", "high"]
    confidence_choice = st.sidebar.selectbox("Confidence band", options=confidence_options, index=0)

    st.sidebar.markdown("---")
    st.sidebar.subheader("Guardrail toggles")
    cap_only = st.sidebar.checkbox("Show only cap_applied", value=False)
    rate_limit_only = st.sidebar.checkbox("Show only rate_limit_applied", value=False)
    low_confidence_only = st.sidebar.checkbox("Show only low confidence", value=False)

    st.sidebar.markdown("---")
    st.sidebar.subheader("Pricing run mode")
    run_mode_label = st.sidebar.radio(
        "Pricing run selection",
        options=["Latest pricing run", "Choose specific run_id"],
        index=0,
    )

    pricing_run_id: str | None = None
    pricing_run_mode = "latest"
    if run_mode_label == "Choose specific run_id":
        pricing_run_mode = "specific"
        run_ids = (
            recent_pricing_runs["run_id"].dropna().astype(str).tolist()
            if "run_id" in recent_pricing_runs.columns
            else []
        )
        if run_ids:
            pricing_run_id = st.sidebar.selectbox("pricing run_id", options=run_ids, index=0)
        else:
            st.sidebar.info("No run IDs found in pricing_run_log. Falling back to latest mode.")
            pricing_run_mode = "latest"

    page_size = int(
        st.sidebar.number_input(
            "Max rows per query",
            min_value=10,
            max_value=config.max_page_size,
            value=config.default_page_size,
            step=10,
            help="Limits query size to keep dashboard response times stable.",
        )
    )

    return DashboardFilters(
        start_ts=start_ts,
        end_ts=end_ts,
        borough=None if borough_choice == "All" else borough_choice,
        zone_id=zone_lookup.get(zone_label),
        uncertainty_band=None if confidence_choice == "All" else confidence_choice,
        cap_only=cap_only,
        rate_limit_only=rate_limit_only,
        low_confidence_only=low_confidence_only,
        pricing_run_mode=pricing_run_mode,
        pricing_run_id=pricing_run_id,
        page_size=page_size,
    )
