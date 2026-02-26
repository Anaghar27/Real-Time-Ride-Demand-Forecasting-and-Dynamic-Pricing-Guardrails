# This file is the Streamlit entrypoint for the user-facing decision-support dashboard.
# It exists to combine global filters, API-first data retrieval, and page-level storytelling in one app.
# The app keeps non-technical interpretation front and center with plain-language fields and tooltips.
# It also handles empty and partial data coverage without failing the user workflow.

from __future__ import annotations

import streamlit as st

from src.dashboard_user.components.filters import render_sidebar_filters
from src.dashboard_user.dashboard_config import DashboardFilters, load_dashboard_config
from src.dashboard_user.data_access import DashboardDataAccess
from src.dashboard_user.page_views import (
    forecast_explorer,
    guardrail_transparency,
    overview,
    pricing_explorer,
)
from src.dashboard_user.tooltips import TOOLTIPS
from src.dashboard_user.ui_text import APP_SUBTITLE, APP_TITLE, PARTIAL_COVERAGE


@st.cache_resource
def get_data_access() -> DashboardDataAccess:
    config = load_dashboard_config()
    return DashboardDataAccess(config=config)


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")

    config = load_dashboard_config()
    data_access = get_data_access()

    @st.cache_data(ttl=config.metadata_cache_ttl_seconds)
    def load_zones() -> tuple[object, str]:
        return data_access.get_zone_catalog()

    @st.cache_data(ttl=config.metadata_cache_ttl_seconds)
    def load_reason_codes() -> tuple[object, str]:
        return data_access.get_reason_code_catalog()

    @st.cache_data(ttl=config.metadata_cache_ttl_seconds)
    def load_recent_pricing_runs() -> object:
        return data_access.get_recent_pricing_runs(max_items=config.max_run_selector_options)

    @st.cache_data(ttl=config.metadata_cache_ttl_seconds)
    def load_latest_run_metadata() -> dict[str, object]:
        return data_access.get_latest_run_metadata()

    @st.cache_data(ttl=config.metadata_cache_ttl_seconds)
    def load_feature_time_bounds() -> tuple[object, object, str]:
        return data_access.get_feature_time_bounds()

    @st.cache_data(ttl=config.query_cache_ttl_seconds)
    def load_pricing(filters: DashboardFilters) -> tuple[object, str]:
        return data_access.get_pricing_data(filters)

    @st.cache_data(ttl=config.query_cache_ttl_seconds)
    def load_forecast(filters: DashboardFilters) -> tuple[object, str]:
        return data_access.get_forecast_data(filters)

    zones_df, zones_source = load_zones()
    reason_codes_df, reason_source = load_reason_codes()
    recent_runs_df = load_recent_pricing_runs()
    latest_run_metadata = load_latest_run_metadata()
    min_feature_ts, max_feature_ts, feature_bounds_source = load_feature_time_bounds()

    st.title(APP_TITLE)
    st.caption(APP_SUBTITLE)

    filters = render_sidebar_filters(
        config=config,
        zones=zones_df,
        recent_pricing_runs=recent_runs_df,
        min_feature_ts=min_feature_ts,
        max_feature_ts=max_feature_ts,
    )

    pricing_df, pricing_source = load_pricing(filters)
    forecast_df, forecast_source = load_forecast(filters)

    st.sidebar.markdown("---")
    st.sidebar.caption(f"Zone catalog source: {zones_source}")
    st.sidebar.caption(f"Reason catalog source: {reason_source}")
    st.sidebar.caption(f"Feature bounds source: {feature_bounds_source}")
    st.sidebar.caption(f"Pricing rows source: {pricing_source}")
    st.sidebar.caption(f"Forecast rows source: {forecast_source}")

    if (pricing_df.empty and not forecast_df.empty) or (forecast_df.empty and not pricing_df.empty):
        st.warning(PARTIAL_COVERAGE)

    tabs = st.tabs(
        [
            "Overview",
            "Pricing Explorer",
            "Forecast Explorer",
            "Guardrail Transparency",
        ]
    )

    with tabs[0]:
        overview.render(
            pricing_df=pricing_df,
            latest_run_metadata=latest_run_metadata,
            low_confidence_threshold=config.low_confidence_threshold,
            tooltips=TOOLTIPS,
        )

    with tabs[1]:
        pricing_explorer.render(pricing_df=pricing_df, tooltips=TOOLTIPS)

    with tabs[2]:
        forecast_explorer.render(forecast_df=forecast_df, tooltips=TOOLTIPS)

    with tabs[3]:
        guardrail_transparency.render(
            pricing_df=pricing_df,
            reason_catalog_df=reason_codes_df,
            tooltips=TOOLTIPS,
        )


if __name__ == "__main__":
    main()
