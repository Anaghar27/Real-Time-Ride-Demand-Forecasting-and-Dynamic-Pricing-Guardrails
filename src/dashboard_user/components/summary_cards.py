# This file renders compact KPI cards used on overview-style dashboard sections.
# It exists so key metrics share one consistent visual and tooltip pattern.
# Reusing this component keeps cards readable and easy to compare across reruns.
# The function expects business-ready values and does not perform heavy computation.

from __future__ import annotations

import streamlit as st


def render_overview_cards(
    *,
    zones_covered: str,
    avg_final_multiplier: str,
    count_capped: str,
    count_rate_limited: str,
    low_confidence_share: str,
    tooltips: dict[str, str],
) -> None:
    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric(
        "Zones Covered",
        zones_covered,
        help=tooltips["zones_covered_card"],
    )
    col2.metric(
        "Avg Final Multiplier",
        avg_final_multiplier,
        help=tooltips["avg_final_multiplier_card"],
    )
    col3.metric(
        "Count Capped",
        count_capped,
        help=tooltips["count_capped_card"],
    )
    col4.metric(
        "Count Rate-Limited",
        count_rate_limited,
        help=tooltips["count_rate_limited_card"],
    )
    col5.metric(
        "Low Confidence Share",
        low_confidence_share,
        help=tooltips["low_confidence_share_card"],
    )
