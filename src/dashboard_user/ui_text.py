# This file stores copy blocks for headings, section descriptions, and empty-state messages.
# It exists so narrative wording stays consistent across the dashboard pages.
# Centralizing text also makes future wording reviews easier without touching rendering logic.
# The constants are intentionally plain and business-facing for non-technical readers.

from __future__ import annotations

APP_TITLE = "Ride Demand Decision Support"
APP_SUBTITLE = (
    "Explore demand forecasts and dynamic pricing outcomes with plain-language guardrail context."
)

EMPTY_PRICING = "No pricing decisions match the current filters in this window."
EMPTY_FORECAST = "No demand forecasts match the current filters in this window."
EMPTY_METADATA = "Reference metadata is currently unavailable."
PARTIAL_COVERAGE = (
    "Partial coverage detected: some views have data while others are empty for this filter window."
)
