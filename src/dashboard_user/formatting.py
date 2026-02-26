# This file collects small formatting helpers used across dashboard pages.
# It exists so metric cards and tables present numbers consistently to non-technical users.
# Keeping these helpers centralized avoids repeated formatting code and subtle inconsistencies.
# The functions intentionally return simple strings that Streamlit can display directly.

from __future__ import annotations


def format_multiplier(value: float | int | None) -> str:
    if value is None:
        return "-"
    return f"{float(value):.2f}x"


def format_percent(value: float | int | None) -> str:
    if value is None:
        return "-"
    return f"{100.0 * float(value):.1f}%"


def format_count(value: int | float | None) -> str:
    if value is None:
        return "0"
    return f"{int(value):,}"
