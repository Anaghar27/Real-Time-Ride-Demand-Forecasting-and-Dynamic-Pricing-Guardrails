# This package holds the top-level dashboard page renderers for each decision-support workflow.
# It exists so each page can own its own charts, tables, and explanatory copy.
# Keeping pages separate makes it easier to extend without turning app.py into a monolith.

__all__ = ["overview", "pricing_explorer", "forecast_explorer", "guardrail_transparency"]
