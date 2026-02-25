# This file implements lightweight diagnostics aggregations for API consumers.
# It exists so monitoring dashboards can quickly check coverage and guardrail behavior.
# The service computes summaries from latest successful forecast and pricing runs.
# Keeping diagnostics read-only and compact helps keep endpoint latency predictable.

from __future__ import annotations

from typing import Any

from src.api.api_config import ApiConfig
from src.api.db_access import DatabaseClient


class DiagnosticsService:
    """Summary diagnostics queries for API endpoints."""

    def __init__(self, *, config: ApiConfig, db: DatabaseClient) -> None:
        self.config = config
        self.db = db
        self.pricing_table = self.config.validate_table_name(self.config.pricing_table_name)
        self.forecast_table = self.config.validate_table_name(self.config.forecast_table_name)
        self.pricing_run_log_table = self.config.validate_table_name(
            self.config.pricing_run_log_table_name
        )
        self.forecast_run_log_table = self.config.validate_table_name(
            self.config.forecast_run_log_table_name
        )

    def get_latest_coverage_summary(self) -> dict[str, Any]:
        pricing_run_id = self._latest_pricing_run_id()
        forecast_run_id = self._latest_forecast_run_id()

        pricing_counts = {
            "pricing_zone_count": 0,
            "pricing_row_count": 0,
        }
        forecast_counts = {
            "forecast_zone_count": 0,
            "forecast_row_count": 0,
        }

        if pricing_run_id is not None:
            pricing_query = f"""
            SELECT
                COUNT(DISTINCT zone_id) AS pricing_zone_count,
                COUNT(*) AS pricing_row_count
            FROM {self.pricing_table}
            WHERE run_id = :run_id
            """
            pricing_counts = self.db.fetch_one(pricing_query, {"run_id": pricing_run_id}) or pricing_counts

        if forecast_run_id is not None:
            forecast_query = f"""
            SELECT
                COUNT(DISTINCT zone_id) AS forecast_zone_count,
                COUNT(*) AS forecast_row_count
            FROM {self.forecast_table}
            WHERE run_id = :run_id
            """
            forecast_counts = self.db.fetch_one(forecast_query, {"run_id": forecast_run_id}) or forecast_counts

        return {
            "pricing_run_id": pricing_run_id,
            "forecast_run_id": forecast_run_id,
            "pricing_zone_count": int(pricing_counts.get("pricing_zone_count", 0)),
            "forecast_zone_count": int(forecast_counts.get("forecast_zone_count", 0)),
            "pricing_row_count": int(pricing_counts.get("pricing_row_count", 0)),
            "forecast_row_count": int(forecast_counts.get("forecast_row_count", 0)),
        }

    def get_latest_guardrail_summary(self) -> dict[str, Any]:
        pricing_run_id = self._latest_pricing_run_id()
        if pricing_run_id is None:
            return {
                "pricing_run_id": None,
                "total_rows": 0,
                "cap_applied_rows": 0,
                "rate_limited_rows": 0,
                "smoothing_applied_rows": 0,
                "cap_applied_rate": 0.0,
                "rate_limited_rate": 0.0,
            }

        query = f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN cap_applied THEN 1 ELSE 0 END) AS cap_applied_rows,
            SUM(CASE WHEN rate_limit_applied THEN 1 ELSE 0 END) AS rate_limited_rows,
            SUM(CASE WHEN smoothing_applied THEN 1 ELSE 0 END) AS smoothing_applied_rows
        FROM {self.pricing_table}
        WHERE run_id = :run_id
        """
        row = self.db.fetch_one(query, {"run_id": pricing_run_id}) or {}
        total_rows = int(row.get("total_rows", 0) or 0)
        cap_applied_rows = int(row.get("cap_applied_rows", 0) or 0)
        rate_limited_rows = int(row.get("rate_limited_rows", 0) or 0)
        smoothing_applied_rows = int(row.get("smoothing_applied_rows", 0) or 0)

        cap_rate = (cap_applied_rows / total_rows) if total_rows else 0.0
        rate_limit_rate = (rate_limited_rows / total_rows) if total_rows else 0.0

        return {
            "pricing_run_id": pricing_run_id,
            "total_rows": total_rows,
            "cap_applied_rows": cap_applied_rows,
            "rate_limited_rows": rate_limited_rows,
            "smoothing_applied_rows": smoothing_applied_rows,
            "cap_applied_rate": cap_rate,
            "rate_limited_rate": rate_limit_rate,
        }

    def get_latest_confidence_summary(self) -> dict[str, Any]:
        forecast_run_id = self._latest_forecast_run_id()
        if forecast_run_id is None:
            return {"forecast_run_id": None, "bands": []}

        query = f"""
        SELECT
            uncertainty_band,
            COUNT(*) AS row_count,
            AVG(confidence_score) AS avg_confidence_score
        FROM {self.forecast_table}
        WHERE run_id = :run_id
        GROUP BY uncertainty_band
        ORDER BY uncertainty_band ASC
        """
        rows = self.db.fetch_all(query, {"run_id": forecast_run_id})

        bands: list[dict[str, Any]] = []
        for row in rows:
            bands.append(
                {
                    "uncertainty_band": str(row.get("uncertainty_band", "unknown")),
                    "row_count": int(row.get("row_count", 0) or 0),
                    "avg_confidence_score": float(row.get("avg_confidence_score", 0.0) or 0.0),
                }
            )

        return {
            "forecast_run_id": forecast_run_id,
            "bands": bands,
        }

    def _latest_pricing_run_id(self) -> str | None:
        query = f"""
        SELECT run_id
        FROM {self.pricing_run_log_table}
        WHERE LOWER(status) = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """
        row = self.db.fetch_one(query)
        if row and row.get("run_id"):
            return str(row["run_id"])

        fallback_query = f"""
        SELECT run_id
        FROM {self.pricing_table}
        ORDER BY pricing_created_at DESC
        LIMIT 1
        """
        fallback_row = self.db.fetch_one(fallback_query)
        return str(fallback_row["run_id"]) if fallback_row and fallback_row.get("run_id") else None

    def _latest_forecast_run_id(self) -> str | None:
        query = f"""
        SELECT run_id
        FROM {self.forecast_run_log_table}
        WHERE LOWER(status) = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """
        row = self.db.fetch_one(query)
        if row and row.get("run_id"):
            return str(row["run_id"])

        fallback_query = f"""
        SELECT run_id
        FROM {self.forecast_table}
        ORDER BY forecast_created_at DESC
        LIMIT 1
        """
        fallback_row = self.db.fetch_one(fallback_query)
        return str(fallback_row["run_id"]) if fallback_row and fallback_row.get("run_id") else None
