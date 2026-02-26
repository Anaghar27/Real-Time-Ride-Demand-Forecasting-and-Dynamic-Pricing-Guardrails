# This file provides direct Postgres queries for dashboard fallback and observability joins.
# It exists so the Streamlit app can keep working when the API is down or when specific run filters are needed.
# The client keeps SQL in one place and returns pandas DataFrames ready for charting.
# Having this separate layer also makes it easier to test API-to-DB fallback behavior.

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pandas as pd

from src.api.db_access import DatabaseClient


class DatabaseUnavailableError(RuntimeError):
    """Raised when DB fallback is requested without a configured connection."""


class DashboardDbClient:
    def __init__(self, *, database_url: str | None) -> None:
        self._database_url = database_url
        self._db = DatabaseClient(database_url=database_url) if database_url else None

    def can_connect(self) -> bool:
        if self._db is None:
            return False
        return self._db.can_connect()

    def get_zone_catalog(self) -> pd.DataFrame:
        query = """
        SELECT
            z.location_id AS zone_id,
            z.zone AS zone_name,
            z.borough,
            z.service_zone
        FROM dim_zone z
        ORDER BY z.borough ASC, z.zone ASC
        """
        return self._fetch_dataframe(query)

    def get_reason_code_catalog(self) -> pd.DataFrame:
        query = """
        SELECT
            r.reason_code,
            r.category,
            r.description,
            r.active_flag
        FROM reason_code_reference r
        WHERE r.active_flag = TRUE
        ORDER BY r.reason_code ASC
        """
        return self._fetch_dataframe(query)

    def get_recent_pricing_runs(self, *, limit: int = 50) -> pd.DataFrame:
        query = """
        SELECT
            run_id,
            status,
            started_at,
            ended_at,
            failure_reason,
            pricing_policy_version,
            forecast_run_id,
            target_bucket_start,
            target_bucket_end,
            zone_count,
            row_count,
            cap_applied_count,
            rate_limited_count,
            low_confidence_count,
            latency_ms
        FROM pricing_run_log
        ORDER BY started_at DESC
        LIMIT :limit
        """
        return self._fetch_dataframe(query, {"limit": limit})

    def get_feature_time_bounds(self) -> tuple[datetime | None, datetime | None]:
        query = """
        SELECT
            MIN(bucket_start_ts) AS min_feature_ts,
            MAX(bucket_start_ts) AS max_feature_ts
        FROM fact_demand_features
        """
        row = self._fetch_one(query) or {}
        min_feature_ts = row.get("min_feature_ts")
        max_feature_ts = row.get("max_feature_ts")
        return min_feature_ts, max_feature_ts

    def get_latest_pricing_run(self) -> dict[str, Any] | None:
        query = """
        SELECT
            run_id,
            status,
            started_at,
            ended_at,
            failure_reason,
            pricing_policy_version,
            forecast_run_id,
            target_bucket_start,
            target_bucket_end,
            zone_count,
            row_count,
            cap_applied_count,
            rate_limited_count,
            low_confidence_count,
            latency_ms
        FROM pricing_run_log
        WHERE LOWER(status) = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """
        return self._fetch_one(query)

    def get_latest_forecast_run(self) -> dict[str, Any] | None:
        query = """
        SELECT
            run_id,
            status,
            started_at,
            ended_at,
            failure_reason,
            model_name,
            model_version,
            model_stage,
            feature_version,
            forecast_run_key,
            forecast_start_ts,
            forecast_end_ts,
            horizon_buckets,
            bucket_minutes,
            zone_count,
            row_count,
            latency_ms
        FROM scoring_run_log
        WHERE LOWER(status) = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """
        return self._fetch_one(query)

    def get_pricing_window(
        self,
        *,
        start_ts: datetime,
        end_ts: datetime,
        borough: str | None,
        zone_id: int | None,
        uncertainty_band: str | None,
        cap_only: bool,
        rate_limit_only: bool,
        low_confidence_only: bool,
        low_confidence_threshold: float,
        run_id: str | None,
        page_size: int,
    ) -> pd.DataFrame:
        effective_run_id = run_id or self._latest_pricing_run_id()
        if not effective_run_id:
            return pd.DataFrame(columns=self._pricing_columns())

        where_clauses = [
            "p.run_id = :run_id",
            "p.bucket_start_ts >= :start_ts",
            "p.bucket_start_ts <= :end_ts",
        ]
        params: dict[str, Any] = {
            "run_id": effective_run_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": page_size,
            "low_confidence_threshold": low_confidence_threshold,
        }

        if borough:
            where_clauses.append("LOWER(z.borough) = LOWER(:borough)")
            params["borough"] = borough
        if zone_id is not None:
            where_clauses.append("p.zone_id = :zone_id")
            params["zone_id"] = zone_id
        if uncertainty_band:
            where_clauses.append("LOWER(p.uncertainty_band) = LOWER(:uncertainty_band)")
            params["uncertainty_band"] = uncertainty_band
        if cap_only:
            where_clauses.append("p.cap_applied = TRUE")
        if rate_limit_only:
            where_clauses.append("p.rate_limit_applied = TRUE")
        if low_confidence_only:
            where_clauses.append("p.confidence_score < :low_confidence_threshold")

        query = f"""
        SELECT
            p.zone_id,
            z.zone AS zone_name,
            z.borough,
            z.service_zone,
            p.bucket_start_ts,
            p.pricing_created_at,
            p.run_id,
            p.pricing_run_key,
            p.forecast_run_id,
            p.final_multiplier,
            p.raw_multiplier,
            p.pre_cap_multiplier,
            p.post_cap_multiplier,
            p.confidence_score,
            p.uncertainty_band,
            p.y_pred,
            p.y_pred_lower,
            p.y_pred_upper,
            p.cap_applied,
            p.cap_type,
            p.cap_reason,
            p.rate_limit_applied,
            p.rate_limit_direction,
            p.smoothing_applied,
            p.primary_reason_code,
            p.reason_codes_json,
            p.reason_summary,
            p.pricing_policy_version
        FROM pricing_decisions p
        LEFT JOIN dim_zone z ON z.location_id = p.zone_id
        WHERE {' AND '.join(where_clauses)}
        ORDER BY p.bucket_start_ts DESC, p.zone_id ASC
        LIMIT :limit
        """

        dataframe = self._fetch_dataframe(query, params)
        if dataframe.empty:
            return pd.DataFrame(columns=self._pricing_columns())

        dataframe["reason_codes"] = dataframe["reason_codes_json"].apply(
            self._normalize_reason_codes
        )
        dataframe = dataframe.drop(columns=["reason_codes_json"])
        return dataframe

    def get_forecast_window(
        self,
        *,
        start_ts: datetime,
        end_ts: datetime,
        borough: str | None,
        zone_id: int | None,
        uncertainty_band: str | None,
        low_confidence_only: bool,
        low_confidence_threshold: float,
        run_id: str | None,
        page_size: int,
    ) -> pd.DataFrame:
        effective_run_id = run_id or self._latest_forecast_run_id()
        if not effective_run_id:
            return pd.DataFrame(columns=self._forecast_columns())

        where_clauses = [
            "f.run_id = :run_id",
            "f.bucket_start_ts >= :start_ts",
            "f.bucket_start_ts <= :end_ts",
        ]
        params: dict[str, Any] = {
            "run_id": effective_run_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": page_size,
            "low_confidence_threshold": low_confidence_threshold,
        }

        if borough:
            where_clauses.append("LOWER(z.borough) = LOWER(:borough)")
            params["borough"] = borough
        if zone_id is not None:
            where_clauses.append("f.zone_id = :zone_id")
            params["zone_id"] = zone_id
        if uncertainty_band:
            where_clauses.append("LOWER(f.uncertainty_band) = LOWER(:uncertainty_band)")
            params["uncertainty_band"] = uncertainty_band
        if low_confidence_only:
            where_clauses.append("f.confidence_score < :low_confidence_threshold")

        query = f"""
        SELECT
            f.zone_id,
            z.zone AS zone_name,
            z.borough,
            z.service_zone,
            f.bucket_start_ts,
            f.forecast_created_at,
            f.run_id,
            f.forecast_run_key,
            f.horizon_index,
            f.y_pred,
            f.y_pred_lower,
            f.y_pred_upper,
            f.confidence_score,
            f.uncertainty_band,
            f.used_recursive_features,
            f.model_name,
            f.model_version,
            f.model_stage,
            f.feature_version
        FROM demand_forecast f
        LEFT JOIN dim_zone z ON z.location_id = f.zone_id
        WHERE {' AND '.join(where_clauses)}
        ORDER BY f.bucket_start_ts ASC, f.zone_id ASC
        LIMIT :limit
        """

        dataframe = self._fetch_dataframe(query, params)
        if dataframe.empty:
            return pd.DataFrame(columns=self._forecast_columns())
        return dataframe

    def _latest_pricing_run_id(self) -> str | None:
        query = """
        SELECT run_id
        FROM pricing_run_log
        WHERE LOWER(status) = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """
        row = self._fetch_one(query)
        if row and row.get("run_id"):
            return str(row["run_id"])

        fallback_query = """
        SELECT run_id
        FROM pricing_decisions
        ORDER BY pricing_created_at DESC
        LIMIT 1
        """
        fallback = self._fetch_one(fallback_query)
        return str(fallback["run_id"]) if fallback and fallback.get("run_id") else None

    def _latest_forecast_run_id(self) -> str | None:
        query = """
        SELECT run_id
        FROM scoring_run_log
        WHERE LOWER(status) = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """
        row = self._fetch_one(query)
        if row and row.get("run_id"):
            return str(row["run_id"])

        fallback_query = """
        SELECT run_id
        FROM demand_forecast
        ORDER BY forecast_created_at DESC
        LIMIT 1
        """
        fallback = self._fetch_one(fallback_query)
        return str(fallback["run_id"]) if fallback and fallback.get("run_id") else None

    @staticmethod
    def _normalize_reason_codes(raw_codes: Any) -> list[str]:
        if raw_codes is None:
            return []
        if isinstance(raw_codes, list):
            return [str(item) for item in raw_codes]
        if isinstance(raw_codes, str):
            try:
                decoded = json.loads(raw_codes)
            except json.JSONDecodeError:
                return []
            if isinstance(decoded, list):
                return [str(item) for item in decoded]
        return []

    def _fetch_one(self, query: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        db = self._get_db()
        return db.fetch_one(query, params)

    def _fetch_dataframe(self, query: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
        db = self._get_db()
        rows = db.fetch_all(query, params)
        return pd.DataFrame(rows)

    def _get_db(self) -> DatabaseClient:
        if self._db is None:
            raise DatabaseUnavailableError(
                "DASHBOARD_DATABASE_URL or DATABASE_URL is required for DB fallback operations."
            )
        return self._db

    @staticmethod
    def _pricing_columns() -> list[str]:
        return [
            "zone_id",
            "zone_name",
            "borough",
            "service_zone",
            "bucket_start_ts",
            "pricing_created_at",
            "run_id",
            "pricing_run_key",
            "forecast_run_id",
            "final_multiplier",
            "raw_multiplier",
            "pre_cap_multiplier",
            "post_cap_multiplier",
            "confidence_score",
            "uncertainty_band",
            "y_pred",
            "y_pred_lower",
            "y_pred_upper",
            "cap_applied",
            "cap_type",
            "cap_reason",
            "rate_limit_applied",
            "rate_limit_direction",
            "smoothing_applied",
            "primary_reason_code",
            "reason_codes",
            "reason_summary",
            "pricing_policy_version",
        ]

    @staticmethod
    def _forecast_columns() -> list[str]:
        return [
            "zone_id",
            "zone_name",
            "borough",
            "service_zone",
            "bucket_start_ts",
            "forecast_created_at",
            "run_id",
            "forecast_run_key",
            "horizon_index",
            "y_pred",
            "y_pred_lower",
            "y_pred_upper",
            "confidence_score",
            "uncertainty_band",
            "used_recursive_features",
            "model_name",
            "model_version",
            "model_stage",
            "feature_version",
        ]
