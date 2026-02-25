# This file implements read services for forecast endpoints.
# It exists so route handlers can focus on HTTP concerns while query logic stays centralized.
# The service guarantees deterministic sort and pagination across forecast list responses.
# It also applies plain-language demand and confidence summaries when enabled.

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.api.api_config import ApiConfig
from src.api.db_access import DatabaseClient
from src.api.error_handlers import APIError
from src.api.pagination import SortSpec
from src.api.plain_language import forecast_plain_fields

FORECAST_SORT_FIELD_MAP: dict[str, str] = {
    "zone_id": "f.zone_id",
    "bucket_start_ts": "f.bucket_start_ts",
    "y_pred": "f.y_pred",
    "confidence_score": "f.confidence_score",
    "borough": "z.borough",
}


class ForecastService:
    """Data retrieval and shaping for forecast API routes."""

    def __init__(self, *, config: ApiConfig, db: DatabaseClient) -> None:
        self.config = config
        self.db = db
        self.forecast_table = self.config.validate_table_name(self.config.forecast_table_name)
        self.zone_table = self.config.validate_table_name(self.config.zone_table_name)
        self.forecast_run_log_table = self.config.validate_table_name(
            self.config.forecast_run_log_table_name
        )

    def get_latest_forecast(
        self,
        *,
        zone_id: int | None,
        borough: str | None,
        page: int,
        page_size: int,
        sort: SortSpec,
        include_plain_language_fields: bool,
    ) -> dict[str, Any]:
        latest_run_id = self._latest_run_id()
        if latest_run_id is None:
            return {"rows": [], "total_count": 0, "warnings": ["No forecast run found."]}

        return self.get_forecast_window(
            start_ts=None,
            end_ts=None,
            zone_id=zone_id,
            borough=borough,
            run_id=latest_run_id,
            page=page,
            page_size=page_size,
            sort=sort,
            include_plain_language_fields=include_plain_language_fields,
        )

    def get_forecast_window(
        self,
        *,
        start_ts: datetime | None,
        end_ts: datetime | None,
        zone_id: int | None,
        borough: str | None,
        run_id: str | None,
        page: int,
        page_size: int,
        sort: SortSpec,
        include_plain_language_fields: bool,
    ) -> dict[str, Any]:
        effective_run_id = run_id or self._latest_run_id()
        if effective_run_id is None:
            return {"rows": [], "total_count": 0, "warnings": ["No forecast run found."]}

        where_clauses: list[str] = ["f.run_id = :run_id"]
        params: dict[str, Any] = {"run_id": effective_run_id}

        if start_ts is not None:
            where_clauses.append("f.bucket_start_ts >= :start_ts")
            params["start_ts"] = start_ts
        if end_ts is not None:
            where_clauses.append("f.bucket_start_ts <= :end_ts")
            params["end_ts"] = end_ts
        if zone_id is not None:
            where_clauses.append("f.zone_id = :zone_id")
            params["zone_id"] = zone_id
        if borough is not None:
            where_clauses.append("LOWER(z.borough) = LOWER(:borough)")
            params["borough"] = borough

        where_sql = " AND ".join(where_clauses)
        order_sql = self._order_by_clause(sort)

        count_query = f"""
        SELECT COUNT(*) AS total_count
        FROM {self.forecast_table} f
        LEFT JOIN {self.zone_table} z ON z.location_id = f.zone_id
        WHERE {where_sql}
        """
        total_count_row = self.db.fetch_one(count_query, params) or {"total_count": 0}
        total_count = int(total_count_row["total_count"])

        data_query = f"""
        SELECT
            f.zone_id,
            f.bucket_start_ts,
            f.forecast_run_key,
            f.run_id,
            f.horizon_index,
            z.zone AS zone_name,
            z.borough,
            z.service_zone,
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
        FROM {self.forecast_table} f
        LEFT JOIN {self.zone_table} z ON z.location_id = f.zone_id
        WHERE {where_sql}
        ORDER BY {order_sql}, f.zone_id ASC, f.bucket_start_ts ASC
        LIMIT :limit OFFSET :offset
        """

        page_params = dict(params)
        page_params["limit"] = page_size
        page_params["offset"] = (page - 1) * page_size

        raw_rows = self.db.fetch_all(data_query, page_params)

        rows: list[dict[str, Any]] = []
        for row in raw_rows:
            shaped = dict(row)
            if include_plain_language_fields:
                shaped.update(forecast_plain_fields(shaped))
            else:
                shaped["zone_name"] = None
                shaped["demand_outlook_label"] = None
                shaped["confidence_note"] = None
                shaped["forecast_range_summary"] = None
            rows.append(shaped)

        return {"rows": rows, "total_count": total_count, "warnings": None}

    def get_zone_timeline(
        self,
        *,
        zone_id: int,
        start_ts: datetime | None,
        end_ts: datetime | None,
        page: int,
        page_size: int,
        sort: SortSpec,
        include_plain_language_fields: bool,
    ) -> dict[str, Any]:
        if not self.zone_exists(zone_id):
            raise APIError(
                status_code=404,
                error_code="ZONE_NOT_FOUND",
                message=f"Unknown zone_id: {zone_id}",
            )

        return self.get_forecast_window(
            start_ts=start_ts,
            end_ts=end_ts,
            zone_id=zone_id,
            borough=None,
            run_id=None,
            page=page,
            page_size=page_size,
            sort=sort,
            include_plain_language_fields=include_plain_language_fields,
        )

    def get_latest_run_summary(self) -> dict[str, Any] | None:
        query = f"""
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
        FROM {self.forecast_run_log_table}
        ORDER BY started_at DESC
        LIMIT 1
        """
        return self.db.fetch_one(query)

    def get_run_summary(self, *, run_id: str) -> dict[str, Any] | None:
        query = f"""
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
        FROM {self.forecast_run_log_table}
        WHERE run_id = :run_id
        LIMIT 1
        """
        return self.db.fetch_one(query, {"run_id": run_id})

    def zone_exists(self, zone_id: int) -> bool:
        query = f"SELECT 1 FROM {self.zone_table} WHERE location_id = :zone_id LIMIT 1"
        return self.db.fetch_one(query, {"zone_id": zone_id}) is not None

    def _latest_run_id(self) -> str | None:
        by_log_query = f"""
        SELECT run_id
        FROM {self.forecast_run_log_table}
        WHERE LOWER(status) = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """
        row = self.db.fetch_one(by_log_query)
        if row and row.get("run_id"):
            return str(row["run_id"])

        fallback_query = f"""
        SELECT run_id
        FROM {self.forecast_table}
        ORDER BY forecast_created_at DESC
        LIMIT 1
        """
        fallback = self.db.fetch_one(fallback_query)
        if fallback and fallback.get("run_id"):
            return str(fallback["run_id"])
        return None

    @staticmethod
    def _order_by_clause(sort: SortSpec) -> str:
        sql_expr = FORECAST_SORT_FIELD_MAP[sort.field]
        return f"{sql_expr} {sort.order.upper()}"
