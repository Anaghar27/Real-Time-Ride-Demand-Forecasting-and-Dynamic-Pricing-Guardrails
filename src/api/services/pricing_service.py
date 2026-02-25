# This file implements read services for pricing decision endpoints.
# It exists so routers can stay transport-focused while SQL and row shaping live in one layer.
# The service enforces deterministic sort/pagination and uses parameterized queries for safety.
# It also adds plain-language fields so non-technical users can interpret pricing decisions quickly.

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from src.api.api_config import ApiConfig
from src.api.db_access import DatabaseClient
from src.api.error_handlers import APIError
from src.api.pagination import SortSpec
from src.api.plain_language import pricing_plain_fields

PRICING_SORT_FIELD_MAP: dict[str, str] = {
    "zone_id": "p.zone_id",
    "bucket_start_ts": "p.bucket_start_ts",
    "final_multiplier": "p.final_multiplier",
    "raw_multiplier": "p.raw_multiplier",
    "confidence_score": "p.confidence_score",
    "borough": "z.borough",
}


class PricingService:
    """Data retrieval and shaping for pricing API routes."""

    def __init__(self, *, config: ApiConfig, db: DatabaseClient) -> None:
        self.config = config
        self.db = db
        self.pricing_table = self.config.validate_table_name(self.config.pricing_table_name)
        self.zone_table = self.config.validate_table_name(self.config.zone_table_name)
        self.pricing_run_log_table = self.config.validate_table_name(
            self.config.pricing_run_log_table_name
        )

    def get_latest_pricing(
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
            return {"rows": [], "total_count": 0, "warnings": ["No pricing run found."]}

        return self.get_pricing_window(
            start_ts=None,
            end_ts=None,
            zone_id=zone_id,
            borough=borough,
            uncertainty_band=None,
            cap_applied=None,
            rate_limit_applied=None,
            run_id=latest_run_id,
            page=page,
            page_size=page_size,
            sort=sort,
            include_plain_language_fields=include_plain_language_fields,
        )

    def get_pricing_window(
        self,
        *,
        start_ts: datetime | None,
        end_ts: datetime | None,
        zone_id: int | None,
        borough: str | None,
        uncertainty_band: str | None,
        cap_applied: bool | None,
        rate_limit_applied: bool | None,
        run_id: str | None,
        page: int,
        page_size: int,
        sort: SortSpec,
        include_plain_language_fields: bool,
    ) -> dict[str, Any]:
        effective_run_id = run_id or self._latest_run_id()
        if effective_run_id is None:
            return {"rows": [], "total_count": 0, "warnings": ["No pricing run found."]}

        where_clauses: list[str] = ["p.run_id = :run_id"]
        params: dict[str, Any] = {"run_id": effective_run_id}

        if start_ts is not None:
            where_clauses.append("p.bucket_start_ts >= :start_ts")
            params["start_ts"] = start_ts
        if end_ts is not None:
            where_clauses.append("p.bucket_start_ts <= :end_ts")
            params["end_ts"] = end_ts
        if zone_id is not None:
            where_clauses.append("p.zone_id = :zone_id")
            params["zone_id"] = zone_id
        if borough is not None:
            where_clauses.append("LOWER(z.borough) = LOWER(:borough)")
            params["borough"] = borough
        if uncertainty_band is not None:
            where_clauses.append("LOWER(p.uncertainty_band) = LOWER(:uncertainty_band)")
            params["uncertainty_band"] = uncertainty_band
        if cap_applied is not None:
            where_clauses.append("p.cap_applied = :cap_applied")
            params["cap_applied"] = cap_applied
        if rate_limit_applied is not None:
            where_clauses.append("p.rate_limit_applied = :rate_limit_applied")
            params["rate_limit_applied"] = rate_limit_applied

        where_sql = " AND ".join(where_clauses)
        order_sql = self._order_by_clause(sort)

        count_query = f"""
        SELECT COUNT(*) AS total_count
        FROM {self.pricing_table} p
        LEFT JOIN {self.zone_table} z ON z.location_id = p.zone_id
        WHERE {where_sql}
        """
        total_count_row = self.db.fetch_one(count_query, params) or {"total_count": 0}
        total_count = int(total_count_row["total_count"])

        data_query = f"""
        SELECT
            p.zone_id,
            p.bucket_start_ts,
            p.pricing_run_key,
            p.run_id,
            p.forecast_run_id,
            z.zone AS zone_name,
            z.borough,
            z.service_zone,
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
        FROM {self.pricing_table} p
        LEFT JOIN {self.zone_table} z ON z.location_id = p.zone_id
        WHERE {where_sql}
        ORDER BY {order_sql}, p.zone_id ASC, p.bucket_start_ts ASC
        LIMIT :limit OFFSET :offset
        """

        page_params = dict(params)
        page_params["limit"] = page_size
        page_params["offset"] = (page - 1) * page_size
        raw_rows = self.db.fetch_all(data_query, page_params)

        rows: list[dict[str, Any]] = []
        for row in raw_rows:
            shaped = dict(row)
            shaped["reason_codes"] = self._normalize_reason_codes(shaped.pop("reason_codes_json", None))
            if include_plain_language_fields:
                shaped.update(pricing_plain_fields(shaped))
            else:
                shaped["zone_name"] = None
                shaped["recommended_price_action"] = None
                shaped["why_this_price"] = None
                shaped["guardrail_note"] = None
                shaped["confidence_note"] = None
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

        return self.get_pricing_window(
            start_ts=start_ts,
            end_ts=end_ts,
            zone_id=zone_id,
            borough=None,
            uncertainty_band=None,
            cap_applied=None,
            rate_limit_applied=None,
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
        FROM {self.pricing_run_log_table}
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
        FROM {self.pricing_run_log_table}
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
        FROM {self.pricing_run_log_table}
        WHERE LOWER(status) = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """
        row = self.db.fetch_one(by_log_query)
        if row and row.get("run_id"):
            return str(row["run_id"])

        fallback_query = f"""
        SELECT run_id
        FROM {self.pricing_table}
        ORDER BY pricing_created_at DESC
        LIMIT 1
        """
        fallback = self.db.fetch_one(fallback_query)
        if fallback and fallback.get("run_id"):
            return str(fallback["run_id"])
        return None

    @staticmethod
    def _normalize_reason_codes(reason_codes_json: Any) -> list[str]:
        if reason_codes_json is None:
            return []
        if isinstance(reason_codes_json, list):
            return [str(item) for item in reason_codes_json]
        if isinstance(reason_codes_json, str):
            try:
                parsed = json.loads(reason_codes_json)
            except json.JSONDecodeError:
                return []
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        return []

    @staticmethod
    def _order_by_clause(sort: SortSpec) -> str:
        sql_expr = PRICING_SORT_FIELD_MAP[sort.field]
        return f"{sql_expr} {sort.order.upper()}"
