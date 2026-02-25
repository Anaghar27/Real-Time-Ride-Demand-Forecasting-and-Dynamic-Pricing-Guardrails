# This file implements metadata lookup services for zones, reason codes, and policy snapshots.
# It exists so routers can return reference data without embedding SQL directly.
# The service provides deterministic list sorting and pagination for catalog endpoints.
# It also exposes a schema catalog payload used by version-aware API consumers.

from __future__ import annotations

from typing import Any

from src.api.api_config import ApiConfig
from src.api.db_access import DatabaseClient
from src.api.pagination import SortSpec

ZONE_SORT_FIELD_MAP: dict[str, str] = {
    "zone_id": "z.location_id",
    "zone_name": "z.zone",
    "borough": "z.borough",
}

REASON_SORT_FIELD_MAP: dict[str, str] = {
    "reason_code": "r.reason_code",
    "category": "r.category",
    "active_flag": "r.active_flag",
}


class MetadataService:
    """Data retrieval for metadata endpoints."""

    def __init__(self, *, config: ApiConfig, db: DatabaseClient) -> None:
        self.config = config
        self.db = db
        self.zone_table = self.config.validate_table_name(self.config.zone_table_name)
        self.reason_code_table = self.config.validate_table_name(self.config.reason_code_table_name)
        self.policy_table = self.config.validate_table_name(self.config.pricing_policy_snapshot_table_name)

    def get_zones(
        self,
        *,
        borough: str | None,
        service_zone: str | None,
        page: int,
        page_size: int,
        sort: SortSpec,
    ) -> dict[str, Any]:
        where_clauses: list[str] = ["1 = 1"]
        params: dict[str, Any] = {}

        if borough:
            where_clauses.append("LOWER(z.borough) = LOWER(:borough)")
            params["borough"] = borough
        if service_zone:
            where_clauses.append("LOWER(COALESCE(z.service_zone, '')) = LOWER(:service_zone)")
            params["service_zone"] = service_zone

        where_sql = " AND ".join(where_clauses)
        order_sql = self._zone_order_by_clause(sort)

        count_query = f"""
        SELECT COUNT(*) AS total_count
        FROM {self.zone_table} z
        WHERE {where_sql}
        """
        total_count_row = self.db.fetch_one(count_query, params) or {"total_count": 0}
        total_count = int(total_count_row["total_count"])

        data_query = f"""
        SELECT
            z.location_id AS zone_id,
            z.zone AS zone_name,
            z.borough,
            z.service_zone
        FROM {self.zone_table} z
        WHERE {where_sql}
        ORDER BY {order_sql}, z.location_id ASC
        LIMIT :limit OFFSET :offset
        """
        page_params = dict(params)
        page_params["limit"] = page_size
        page_params["offset"] = (page - 1) * page_size

        rows = self.db.fetch_all(data_query, page_params)
        return {"rows": rows, "total_count": total_count, "warnings": None}

    def get_reason_codes(
        self,
        *,
        category: str | None,
        active_only: bool,
        page: int,
        page_size: int,
        sort: SortSpec,
    ) -> dict[str, Any]:
        where_clauses: list[str] = ["1 = 1"]
        params: dict[str, Any] = {}

        if category:
            where_clauses.append("LOWER(r.category) = LOWER(:category)")
            params["category"] = category
        if active_only:
            where_clauses.append("r.active_flag = TRUE")

        where_sql = " AND ".join(where_clauses)
        order_sql = self._reason_order_by_clause(sort)

        count_query = f"""
        SELECT COUNT(*) AS total_count
        FROM {self.reason_code_table} r
        WHERE {where_sql}
        """
        total_count_row = self.db.fetch_one(count_query, params) or {"total_count": 0}
        total_count = int(total_count_row["total_count"])

        data_query = f"""
        SELECT
            r.reason_code,
            r.category,
            r.description,
            r.active_flag
        FROM {self.reason_code_table} r
        WHERE {where_sql}
        ORDER BY {order_sql}, r.reason_code ASC
        LIMIT :limit OFFSET :offset
        """
        page_params = dict(params)
        page_params["limit"] = page_size
        page_params["offset"] = (page - 1) * page_size

        rows = self.db.fetch_all(data_query, page_params)
        return {"rows": rows, "total_count": total_count, "warnings": None}

    def get_current_policy(self) -> dict[str, Any]:
        query = f"""
        SELECT
            policy_version,
            effective_from,
            active_flag,
            config_json AS policy_summary
        FROM {self.policy_table}
        WHERE active_flag = TRUE
        ORDER BY effective_from DESC, created_at DESC
        LIMIT 1
        """
        row = self.db.fetch_one(query)
        if row is None:
            return {
                "policy_version": None,
                "effective_from": None,
                "active_flag": None,
                "policy_summary": None,
            }
        return row

    def get_schema_catalog(self) -> dict[str, Any]:
        endpoints = [
            {"endpoint_path": "/health", "method": "GET", "response_model_name": "HealthResponse"},
            {
                "endpoint_path": "/ready",
                "method": "GET",
                "response_model_name": "ReadinessResponse",
            },
            {
                "endpoint_path": "/version",
                "method": "GET",
                "response_model_name": "VersionResponse",
            },
            {
                "endpoint_path": f"{self.config.api_version_path}/pricing/latest",
                "method": "GET",
                "response_model_name": "PricingDecisionListResponseV1",
            },
            {
                "endpoint_path": f"{self.config.api_version_path}/forecast/latest",
                "method": "GET",
                "response_model_name": "ForecastListResponseV1",
            },
            {
                "endpoint_path": f"{self.config.api_version_path}/metadata/zones",
                "method": "GET",
                "response_model_name": "ZoneMetadataListResponseV1",
            },
        ]

        return {
            "api_version_path": self.config.api_version_path,
            "schema_version": self.config.schema_version,
            "compatibility_policy": {
                "non_breaking_changes": [
                    "adding optional fields",
                    "adding new endpoints",
                    "adding reason codes",
                ],
                "breaking_changes": [
                    "renaming fields",
                    "removing fields",
                    "changing field types",
                    "changing nesting shape",
                ],
            },
            "endpoints": endpoints,
        }

    def zone_exists(self, zone_id: int) -> bool:
        query = f"SELECT 1 FROM {self.zone_table} WHERE location_id = :zone_id LIMIT 1"
        return self.db.fetch_one(query, {"zone_id": zone_id}) is not None

    @staticmethod
    def _zone_order_by_clause(sort: SortSpec) -> str:
        return f"{ZONE_SORT_FIELD_MAP[sort.field]} {sort.order.upper()}"

    @staticmethod
    def _reason_order_by_clause(sort: SortSpec) -> str:
        return f"{REASON_SORT_FIELD_MAP[sort.field]} {sort.order.upper()}"
