# This file is the single data interface for the Streamlit decision-support dashboard.
# It exists so pages can request business-ready datasets without caring whether data came from API or DB.
# The module enforces API-first behavior, DB fallback, and TTL caching for repeated queries.
# It also backfills plain-language fields when API responses are unavailable.

from __future__ import annotations

import time
from dataclasses import asdict
from datetime import datetime
from typing import Any, cast

import pandas as pd

from src.api.plain_language import forecast_plain_fields, pricing_plain_fields
from src.dashboard_user.api_client import ApiUnavailableError, DashboardApiClient
from src.dashboard_user.dashboard_config import DashboardConfig, DashboardFilters
from src.dashboard_user.db_client import DashboardDbClient, DatabaseUnavailableError


class _TTLCache:
    def __init__(self) -> None:
        self._store: dict[tuple[Any, ...], tuple[float, Any]] = {}

    def get(self, key: tuple[Any, ...]) -> Any | None:
        cached = self._store.get(key)
        if not cached:
            return None
        expires_at, value = cached
        if time.time() > expires_at:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: tuple[Any, ...], *, value: Any, ttl_seconds: int) -> None:
        self._store[key] = (time.time() + ttl_seconds, value)


class DashboardDataAccess:
    def __init__(
        self,
        *,
        config: DashboardConfig,
        api_client: DashboardApiClient | None = None,
        db_client: DashboardDbClient | None = None,
    ) -> None:
        self.config = config
        self.api_client = api_client or DashboardApiClient(
            base_url=config.api_base_url,
            timeout_seconds=config.request_timeout_seconds,
        )
        self.db_client = db_client or DashboardDbClient(database_url=config.database_url)
        self.cache = _TTLCache()

    def get_zone_catalog(self) -> tuple[pd.DataFrame, str]:
        cache_key = ("zones",)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cast(tuple[pd.DataFrame, str], cached)

        def loader() -> tuple[pd.DataFrame, str]:
            try:
                rows = self.api_client.get_zones(page_size=5000)
                api_df = pd.DataFrame(rows)
                return self._normalize_zone_catalog(api_df), "api"
            except (ApiUnavailableError, ValueError):
                pass

            try:
                db_df = self.db_client.get_zone_catalog()
                return self._normalize_zone_catalog(db_df), "db"
            except DatabaseUnavailableError:
                return (
                    pd.DataFrame(columns=["zone_id", "zone_name", "borough", "service_zone"]),
                    "empty",
                )

        value = loader()
        self.cache.set(
            cache_key,
            value=value,
            ttl_seconds=self.config.metadata_cache_ttl_seconds,
        )
        return value

    def get_reason_code_catalog(self) -> tuple[pd.DataFrame, str]:
        cache_key = ("reason_codes",)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cast(tuple[pd.DataFrame, str], cached)

        def loader() -> tuple[pd.DataFrame, str]:
            try:
                rows = self.api_client.get_reason_codes(page_size=2000)
                api_df = pd.DataFrame(rows)
                return self._normalize_reason_catalog(api_df), "api"
            except (ApiUnavailableError, ValueError):
                pass

            try:
                db_df = self.db_client.get_reason_code_catalog()
                return self._normalize_reason_catalog(db_df), "db"
            except DatabaseUnavailableError:
                return (
                    pd.DataFrame(columns=["reason_code", "category", "description", "active_flag"]),
                    "empty",
                )

        value = loader()
        self.cache.set(
            cache_key,
            value=value,
            ttl_seconds=self.config.metadata_cache_ttl_seconds,
        )
        return value

    def get_latest_run_metadata(self) -> dict[str, Any]:
        cache_key = ("latest_run_metadata",)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cast(dict[str, Any], cached)

        pricing_run: dict[str, Any] | None = None
        pricing_source = "none"
        forecast_run: dict[str, Any] | None = None
        forecast_source = "none"

        try:
            pricing_run = self.api_client.get_latest_pricing_run()
            pricing_source = "api"
        except (ApiUnavailableError, ValueError):
            pricing_run = None

        if pricing_run is None:
            try:
                pricing_run = self.db_client.get_latest_pricing_run()
                pricing_source = "db"
            except DatabaseUnavailableError:
                pricing_run = None

        try:
            forecast_run = self.api_client.get_latest_forecast_run()
            forecast_source = "api"
        except (ApiUnavailableError, ValueError):
            forecast_run = None

        if forecast_run is None:
            try:
                forecast_run = self.db_client.get_latest_forecast_run()
                forecast_source = "db"
            except DatabaseUnavailableError:
                forecast_run = None

        result = {
            "pricing_run": pricing_run,
            "forecast_run": forecast_run,
            "source": {"pricing": pricing_source, "forecast": forecast_source},
        }
        self.cache.set(
            cache_key,
            value=result,
            ttl_seconds=self.config.metadata_cache_ttl_seconds,
        )
        return result

    def get_recent_pricing_runs(self, *, max_items: int) -> pd.DataFrame:
        cache_key = ("recent_pricing_runs", max_items)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cast(pd.DataFrame, cached)

        try:
            runs = self.db_client.get_recent_pricing_runs(limit=max_items)
        except DatabaseUnavailableError:
            runs = pd.DataFrame(columns=["run_id", "status", "started_at", "ended_at"])

        if "started_at" in runs.columns:
            runs["started_at"] = pd.to_datetime(runs["started_at"], utc=True, errors="coerce")
        if "ended_at" in runs.columns:
            runs["ended_at"] = pd.to_datetime(runs["ended_at"], utc=True, errors="coerce")

        self.cache.set(
            cache_key,
            value=runs,
            ttl_seconds=self.config.metadata_cache_ttl_seconds,
        )
        return runs

    def get_feature_time_bounds(self) -> tuple[datetime | None, datetime | None, str]:
        cache_key = ("feature_time_bounds",)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cast(tuple[datetime | None, datetime | None, str], cached)

        try:
            min_ts, max_ts = self.db_client.get_feature_time_bounds()
            value = (min_ts, max_ts, "db")
        except DatabaseUnavailableError:
            value = (None, None, "empty")
        except Exception:
            value = (None, None, "empty")

        self.cache.set(
            cache_key,
            value=value,
            ttl_seconds=self.config.metadata_cache_ttl_seconds,
        )
        return value

    def get_pricing_data(self, filters: DashboardFilters) -> tuple[pd.DataFrame, str]:
        cache_key = ("pricing_window", tuple(asdict(filters).items()))
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cast(tuple[pd.DataFrame, str], cached)

        page_size = self.config.clamp_page_size(filters.page_size)

        if filters.pricing_run_mode == "latest":
            try:
                api_rows = self.api_client.get_pricing_window(
                    start_ts=filters.start_ts,
                    end_ts=filters.end_ts,
                    zone_id=filters.zone_id,
                    borough=filters.borough,
                    uncertainty_band=filters.uncertainty_band,
                    cap_applied=True if filters.cap_only else None,
                    rate_limit_applied=True if filters.rate_limit_only else None,
                    page_size=page_size,
                )
                api_df = self._normalize_pricing_dataframe(pd.DataFrame(api_rows))
                api_df = self._apply_pricing_plain_language(api_df)
                api_df = self._apply_low_confidence_filter(api_df, filters.low_confidence_only)
                value = (api_df, "api")
                self.cache.set(
                    cache_key,
                    value=value,
                    ttl_seconds=self.config.query_cache_ttl_seconds,
                )
                return value
            except (ApiUnavailableError, ValueError):
                pass

        db_run_id = filters.pricing_run_id if filters.pricing_run_mode == "specific" else None

        try:
            db_df = self.db_client.get_pricing_window(
                start_ts=filters.start_ts,
                end_ts=filters.end_ts,
                borough=filters.borough,
                zone_id=filters.zone_id,
                uncertainty_band=filters.uncertainty_band,
                cap_only=filters.cap_only,
                rate_limit_only=filters.rate_limit_only,
                low_confidence_only=filters.low_confidence_only,
                low_confidence_threshold=self.config.low_confidence_threshold,
                run_id=db_run_id,
                page_size=page_size,
            )
        except DatabaseUnavailableError:
            db_df = pd.DataFrame(columns=self._pricing_columns())

        db_df = self._normalize_pricing_dataframe(db_df)
        db_df = self._apply_pricing_plain_language(db_df)
        value = (db_df, "db")
        self.cache.set(cache_key, value=value, ttl_seconds=self.config.query_cache_ttl_seconds)
        return value

    def get_forecast_data(self, filters: DashboardFilters) -> tuple[pd.DataFrame, str]:
        cache_key = ("forecast_window", tuple(asdict(filters).items()))
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cast(tuple[pd.DataFrame, str], cached)

        page_size = self.config.clamp_page_size(filters.page_size)

        try:
            api_rows = self.api_client.get_forecast_window(
                start_ts=filters.start_ts,
                end_ts=filters.end_ts,
                zone_id=filters.zone_id,
                borough=filters.borough,
                page_size=page_size,
            )
            api_df = self._normalize_forecast_dataframe(pd.DataFrame(api_rows))
            api_df = self._apply_forecast_plain_language(api_df)
            if filters.uncertainty_band:
                api_df = api_df[
                    api_df["uncertainty_band"].str.lower() == filters.uncertainty_band.lower()
                ]
            api_df = self._apply_low_confidence_filter(api_df, filters.low_confidence_only)
            value = (api_df, "api")
            self.cache.set(cache_key, value=value, ttl_seconds=self.config.query_cache_ttl_seconds)
            return value
        except (ApiUnavailableError, ValueError):
            pass

        try:
            db_df = self.db_client.get_forecast_window(
                start_ts=filters.start_ts,
                end_ts=filters.end_ts,
                borough=filters.borough,
                zone_id=filters.zone_id,
                uncertainty_band=filters.uncertainty_band,
                low_confidence_only=filters.low_confidence_only,
                low_confidence_threshold=self.config.low_confidence_threshold,
                run_id=None,
                page_size=page_size,
            )
        except DatabaseUnavailableError:
            db_df = pd.DataFrame(columns=self._forecast_columns())

        db_df = self._normalize_forecast_dataframe(db_df)
        db_df = self._apply_forecast_plain_language(db_df)
        value = (db_df, "db")
        self.cache.set(cache_key, value=value, ttl_seconds=self.config.query_cache_ttl_seconds)
        return value

    def _apply_low_confidence_filter(self, dataframe: pd.DataFrame, enabled: bool) -> pd.DataFrame:
        if not enabled or dataframe.empty or "confidence_score" not in dataframe.columns:
            return dataframe
        return dataframe[dataframe["confidence_score"] < self.config.low_confidence_threshold]

    def _normalize_zone_catalog(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        expected = ["zone_id", "zone_name", "borough", "service_zone"]
        result = dataframe.copy()
        for column in expected:
            if column not in result.columns:
                result[column] = None
        return result[expected].sort_values(["borough", "zone_name"], na_position="last")

    def _normalize_reason_catalog(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        expected = ["reason_code", "category", "description", "active_flag"]
        result = dataframe.copy()
        for column in expected:
            if column not in result.columns:
                result[column] = None
        return result[expected].sort_values("reason_code", na_position="last")

    def _normalize_pricing_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        result = dataframe.copy()
        for column in self._pricing_columns():
            if column not in result.columns:
                result[column] = None

        result["bucket_start_ts"] = pd.to_datetime(
            result["bucket_start_ts"], utc=True, errors="coerce"
        )
        result["pricing_created_at"] = pd.to_datetime(
            result["pricing_created_at"], utc=True, errors="coerce"
        )
        for numeric_column in [
            "final_multiplier",
            "raw_multiplier",
            "pre_cap_multiplier",
            "post_cap_multiplier",
            "confidence_score",
            "y_pred",
            "y_pred_lower",
            "y_pred_upper",
        ]:
            result[numeric_column] = pd.to_numeric(result[numeric_column], errors="coerce")
        for bool_column in ["cap_applied", "rate_limit_applied", "smoothing_applied"]:
            result[bool_column] = result[bool_column].fillna(False).astype(bool)

        if "reason_codes" in result.columns:
            result["reason_codes"] = result["reason_codes"].apply(self._normalize_reason_code_list)

        return result[self._pricing_columns()].sort_values(
            ["bucket_start_ts", "zone_id"], ascending=[False, True], na_position="last"
        )

    def _normalize_forecast_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        result = dataframe.copy()
        for column in self._forecast_columns():
            if column not in result.columns:
                result[column] = None

        result["bucket_start_ts"] = pd.to_datetime(
            result["bucket_start_ts"], utc=True, errors="coerce"
        )
        result["forecast_created_at"] = pd.to_datetime(
            result["forecast_created_at"], utc=True, errors="coerce"
        )
        for numeric_column in [
            "horizon_index",
            "y_pred",
            "y_pred_lower",
            "y_pred_upper",
            "confidence_score",
        ]:
            result[numeric_column] = pd.to_numeric(result[numeric_column], errors="coerce")

        return result[self._forecast_columns()].sort_values(
            ["bucket_start_ts", "zone_id"], ascending=[True, True], na_position="last"
        )

    def _apply_pricing_plain_language(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        if dataframe.empty:
            return dataframe

        output = dataframe.copy()
        for target_column in [
            "why_this_price",
            "guardrail_note",
            "confidence_note",
            "recommended_price_action",
        ]:
            if target_column not in output.columns:
                output[target_column] = None

        for index, row in output.iterrows():
            if all(
                row.get(column)
                for column in [
                    "why_this_price",
                    "guardrail_note",
                    "confidence_note",
                    "recommended_price_action",
                ]
            ):
                continue
            generated = pricing_plain_fields(row.to_dict())
            for key, value in generated.items():
                output.at[index, key] = value

        return output

    def _apply_forecast_plain_language(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        if dataframe.empty:
            return dataframe

        output = dataframe.copy()
        for target_column in ["demand_outlook_label", "confidence_note", "forecast_range_summary"]:
            if target_column not in output.columns:
                output[target_column] = None

        for index, row in output.iterrows():
            if all(
                row.get(column)
                for column in ["demand_outlook_label", "confidence_note", "forecast_range_summary"]
            ):
                continue
            generated = forecast_plain_fields(row.to_dict())
            for key, value in generated.items():
                output.at[index, key] = value

        return output

    @staticmethod
    def _normalize_reason_code_list(raw_value: Any) -> list[str]:
        if raw_value is None:
            return []
        if isinstance(raw_value, list):
            return [str(item) for item in raw_value]
        if isinstance(raw_value, str):
            cleaned = raw_value.strip()
            if not cleaned:
                return []
            if cleaned.startswith("[") and cleaned.endswith("]"):
                cleaned = cleaned[1:-1]
            return [part.strip().strip('"') for part in cleaned.split(",") if part.strip()]
        return []

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
            "recommended_price_action",
            "why_this_price",
            "guardrail_note",
            "confidence_note",
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
            "demand_outlook_label",
            "confidence_note",
            "forecast_range_summary",
        ]
