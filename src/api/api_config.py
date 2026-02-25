# This file defines runtime settings for the API layer in one place.
# It exists so endpoint behavior, versioning, pagination, and data-source names can be configured without code edits.
# The config loader reads environment variables and applies safe defaults for local development.
# It also validates table names and version paths to prevent unsafe SQL identifier usage.

from __future__ import annotations

import os
import re
from functools import lru_cache

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, field_validator

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class ApiConfig(BaseModel):
    """Typed API runtime configuration."""

    model_config = ConfigDict(extra="ignore")

    api_name: str = "Ride Demand Forecast and Pricing API"
    api_version_path: str = "/api/v1"
    schema_version: str = "1.0.0"
    host: str = "0.0.0.0"
    port: int = 8000
    environment: str = "local"
    database_url: str
    default_page_size: int = 50
    max_page_size: int = 200
    default_sort_order: str = "bucket_start_ts:desc"
    request_timeout_seconds: int = 30
    enable_request_logging: bool = False
    include_plain_language_fields: bool = True
    allowed_origins: list[str] = Field(default_factory=list)
    pricing_table_name: str = "pricing_decisions"
    forecast_table_name: str = "demand_forecast"
    zone_table_name: str = "dim_zone"
    reason_code_table_name: str = "reason_code_reference"
    pricing_run_log_table_name: str = "pricing_run_log"
    forecast_run_log_table_name: str = "scoring_run_log"
    pricing_policy_snapshot_table_name: str = "pricing_policy_snapshot"
    request_log_table_name: str = "api_request_log"
    contract_registry_table_name: str = "api_contract_registry"
    app_version: str = "0.1.0"
    allowed_table_names: set[str] = Field(default_factory=set)

    @field_validator("api_version_path")
    @classmethod
    def validate_api_version_path(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("api_version_path must start with '/'.")
        parts = [part for part in value.split("/") if part]
        if len(parts) < 2 or parts[-1].startswith("v") is False:
            raise ValueError("api_version_path must look like '/api/v1'.")
        return value.rstrip("/")

    @field_validator(
        "pricing_table_name",
        "forecast_table_name",
        "zone_table_name",
        "reason_code_table_name",
        "pricing_run_log_table_name",
        "forecast_run_log_table_name",
        "pricing_policy_snapshot_table_name",
        "request_log_table_name",
        "contract_registry_table_name",
    )
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        if not _IDENTIFIER_RE.match(value):
            raise ValueError(f"Unsafe SQL identifier: {value!r}")
        return value

    @field_validator("default_page_size", "max_page_size", "request_timeout_seconds")
    @classmethod
    def validate_positive_ints(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("Value must be greater than 0.")
        return value

    def api_version_label(self) -> str:
        return self.api_version_path.rstrip("/").split("/")[-1]

    def validate_table_name(self, table_name: str) -> str:
        if not _IDENTIFIER_RE.match(table_name):
            raise ValueError(f"Unsafe SQL identifier: {table_name!r}")
        if table_name not in self.allowed_table_names:
            raise ValueError(f"Table name is not in allowlist: {table_name!r}")
        return table_name


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be boolean-like, got {raw!r}")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_list(name: str, default: list[str] | None = None) -> list[str]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return list(default or [])
    return [item.strip() for item in raw.split(",") if item.strip()]


def _build_allowed_table_names(config_values: dict[str, object]) -> set[str]:
    configured_names = {
        str(config_values["pricing_table_name"]),
        str(config_values["forecast_table_name"]),
        str(config_values["zone_table_name"]),
        str(config_values["reason_code_table_name"]),
        str(config_values["pricing_run_log_table_name"]),
        str(config_values["forecast_run_log_table_name"]),
        str(config_values["pricing_policy_snapshot_table_name"]),
        str(config_values["request_log_table_name"]),
        str(config_values["contract_registry_table_name"]),
    }
    configured_names.update(
        {
            "pricing_decisions",
            "demand_forecast",
            "dim_zone",
            "reason_code_reference",
            "pricing_run_log",
            "scoring_run_log",
            "pricing_policy_snapshot",
            "api_request_log",
            "api_contract_registry",
        }
    )
    configured_names.update(_env_list("API_ALLOWED_TABLE_NAMES", []))
    for table_name in configured_names:
        if not _IDENTIFIER_RE.match(table_name):
            raise ValueError(f"Unsafe SQL identifier in allowlist: {table_name!r}")
    return configured_names


def load_api_config(*, load_env: bool = True) -> ApiConfig:
    """Load API configuration from `.env` and process environment."""

    if load_env:
        load_dotenv()

    config_values: dict[str, object] = {
        "api_name": os.getenv("API_NAME", "Ride Demand Forecast and Pricing API"),
        "api_version_path": os.getenv("API_VERSION_PATH", "/api/v1"),
        "schema_version": os.getenv("API_SCHEMA_VERSION", "1.0.0"),
        "host": os.getenv("API_HOST", "0.0.0.0"),
        "port": _env_int("API_PORT", 8000),
        "environment": os.getenv("ENV", "local"),
        "database_url": os.getenv("DATABASE_URL", ""),
        "default_page_size": _env_int("API_DEFAULT_PAGE_SIZE", 50),
        "max_page_size": _env_int("API_MAX_PAGE_SIZE", 200),
        "default_sort_order": os.getenv("API_DEFAULT_SORT_ORDER", "bucket_start_ts:desc"),
        "request_timeout_seconds": _env_int("API_REQUEST_TIMEOUT_SECONDS", 30),
        "enable_request_logging": _env_bool("API_ENABLE_REQUEST_LOGGING", False),
        "include_plain_language_fields": _env_bool("API_INCLUDE_PLAIN_LANGUAGE_FIELDS", True),
        "allowed_origins": _env_list("API_ALLOWED_ORIGINS", []),
        "pricing_table_name": os.getenv("API_PRICING_TABLE_NAME", "pricing_decisions"),
        "forecast_table_name": os.getenv("API_FORECAST_TABLE_NAME", "demand_forecast"),
        "zone_table_name": os.getenv("API_ZONE_TABLE_NAME", "dim_zone"),
        "reason_code_table_name": os.getenv("API_REASON_CODE_TABLE_NAME", "reason_code_reference"),
        "pricing_run_log_table_name": os.getenv("API_PRICING_RUN_LOG_TABLE_NAME", "pricing_run_log"),
        "forecast_run_log_table_name": os.getenv("API_FORECAST_RUN_LOG_TABLE_NAME", "scoring_run_log"),
        "pricing_policy_snapshot_table_name": os.getenv(
            "API_PRICING_POLICY_SNAPSHOT_TABLE_NAME", "pricing_policy_snapshot"
        ),
        "request_log_table_name": os.getenv("API_REQUEST_LOG_TABLE_NAME", "api_request_log"),
        "contract_registry_table_name": os.getenv(
            "API_CONTRACT_REGISTRY_TABLE_NAME", "api_contract_registry"
        ),
        "app_version": os.getenv("APP_VERSION", "0.1.0"),
    }
    if not config_values["database_url"]:
        raise RuntimeError("DATABASE_URL is required for API startup.")

    config_values["allowed_table_names"] = _build_allowed_table_names(config_values)

    return ApiConfig.model_validate(config_values)


@lru_cache(maxsize=1)
def get_api_config() -> ApiConfig:
    """Cached accessor for API config."""

    return load_api_config()
