# This file builds response envelopes for API endpoints in a consistent format.
# It exists so downstream systems always receive version metadata and request tracing fields.
# The helpers return plain dictionaries that Pydantic response models validate at runtime.
# This keeps endpoint functions focused on data retrieval instead of repetitive envelope assembly.

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.api.schema_versions import build_version_fields


def utc_now_iso() -> datetime:
    """Return timezone-aware UTC timestamp for response generation."""

    return datetime.now(tz=UTC)


def build_list_envelope(
    *,
    api_version_path: str,
    schema_version: str,
    request_id: str,
    data: list[dict[str, Any]],
    pagination: dict[str, Any],
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    """Build standard list response envelope."""

    payload = {
        **build_version_fields(api_version_path=api_version_path, schema_version=schema_version),
        "request_id": request_id,
        "generated_at": utc_now_iso(),
        "data": data,
        "pagination": pagination,
        "warnings": warnings,
    }
    return payload


def build_object_envelope(
    *,
    api_version_path: str,
    schema_version: str,
    request_id: str,
    data: dict[str, Any] | list[dict[str, Any]] | None,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    """Build standard non-list response envelope."""

    return {
        **build_version_fields(api_version_path=api_version_path, schema_version=schema_version),
        "request_id": request_id,
        "generated_at": utc_now_iso(),
        "data": data,
        "warnings": warnings,
    }
