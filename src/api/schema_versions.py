# This file defines helpers for API path versioning and schema version metadata.
# It exists so every response can carry explicit version fields for consumers.
# The module also provides a basic breaking-change detector used by contract checks.
# Keeping these rules centralized makes compatibility decisions easier to enforce.

from __future__ import annotations

from typing import Any


def api_version_label(api_version_path: str) -> str:
    """Convert `/api/v1` style paths into `v1` labels."""

    cleaned = api_version_path.rstrip("/")
    parts = [part for part in cleaned.split("/") if part]
    if not parts:
        raise ValueError(f"Invalid api_version_path: {api_version_path!r}")
    return parts[-1]


def build_version_fields(*, api_version_path: str, schema_version: str) -> dict[str, str]:
    """Return a normalized version metadata block."""

    return {
        "api_version": api_version_label(api_version_path),
        "schema_version": schema_version,
    }


def detect_breaking_schema_changes(
    *,
    previous_snapshot: dict[str, Any],
    current_snapshot: dict[str, Any],
) -> list[str]:
    """Detect likely breaking changes by checking removed paths and fields."""

    findings: list[str] = []

    previous_paths = set(previous_snapshot.get("paths", {}).keys())
    current_paths = set(current_snapshot.get("paths", {}).keys())
    removed_paths = sorted(previous_paths - current_paths)
    for path in removed_paths:
        findings.append(f"Removed API path: {path}")

    previous_schemas = previous_snapshot.get("components", {}).get("schemas", {})
    current_schemas = current_snapshot.get("components", {}).get("schemas", {})

    for schema_name, previous_schema in previous_schemas.items():
        if schema_name not in current_schemas:
            findings.append(f"Removed schema component: {schema_name}")
            continue

        previous_required = set(previous_schema.get("required", []))
        current_required = set(current_schemas[schema_name].get("required", []))
        for removed_required in sorted(previous_required - current_required):
            findings.append(
                f"Schema {schema_name} removed required field: {removed_required}"
            )

        previous_properties = previous_schema.get("properties", {})
        current_properties = current_schemas[schema_name].get("properties", {})
        for removed_prop in sorted(set(previous_properties) - set(current_properties)):
            findings.append(f"Schema {schema_name} removed property: {removed_prop}")

    return findings
