# This file tests API schema contracts, envelope helpers, and versioning utilities.
# It exists to detect accidental response-shape changes before release.
# The tests assert required paths and key fields remain present in OpenAPI output.
# Contract checks here complement snapshot-based validation used in automation.

from __future__ import annotations

import json
from pathlib import Path

from src.api.app import app
from src.api.response_envelope import build_list_envelope, build_object_envelope
from src.api.schema_versions import build_version_fields, detect_breaking_schema_changes


def test_openapi_contains_required_phase7_paths() -> None:
    schema = app.openapi()
    required_paths = {
        "/health",
        "/ready",
        "/version",
        "/api/v1/pricing/latest",
        "/api/v1/pricing/window",
        "/api/v1/forecast/latest",
        "/api/v1/forecast/window",
        "/api/v1/metadata/zones",
        "/api/v1/metadata/reason-codes",
        "/api/v1/diagnostics/coverage/latest",
    }
    available_paths = set(schema.get("paths", {}).keys())
    missing = required_paths - available_paths
    assert not missing


def test_response_envelope_builders_include_version_and_request_fields() -> None:
    list_payload = build_list_envelope(
        api_version_path="/api/v1",
        schema_version="1.0.0",
        request_id="req-1",
        data=[],
        pagination={"page": 1, "page_size": 2, "total_count": 0, "total_pages": 0, "sort": "zone_id:asc"},
    )
    object_payload = build_object_envelope(
        api_version_path="/api/v1",
        schema_version="1.0.0",
        request_id="req-2",
        data={"ok": True},
    )

    assert list_payload["api_version"] == "v1"
    assert list_payload["schema_version"] == "1.0.0"
    assert list_payload["request_id"] == "req-1"
    assert object_payload["api_version"] == "v1"
    assert object_payload["request_id"] == "req-2"


def test_schema_version_helpers_and_breaking_change_detector() -> None:
    version_fields = build_version_fields(api_version_path="/api/v1", schema_version="1.0.0")
    assert version_fields == {"api_version": "v1", "schema_version": "1.0.0"}

    previous = {
        "paths": {"/api/v1/a": {}},
        "components": {
            "schemas": {
                "ModelA": {
                    "required": ["field_a"],
                    "properties": {"field_a": {"type": "string"}, "field_b": {"type": "number"}},
                }
            }
        },
    }
    current = {
        "paths": {},
        "components": {
            "schemas": {
                "ModelA": {
                    "required": [],
                    "properties": {"field_a": {"type": "string"}},
                }
            }
        },
    }

    findings = detect_breaking_schema_changes(previous_snapshot=previous, current_snapshot=current)
    assert any("Removed API path" in item for item in findings)
    assert any("removed required field" in item for item in findings)


def test_contract_snapshot_file_exists_and_is_valid_json() -> None:
    snapshot_path = Path("reports/api/contract_checks/latest_contract_snapshot.json")
    assert snapshot_path.exists()
    parsed = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert "paths" in parsed
    assert "components" in parsed
