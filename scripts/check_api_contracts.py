# This file checks API schema contracts against the committed snapshot.
# It exists so breaking response changes are detected before release.
# The script writes both an updated snapshot and a human-readable diff report.
# Failing early here protects backward compatibility commitments for API consumers.
# ruff: noqa: E402

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.api.api_config import get_api_config
from src.api.app import app
from src.api.schema_versions import detect_breaking_schema_changes

SNAPSHOT_PATH = Path("reports/api/contract_checks/latest_contract_snapshot.json")
DIFF_REPORT_PATH = Path("reports/api/contract_checks/contract_diff_report.md")


def _openapi_snapshot() -> dict[str, object]:
    config = get_api_config()
    openapi_schema = app.openapi()
    return {
        "api_version_path": config.api_version_path,
        "schema_version": config.schema_version,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "paths": openapi_schema.get("paths", {}),
        "components": openapi_schema.get("components", {}),
    }


def main() -> int:
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    current = _openapi_snapshot()

    previous: dict[str, object] | None = None
    if SNAPSHOT_PATH.exists():
        previous = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))

    breaking_findings: list[str] = []
    if previous is not None:
        breaking_findings = detect_breaking_schema_changes(
            previous_snapshot=previous,
            current_snapshot=current,
        )

    DIFF_REPORT_PATH.write_text(
        _build_report(
            previous=previous,
            current=current,
            findings=breaking_findings,
        ),
        encoding="utf-8",
    )

    SNAPSHOT_PATH.write_text(
        json.dumps(current, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if previous is None:
        print("No previous API snapshot found. A new baseline snapshot was created.")
        return 0

    previous_version = str(previous.get("api_version_path", ""))
    current_version = str(current.get("api_version_path", ""))
    if breaking_findings and previous_version == current_version:
        print("Breaking contract changes detected without API path version bump:")
        for item in breaking_findings:
            print(f"- {item}")
        return 1

    if breaking_findings:
        print("Breaking changes detected but API version path changed. Review report.")
    else:
        print("No breaking API contract changes detected.")
    return 0


def _build_report(
    *,
    previous: dict[str, object] | None,
    current: dict[str, object],
    findings: list[str],
) -> str:
    lines: list[str] = [
        "# API Contract Diff Report",
        "",
        f"Generated at: {datetime.now(tz=UTC).isoformat()}",
        "",
        f"Current API version path: `{current.get('api_version_path')}`",
        f"Current schema version: `{current.get('schema_version')}`",
        "",
    ]

    if previous is None:
        lines.extend(
            [
                "## Status",
                "",
                "No previous snapshot existed. This run created the initial baseline.",
            ]
        )
        return "\n".join(lines) + "\n"

    lines.append("## Breaking Change Findings")
    lines.append("")
    if not findings:
        lines.append("No breaking differences were detected.")
    else:
        lines.extend([f"- {item}" for item in findings])

    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
