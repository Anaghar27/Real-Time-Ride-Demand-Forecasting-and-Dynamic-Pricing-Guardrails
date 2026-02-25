# This file runs deterministic checks for plain-language mapping rules.
# It exists so required label thresholds stay aligned with business documentation.
# The script is lightweight and can run in CI or local preflight steps.
# Keeping these checks scripted prevents subtle wording regressions in API payloads.
# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.api.plain_language import guardrail_note, price_action_label


def main() -> int:
    checks = [
        (price_action_label(1.0) == "No price change", "1.00 must map to 'No price change'"),
        (price_action_label(1.05) == "Small increase", "1.01-1.08 must map to 'Small increase'"),
        (
            "cap" in guardrail_note(
                cap_applied=True,
                rate_limit_applied=False,
                cap_reason="confidence",
                cap_type="contextual",
            ).lower(),
            "cap_applied=true must mention cap behavior",
        ),
        (
            "rate" in guardrail_note(
                cap_applied=False,
                rate_limit_applied=True,
                cap_reason=None,
                cap_type=None,
            ).lower(),
            "rate_limit_applied=true must mention rate limiting",
        ),
    ]

    failures = [message for passed, message in checks if not passed]
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1

    print("Plain-language mapping checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
