#!/usr/bin/env python3
"""
Enforce Phase 1 gate before historical backfill.
It packages a repeatable workflow so development tasks can be executed consistently.
Run it directly or via `make`, and expect it to print progress and exit non-zero on failure.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.db import engine
from src.ingestion.ddl import apply_ingestion_ddl
from src.ingestion.gate import evaluate_phase1_gate


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check whether Phase 1 gate is satisfied")
    parser.add_argument("--min-successful-batches", type=int, default=2)
    parser.add_argument("--skip-tests", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    apply_ingestion_ddl(engine)

    passed, details = evaluate_phase1_gate(
        engine,
        min_successful_batches=args.min_successful_batches,
        run_tests=not args.skip_tests,
    )
    print(json.dumps({"gate_passed": passed, "details": details}, indent=2))

    if not passed:
        print("Phase 1 gate failed. Step 1.6 is locked until 1.1-1.5 are healthy.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
