#!/usr/bin/env python3
"""
Download sample TLC files and taxi zone lookup into landing paths.
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

from src.ingestion.fetch import download_sample_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch sample TLC files for ingestion pilot")
    parser.add_argument(
        "--months",
        nargs="+",
        default=["2024-01", "2024-02", "2024-03"],
        help="Year-month entries (YYYY-MM) to download for pilot",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest_rows = download_sample_files(args.months)
    print(json.dumps({"files_recorded": len(manifest_rows), "months": args.months}, indent=2))


if __name__ == "__main__":
    main()
