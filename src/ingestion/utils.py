"""
Shared ingestion utility functions.
It supports an idempotent ingestion workflow that loads raw TLC data and reference tables into Postgres.
It is typically invoked via the Phase 1 Make targets and should be safe to re-run.
"""

from __future__ import annotations

import hashlib
from pathlib import Path


def sha256sum(file_path: Path) -> str:
    """Return SHA-256 checksum for a file."""

    digest = hashlib.sha256()
    with file_path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
