"""Dataset fetch utilities for NYC TLC ingestion."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from src.ingestion.utils import sha256sum

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


@dataclass(frozen=True)
class SourceFile:
    source_name: str
    url: str
    destination: Path


def build_sample_sources(months: list[str]) -> list[SourceFile]:
    """Build source descriptors for selected year-month strings."""

    sources: list[SourceFile] = []
    for year_month in months:
        year, month = year_month.split("-")
        filename = f"yellow_tripdata_{year_month}.parquet"
        destination = Path("data/landing/tlc") / f"year={year}" / f"month={month}" / filename
        sources.append(
            SourceFile(
                source_name="yellow_taxi",
                url=f"{TLC_BASE_URL}/{filename}",
                destination=destination,
            )
        )

    sources.append(
        SourceFile(
            source_name="taxi_zone_lookup",
            url=ZONE_LOOKUP_URL,
            destination=Path("data/landing/reference/taxi_zone_lookup.csv"),
        )
    )
    return sources


def download_month_file(year_month: str) -> Path:
    """Ensure a single TLC month file exists in landing path and return it."""

    month_source = build_sample_sources([year_month])[0]
    if not month_source.destination.exists():
        _download_file(month_source.url, month_source.destination)
    return month_source.destination


def _download_file(url: str, destination: Path, timeout_seconds: int = 60) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    with destination.open("wb") as file_obj:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                file_obj.write(chunk)


def _load_manifest(manifest_path: Path) -> dict[str, dict[str, Any]]:
    if not manifest_path.exists():
        return {}

    entries: dict[str, dict[str, Any]] = {}
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        key = f"{payload['source_name']}::{payload['file_path']}::{payload['checksum']}"
        entries[key] = payload
    return entries


def download_sample_files(months: list[str]) -> list[dict[str, Any]]:
    """Download sample files and update immutable manifest rows."""

    manifest_path = Path("data/landing/manifest.jsonl")
    existing = _load_manifest(manifest_path)
    discovered: list[dict[str, Any]] = []

    for source in build_sample_sources(months):
        if not source.destination.exists():
            _download_file(source.url, source.destination)

        checksum = sha256sum(source.destination)
        size = source.destination.stat().st_size
        payload = {
            "source_name": source.source_name,
            "file_path": str(source.destination),
            "checksum": checksum,
            "file_size": size,
            "discovered_at": datetime.now(tz=UTC).isoformat(),
        }
        key = f"{payload['source_name']}::{payload['file_path']}::{payload['checksum']}"
        if key not in existing:
            existing[key] = payload
            discovered.append(payload)

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as file_obj:
        for record in sorted(existing.values(), key=lambda item: (item["source_name"], item["file_path"])):
            file_obj.write(json.dumps(record) + "\n")

    return list(existing.values())
