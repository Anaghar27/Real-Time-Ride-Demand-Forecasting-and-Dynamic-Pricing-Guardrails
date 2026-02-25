# This file handles pagination and sort parsing for list endpoints.
# It exists so every router uses the same deterministic rules for page size and ordering.
# The helpers validate user input and produce stable offset/limit behavior.
# Centralizing this logic keeps endpoint code small and avoids inconsistent query semantics.

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SortSpec:
    field: str
    order: str

    @property
    def as_text(self) -> str:
        return f"{self.field}:{self.order}"


@dataclass(frozen=True)
class PaginationSpec:
    page: int
    page_size: int

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size


def normalize_pagination(
    *,
    page: int,
    page_size: int | None,
    limit: int | None,
    default_page_size: int,
    max_page_size: int,
) -> PaginationSpec:
    """Validate and normalize page/page_size values."""

    resolved_page_size = limit if limit is not None else page_size
    if resolved_page_size is None:
        resolved_page_size = default_page_size
    if page < 1:
        raise ValueError("page must be >= 1")
    if resolved_page_size < 1:
        raise ValueError("page_size must be >= 1")
    if resolved_page_size > max_page_size:
        raise ValueError(f"page_size must be <= {max_page_size}")
    return PaginationSpec(page=page, page_size=resolved_page_size)


def parse_sort(
    *,
    requested_sort: str | None,
    default_sort: str,
    allowed_fields: set[str],
) -> SortSpec:
    """Parse sort input in the form `field:asc|desc`."""

    raw_sort = (requested_sort or default_sort).strip().lower()
    if not raw_sort:
        raise ValueError("sort cannot be empty")

    if ":" in raw_sort:
        field, order = raw_sort.split(":", 1)
    else:
        field, order = raw_sort, "asc"

    if field not in allowed_fields:
        supported = ", ".join(sorted(allowed_fields))
        raise ValueError(f"Unsupported sort field '{field}'. Supported fields: {supported}")
    if order not in {"asc", "desc"}:
        raise ValueError("sort order must be 'asc' or 'desc'")
    return SortSpec(field=field, order=order)


def compute_total_pages(*, total_count: int, page_size: int) -> int:
    """Compute deterministic total page count."""

    if total_count <= 0:
        return 0
    return ((total_count - 1) // page_size) + 1
