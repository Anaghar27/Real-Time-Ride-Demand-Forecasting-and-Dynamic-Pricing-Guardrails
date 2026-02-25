# This file defines pricing data endpoints under the versioned API path.
# It exists so downstream systems can query latest decisions, windows, and run summaries safely.
# The router enforces deterministic pagination and allowlisted sorting for repeatable results.
# Responses include machine fields and optional plain-language explanations from the service layer.

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request

from src.api.api_config import ApiConfig
from src.api.dependencies import get_config, get_pricing_service
from src.api.error_handlers import APIError
from src.api.pagination import compute_total_pages, normalize_pagination, parse_sort
from src.api.response_envelope import build_list_envelope, build_object_envelope
from src.api.schemas.common import PaginationMetadata
from src.api.schemas.pricing_schemas import (
    PricingDecisionListResponseV1,
    PricingRunSummaryResponseV1,
)
from src.api.services.pricing_service import PRICING_SORT_FIELD_MAP, PricingService

router = APIRouter(prefix="/pricing", tags=["pricing"])
PricingServiceDep = Annotated[PricingService, Depends(get_pricing_service)]
ConfigDep = Annotated[ApiConfig, Depends(get_config)]


@router.get(
    "/latest",
    response_model=PricingDecisionListResponseV1,
    response_model_exclude_none=True,
)
def pricing_latest(
    request: Request,
    service: PricingServiceDep,
    config: ConfigDep,
    zone_id: int | None = Query(default=None),
    borough: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1),
    page: int = Query(default=1, ge=1),
    page_size: int | None = Query(default=None, ge=1),
    sort: str | None = Query(default=None),
) -> dict[str, object]:
    try:
        pagination = normalize_pagination(
            page=page,
            page_size=page_size,
            limit=limit,
            default_page_size=config.default_page_size,
            max_page_size=config.max_page_size,
        )
        sort_spec = parse_sort(
            requested_sort=sort,
            default_sort=config.default_sort_order,
            allowed_fields=set(PRICING_SORT_FIELD_MAP),
        )
    except ValueError as exc:
        raise APIError(
            status_code=400,
            error_code="INVALID_QUERY_PARAM",
            message=str(exc),
        ) from exc

    service_result = service.get_latest_pricing(
        zone_id=zone_id,
        borough=borough,
        page=pagination.page,
        page_size=pagination.page_size,
        sort=sort_spec,
        include_plain_language_fields=config.include_plain_language_fields,
    )

    pagination_meta = PaginationMetadata(
        page=pagination.page,
        page_size=pagination.page_size,
        total_count=int(service_result["total_count"]),
        total_pages=compute_total_pages(
            total_count=int(service_result["total_count"]),
            page_size=pagination.page_size,
        ),
        sort=sort_spec.as_text,
    )

    return build_list_envelope(
        api_version_path=config.api_version_path,
        schema_version=config.schema_version,
        request_id=request.state.request_id,
        data=list(service_result["rows"]),
        pagination=pagination_meta.model_dump(),
        warnings=service_result.get("warnings"),
    )


@router.get(
    "/window",
    response_model=PricingDecisionListResponseV1,
    response_model_exclude_none=True,
)
def pricing_window(
    request: Request,
    service: PricingServiceDep,
    config: ConfigDep,
    start_ts: datetime,
    end_ts: datetime,
    zone_id: int | None = Query(default=None),
    borough: str | None = Query(default=None),
    uncertainty_band: str | None = Query(default=None),
    cap_applied: bool | None = Query(default=None),
    rate_limit_applied: bool | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int | None = Query(default=None, ge=1),
    limit: int | None = Query(default=None, ge=1),
    sort: str | None = Query(default=None),
) -> dict[str, object]:
    if start_ts > end_ts:
        raise APIError(
            status_code=400,
            error_code="INVALID_TIME_WINDOW",
            message="start_ts must be less than or equal to end_ts.",
        )

    try:
        pagination = normalize_pagination(
            page=page,
            page_size=page_size,
            limit=limit,
            default_page_size=config.default_page_size,
            max_page_size=config.max_page_size,
        )
        sort_spec = parse_sort(
            requested_sort=sort,
            default_sort=config.default_sort_order,
            allowed_fields=set(PRICING_SORT_FIELD_MAP),
        )
    except ValueError as exc:
        raise APIError(
            status_code=400,
            error_code="INVALID_QUERY_PARAM",
            message=str(exc),
        ) from exc

    service_result = service.get_pricing_window(
        start_ts=start_ts,
        end_ts=end_ts,
        zone_id=zone_id,
        borough=borough,
        uncertainty_band=uncertainty_band,
        cap_applied=cap_applied,
        rate_limit_applied=rate_limit_applied,
        run_id=None,
        page=pagination.page,
        page_size=pagination.page_size,
        sort=sort_spec,
        include_plain_language_fields=config.include_plain_language_fields,
    )

    pagination_meta = PaginationMetadata(
        page=pagination.page,
        page_size=pagination.page_size,
        total_count=int(service_result["total_count"]),
        total_pages=compute_total_pages(
            total_count=int(service_result["total_count"]),
            page_size=pagination.page_size,
        ),
        sort=sort_spec.as_text,
    )

    return build_list_envelope(
        api_version_path=config.api_version_path,
        schema_version=config.schema_version,
        request_id=request.state.request_id,
        data=list(service_result["rows"]),
        pagination=pagination_meta.model_dump(),
        warnings=service_result.get("warnings"),
    )


@router.get(
    "/zone/{zone_id}",
    response_model=PricingDecisionListResponseV1,
    response_model_exclude_none=True,
)
def pricing_zone_timeline(
    request: Request,
    service: PricingServiceDep,
    config: ConfigDep,
    zone_id: int,
    start_ts: datetime,
    end_ts: datetime,
    page: int = Query(default=1, ge=1),
    page_size: int | None = Query(default=None, ge=1),
    limit: int | None = Query(default=None, ge=1),
    sort: str | None = Query(default=None),
) -> dict[str, object]:
    if start_ts > end_ts:
        raise APIError(
            status_code=400,
            error_code="INVALID_TIME_WINDOW",
            message="start_ts must be less than or equal to end_ts.",
        )

    try:
        pagination = normalize_pagination(
            page=page,
            page_size=page_size,
            limit=limit,
            default_page_size=config.default_page_size,
            max_page_size=config.max_page_size,
        )
        sort_spec = parse_sort(
            requested_sort=sort,
            default_sort=config.default_sort_order,
            allowed_fields=set(PRICING_SORT_FIELD_MAP),
        )
    except ValueError as exc:
        raise APIError(
            status_code=400,
            error_code="INVALID_QUERY_PARAM",
            message=str(exc),
        ) from exc

    service_result = service.get_zone_timeline(
        zone_id=zone_id,
        start_ts=start_ts,
        end_ts=end_ts,
        page=pagination.page,
        page_size=pagination.page_size,
        sort=sort_spec,
        include_plain_language_fields=config.include_plain_language_fields,
    )

    pagination_meta = PaginationMetadata(
        page=pagination.page,
        page_size=pagination.page_size,
        total_count=int(service_result["total_count"]),
        total_pages=compute_total_pages(
            total_count=int(service_result["total_count"]),
            page_size=pagination.page_size,
        ),
        sort=sort_spec.as_text,
    )

    return build_list_envelope(
        api_version_path=config.api_version_path,
        schema_version=config.schema_version,
        request_id=request.state.request_id,
        data=list(service_result["rows"]),
        pagination=pagination_meta.model_dump(),
        warnings=service_result.get("warnings"),
    )


@router.get("/runs/latest", response_model=PricingRunSummaryResponseV1)
def pricing_runs_latest(
    request: Request,
    service: PricingServiceDep,
    config: ConfigDep,
) -> dict[str, object]:
    summary = service.get_latest_run_summary()
    if summary is None:
        raise APIError(
            status_code=404,
            error_code="RUN_NOT_FOUND",
            message="No pricing run metadata found.",
        )

    return build_object_envelope(
        api_version_path=config.api_version_path,
        schema_version=config.schema_version,
        request_id=request.state.request_id,
        data=summary,
    )


@router.get("/runs/{run_id}", response_model=PricingRunSummaryResponseV1)
def pricing_run_by_id(
    run_id: str,
    request: Request,
    service: PricingServiceDep,
    config: ConfigDep,
) -> dict[str, object]:
    summary = service.get_run_summary(run_id=run_id)
    if summary is None:
        raise APIError(
            status_code=404,
            error_code="RUN_NOT_FOUND",
            message=f"Pricing run_id not found: {run_id}",
        )

    return build_object_envelope(
        api_version_path=config.api_version_path,
        schema_version=config.schema_version,
        request_id=request.state.request_id,
        data=summary,
    )
