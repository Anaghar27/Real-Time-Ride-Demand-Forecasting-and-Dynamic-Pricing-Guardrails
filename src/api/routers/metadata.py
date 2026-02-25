# This file defines metadata and reference endpoints under the versioned API path.
# It exists so consumers can fetch zone catalogs, reason codes, policy metadata, and schema info.
# List endpoints use deterministic sort and pagination behavior just like data endpoints.
# The router returns explicit envelopes to keep integration behavior predictable.

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request

from src.api.api_config import ApiConfig
from src.api.dependencies import get_config, get_metadata_service
from src.api.error_handlers import APIError
from src.api.pagination import compute_total_pages, normalize_pagination, parse_sort
from src.api.response_envelope import build_list_envelope, build_object_envelope
from src.api.schemas.common import PaginationMetadata
from src.api.schemas.metadata_schemas import (
    PolicySummaryResponseV1,
    ReasonCodeListResponseV1,
    SchemaCatalogResponseV1,
    ZoneMetadataListResponseV1,
)
from src.api.services.metadata_service import (
    REASON_SORT_FIELD_MAP,
    ZONE_SORT_FIELD_MAP,
    MetadataService,
)

router = APIRouter(prefix="/metadata", tags=["metadata"])
MetadataServiceDep = Annotated[MetadataService, Depends(get_metadata_service)]
ConfigDep = Annotated[ApiConfig, Depends(get_config)]


@router.get("/zones", response_model=ZoneMetadataListResponseV1)
def metadata_zones(
    request: Request,
    service: MetadataServiceDep,
    config: ConfigDep,
    borough: str | None = Query(default=None),
    service_zone: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int | None = Query(default=None, ge=1),
    limit: int | None = Query(default=None, ge=1),
    sort: str | None = Query(default="zone_id:asc"),
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
            default_sort="zone_id:asc",
            allowed_fields=set(ZONE_SORT_FIELD_MAP),
        )
    except ValueError as exc:
        raise APIError(
            status_code=400,
            error_code="INVALID_QUERY_PARAM",
            message=str(exc),
        ) from exc

    service_result = service.get_zones(
        borough=borough,
        service_zone=service_zone,
        page=pagination.page,
        page_size=pagination.page_size,
        sort=sort_spec,
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


@router.get("/reason-codes", response_model=ReasonCodeListResponseV1)
def metadata_reason_codes(
    request: Request,
    service: MetadataServiceDep,
    config: ConfigDep,
    category: str | None = Query(default=None),
    active_only: bool = Query(default=True),
    page: int = Query(default=1, ge=1),
    page_size: int | None = Query(default=None, ge=1),
    limit: int | None = Query(default=None, ge=1),
    sort: str | None = Query(default="reason_code:asc"),
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
            default_sort="reason_code:asc",
            allowed_fields=set(REASON_SORT_FIELD_MAP),
        )
    except ValueError as exc:
        raise APIError(
            status_code=400,
            error_code="INVALID_QUERY_PARAM",
            message=str(exc),
        ) from exc

    service_result = service.get_reason_codes(
        category=category,
        active_only=active_only,
        page=pagination.page,
        page_size=pagination.page_size,
        sort=sort_spec,
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


@router.get("/policy/current", response_model=PolicySummaryResponseV1)
def metadata_current_policy(
    request: Request,
    service: MetadataServiceDep,
    config: ConfigDep,
) -> dict[str, object]:
    return build_object_envelope(
        api_version_path=config.api_version_path,
        schema_version=config.schema_version,
        request_id=request.state.request_id,
        data=service.get_current_policy(),
    )


@router.get("/schema", response_model=SchemaCatalogResponseV1)
def metadata_schema(
    request: Request,
    service: MetadataServiceDep,
    config: ConfigDep,
) -> dict[str, object]:
    return build_object_envelope(
        api_version_path=config.api_version_path,
        schema_version=config.schema_version,
        request_id=request.state.request_id,
        data=service.get_schema_catalog(),
    )
