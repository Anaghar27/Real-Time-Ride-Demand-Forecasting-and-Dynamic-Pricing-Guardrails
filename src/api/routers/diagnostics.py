# This file defines lightweight diagnostics endpoints under the versioned API path.
# It exists so operators can quickly inspect coverage, guardrail usage, and confidence distributions.
# Each endpoint returns a typed envelope with version and request tracing metadata.
# Keeping these routes separate helps preserve clear API boundaries for observability use cases.

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request

from src.api.api_config import ApiConfig
from src.api.dependencies import get_config, get_diagnostics_service
from src.api.response_envelope import build_object_envelope
from src.api.schemas.diagnostics_schemas import (
    ConfidenceSummaryResponseV1,
    CoverageSummaryResponseV1,
    GuardrailUsageSummaryResponseV1,
)
from src.api.services.diagnostics_service import DiagnosticsService

router = APIRouter(prefix="/diagnostics", tags=["diagnostics"])
DiagnosticsServiceDep = Annotated[DiagnosticsService, Depends(get_diagnostics_service)]
ConfigDep = Annotated[ApiConfig, Depends(get_config)]


@router.get("/coverage/latest", response_model=CoverageSummaryResponseV1)
def diagnostics_coverage_latest(
    request: Request,
    service: DiagnosticsServiceDep,
    config: ConfigDep,
) -> dict[str, object]:
    summary = service.get_latest_coverage_summary()
    return build_object_envelope(
        api_version_path=config.api_version_path,
        schema_version=config.schema_version,
        request_id=request.state.request_id,
        data=summary,
    )


@router.get("/guardrails/latest", response_model=GuardrailUsageSummaryResponseV1)
def diagnostics_guardrails_latest(
    request: Request,
    service: DiagnosticsServiceDep,
    config: ConfigDep,
) -> dict[str, object]:
    summary = service.get_latest_guardrail_summary()
    return build_object_envelope(
        api_version_path=config.api_version_path,
        schema_version=config.schema_version,
        request_id=request.state.request_id,
        data=summary,
    )


@router.get("/confidence/latest", response_model=ConfidenceSummaryResponseV1)
def diagnostics_confidence_latest(
    request: Request,
    service: DiagnosticsServiceDep,
    config: ConfigDep,
) -> dict[str, object]:
    summary = service.get_latest_confidence_summary()
    return build_object_envelope(
        api_version_path=config.api_version_path,
        schema_version=config.schema_version,
        request_id=request.state.request_id,
        data=summary,
    )
