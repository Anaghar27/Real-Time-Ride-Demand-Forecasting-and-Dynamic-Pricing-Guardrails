# This file defines metadata endpoint schemas for zones, reason codes, policy, and schema catalog data.
# It exists so API metadata contracts are explicit for both humans and automation.
# The models keep descriptive reference data separate from forecasting and pricing rows.
# Having typed metadata responses reduces integration ambiguity for dashboard teams.

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from src.api.schemas.common import EnvelopeFields, PaginationMetadata


class ZoneMetadataRowV1(BaseModel):
    zone_id: int
    zone_name: str
    borough: str
    service_zone: str | None = None


class ZoneMetadataListResponseV1(EnvelopeFields):
    data: list[ZoneMetadataRowV1]
    pagination: PaginationMetadata


class ReasonCodeRowV1(BaseModel):
    reason_code: str
    category: str
    description: str
    active_flag: bool


class ReasonCodeListResponseV1(EnvelopeFields):
    data: list[ReasonCodeRowV1]
    pagination: PaginationMetadata


class PolicySummaryV1(BaseModel):
    policy_version: str | None = None
    effective_from: datetime | None = None
    active_flag: bool | None = None
    policy_summary: dict[str, Any] | None = None


class PolicySummaryResponseV1(EnvelopeFields):
    data: PolicySummaryV1


class SchemaEndpointMetadataV1(BaseModel):
    endpoint_path: str
    method: str
    response_model_name: str


class SchemaCatalogV1(BaseModel):
    api_version_path: str
    schema_version: str
    compatibility_policy: dict[str, list[str]]
    endpoints: list[SchemaEndpointMetadataV1]


class SchemaCatalogResponseV1(EnvelopeFields):
    data: SchemaCatalogV1
