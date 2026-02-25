# Response Contracts

## Envelope Format
List endpoints return a common envelope:
- `api_version`
- `schema_version`
- `request_id`
- `generated_at`
- `data`
- `pagination`
- `warnings` (optional)

Object endpoints return:
- `api_version`
- `schema_version`
- `request_id`
- `generated_at`
- `data`
- `warnings` (optional)

## Error Format
All errors return:
- `error_code`
- `message`
- `details` (optional)
- `request_id`
- `timestamp`

## Core Response Models
- `HealthResponse`
- `ReadinessResponse`
- `VersionResponse`
- `PricingDecisionRowV1`
- `PricingDecisionListResponseV1`
- `ForecastRowV1`
- `ForecastListResponseV1`
- `ZoneMetadataRowV1`
- `ReasonCodeRowV1`
- `PricingRunSummaryV1`
- `ForecastRunSummaryV1`

## Empty Data Behavior
- List endpoints return `200` with `data: []` when filters match no rows.
- Run lookup endpoints return `404` when the requested `run_id` does not exist.

## Validation Error Behavior
- Invalid query parameters return `400` or `422` with structured error payloads.
- Invalid windows (`start_ts > end_ts`) return `400` with `error_code=INVALID_TIME_WINDOW`.
