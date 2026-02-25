# Example API Requests and Responses

## Start API
```bash
make api-dev
```

## Health
```bash
curl -s http://localhost:8000/health | jq
```

Example response:
```json
{
  "api_version": "v1",
  "schema_version": "1.0.0",
  "request_id": "4d7f93f5-4e89-458f-9931-1db30770ec49",
  "status": "ok",
  "environment": "local",
  "service_name": "Ride Demand Forecast and Pricing API",
  "timestamp": "2026-02-25T18:31:00.123456+00:00"
}
```

## Pricing Latest
```bash
curl -s "http://localhost:8000/api/v1/pricing/latest?page=1&page_size=2&sort=bucket_start_ts:desc" | jq
```

## Pricing Window
```bash
curl -s "http://localhost:8000/api/v1/pricing/window?start_ts=2026-02-25T10:00:00Z&end_ts=2026-02-25T11:00:00Z&borough=Manhattan&page=1&page_size=5&sort=zone_id:asc" | jq
```

Example pricing list response:
```json
{
  "api_version": "v1",
  "schema_version": "1.0.0",
  "request_id": "2a331c9f-a72a-4ef6-a6ef-fcc7fc1746fa",
  "generated_at": "2026-02-25T18:31:10.000000+00:00",
  "data": [
    {
      "zone_id": 101,
      "bucket_start_ts": "2026-02-25T10:00:00+00:00",
      "run_id": "run-pr-1",
      "forecast_run_id": "run-fc-1",
      "final_multiplier": 1.08,
      "raw_multiplier": 1.1,
      "cap_applied": true,
      "rate_limit_applied": false,
      "confidence_score": 0.82,
      "uncertainty_band": "low",
      "primary_reason_code": "CAP_APPLIED_CONFIDENCE",
      "reason_codes": [
        "CAP_APPLIED_CONFIDENCE"
      ],
      "zone_name": "Alphabet City",
      "recommended_price_action": "Small increase",
      "why_this_price": "Small increase was recommended and then adjusted by guardrails. Cap applied due to confidence band.",
      "guardrail_note": "A pricing cap was applied due to confidence.",
      "confidence_note": "High confidence forecast with low uncertainty band."
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 2,
    "total_count": 1,
    "total_pages": 1,
    "sort": "bucket_start_ts:desc"
  },
  "warnings": null
}
```

## Forecast Latest
```bash
curl -s "http://localhost:8000/api/v1/forecast/latest?page=1&page_size=2&sort=bucket_start_ts:desc" | jq
```

Example forecast list response:
```json
{
  "api_version": "v1",
  "schema_version": "1.0.0",
  "request_id": "6774380d-4f2d-4f2f-9f7d-f4d7f14d4af4",
  "generated_at": "2026-02-25T18:31:20.000000+00:00",
  "data": [
    {
      "zone_id": 101,
      "bucket_start_ts": "2026-02-25T10:00:00+00:00",
      "forecast_run_key": "fk1",
      "run_id": "run-fc-1",
      "y_pred": 24.0,
      "y_pred_lower": 20.0,
      "y_pred_upper": 28.0,
      "confidence_score": 0.77,
      "uncertainty_band": "medium",
      "demand_outlook_label": "elevated",
      "confidence_note": "Medium confidence forecast with medium uncertainty band.",
      "forecast_range_summary": "Expected demand range is 20.00 to 28.00."
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 2,
    "total_count": 1,
    "total_pages": 1,
    "sort": "bucket_start_ts:desc"
  },
  "warnings": null
}
```

## Metadata Reason Codes
```bash
curl -s "http://localhost:8000/api/v1/metadata/reason-codes?page=1&page_size=20&sort=reason_code:asc" | jq
```

## Diagnostics Confidence
```bash
curl -s "http://localhost:8000/api/v1/diagnostics/confidence/latest" | jq
```
