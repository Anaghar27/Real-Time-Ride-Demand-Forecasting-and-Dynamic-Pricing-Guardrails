# Reason Code Catalog

## Signal
- `HIGH_DEMAND_RATIO`: Forecast demand materially exceeds baseline demand.
- `NORMAL_DEMAND_BASELINE`: Forecast demand is near baseline.
- `LOW_CONFIDENCE_DAMPENING`: Low confidence reduced raw multiplier increase.
- `BASELINE_FALLBACK_ZONE`: Zone-level baseline reference was used.
- `BASELINE_FALLBACK_BOROUGH`: Borough fallback baseline reference was used.
- `BASELINE_FALLBACK_CITY`: City fallback baseline reference was used.

## Guardrail
- `FLOOR_APPLIED`: Floor clamp changed multiplier.
- `CAP_APPLIED_GLOBAL`: Global cap clamp changed multiplier.
- `CAP_APPLIED_CONFIDENCE`: Confidence contextual cap clamp changed multiplier.
- `CAP_APPLIED_SPARSE_ZONE`: Sparse-zone contextual cap clamp changed multiplier.
- `RATE_LIMIT_INCREASE_CLAMP`: Upward step was clamped by rate limit.
- `RATE_LIMIT_DECREASE_CLAMP`: Downward step was clamped by rate limit.
- `SMOOTHING_APPLIED`: EMA smoothing applied after rate limiting.

## Fallback and data quality
- `NO_PREVIOUS_MULTIPLIER_COLD_START`: No previous published multiplier for zone.
- `SPARSE_ZONE_POLICY_ACTIVE`: Sparse-zone policy class attached to zone.
- `MISSING_BASELINE_REFERENCE_FALLBACK`: All baseline tiers missing; global fallback used.

## Output fields
- `primary_reason_code`: top-level deterministic code selected by configured priority.
- `reason_codes_json`: JSON array of all triggered codes.
- `reason_summary`: concise text built from reason descriptions.

## Query example
```sql
WITH latest_run AS (
  SELECT pricing_run_key
  FROM pricing_run_log
  WHERE status = 'succeeded'
  ORDER BY started_at DESC
  LIMIT 1
)
SELECT primary_reason_code, COUNT(*) AS rows
FROM pricing_decisions
WHERE pricing_run_key = (SELECT pricing_run_key FROM latest_run)
GROUP BY primary_reason_code
ORDER BY rows DESC;
```
