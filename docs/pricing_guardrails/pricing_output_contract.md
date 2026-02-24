# Pricing Output Contract

## Table: `pricing_decisions`
Grain: one row per `pricing_run_key x zone_id x bucket_start_ts`.

Required key/timing fields:
- `zone_id`
- `bucket_start_ts`
- `pricing_created_at`
- `pricing_run_key`
- `horizon_index`
- `created_at`

Forecast provenance:
- `forecast_run_id`, `forecast_created_at`
- `y_pred`, `y_pred_lower`, `y_pred_upper`
- `confidence_score`, `uncertainty_band`
- `model_name`, `model_version`, `model_stage`, `feature_version`

Raw and guarded values:
- `baseline_expected_demand`, `baseline_reference_level`, `demand_ratio`
- `raw_multiplier`, `pre_cap_multiplier`, `post_cap_multiplier`
- `candidate_multiplier_before_rate_limit`, `final_multiplier`

Guardrail diagnostics:
- `cap_applied`, `cap_type`, `cap_reason`, `cap_value`
- `rate_limit_applied`, `rate_limit_direction`, `previous_final_multiplier`
- `smoothing_applied`, `fallback_applied`

Reasoning and policy:
- `primary_reason_code`, `reason_codes_json`, `reason_summary`
- `pricing_policy_version`

Operational fields:
- `run_id`, `status`

## Table: `pricing_run_log`
- `run_id`, `pricing_run_key`
- `started_at`, `ended_at`, `status`, `failure_reason`
- `pricing_policy_version`, `forecast_run_id`
- `target_bucket_start`, `target_bucket_end`
- `zone_count`, `row_count`, `cap_applied_count`, `rate_limited_count`, `low_confidence_count`, `latency_ms`
- `config_snapshot`, `check_summary`, `artifacts_path`

## Idempotency contract
- Deterministic run key:
  - `pricing_run_key = sha256(pricing_policy_version + forecast_run_id + target_bucket_start + target_bucket_end)[:24]`
- Unique key:
  - `(pricing_run_key, zone_id, bucket_start_ts)`
- Write strategy:
  - `INSERT ... ON CONFLICT DO UPDATE`

## Example inspection queries
```sql
SELECT zone_id, bucket_start_ts, final_multiplier, primary_reason_code
FROM pricing_decisions
WHERE pricing_run_key = :pricing_run_key
ORDER BY zone_id, bucket_start_ts;

SELECT run_id, status, row_count, cap_applied_count, rate_limited_count
FROM pricing_run_log
ORDER BY started_at DESC
LIMIT 10;
```
