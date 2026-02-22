# Forecast Table Contract

## `demand_forecast`
Grain: **`forecast_run_key × zone_id × bucket_start_ts`**

Purpose: stable, queryable forecast outputs for downstream pricing guardrails.

Key columns:
- `forecast_run_key`: deterministic hash of `(model_version, forecast_start_ts, horizon_buckets)`
- `zone_id`, `bucket_start_ts`: where the demand is forecasted
- `forecast_created_at`: when the forecast was generated
- `horizon_index`: 1..N within the forecast window
- `y_pred`, `y_pred_lower`, `y_pred_upper`: point forecast and interval bounds (nonnegative)
- `confidence_score` in `[0,1]`, `uncertainty_band` in `{low, medium, high}`
- `model_name`, `model_version`, `model_stage`, `feature_version`, `run_id`
- `scoring_window_start`, `scoring_window_end`: the observed-history window used to build lag/rolling features

Idempotency:
- Primary key is `(forecast_run_key, zone_id, bucket_start_ts)`
- Reruns for the same `forecast_run_key` perform an **upsert** (update in place)

## `scoring_run_log`
One row per scoring run.

Contains:
- window (`forecast_start_ts`, `forecast_end_ts`, `horizon_buckets`)
- model info (`model_name`, `model_version`, `model_stage`)
- performance metadata (`latency_ms`, row counts)
- `status` and `failure_reason`
- `config_snapshot` (JSON) for auditability

## `confidence_reference`
Reference quantiles used to build prediction intervals.

Grain: `segment_key × hour_of_day`

Columns:
- `q50_abs_error`, `q90_abs_error`, `q95_abs_error`
- `updated_at`, `source_window`

## Common queries
See `sql/scoring/scoring_queries.sql`.

