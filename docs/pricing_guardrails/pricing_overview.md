# Pricing Guardrails Overview

## Purpose
Phase 6 turns Phase 5 demand forecasts into operational pricing decisions with bounded risk and full auditability. The pipeline computes raw multipliers from forecast signals, then applies policy guardrails before publishing final multipliers to Postgres. Every row is explainable with reason codes and every run is tracked in a run-log table with artifacts.

## Pipeline flow
1. Load policy configuration and validate required keys.
2. Select forecast rows from `demand_forecast` using latest run, explicit run, or explicit window mode.
3. Build baseline demand references by `zone_id x day_of_week x quarter_hour_index` with fallback hierarchy.
4. Compute raw multipliers using configured mapping method.
5. Apply cap/floor guardrails.
6. Apply rate limiting and optional smoothing against last published pricing outputs.
7. Generate reason codes and summaries.
8. Run critical quality checks.
9. Persist idempotent outputs to `pricing_decisions` and `pricing_run_log`.
10. Write run artifacts under `reports/pricing_guardrails/<run_id>/`.

## Key guarantees
- Decisions are computed from forecasting outputs, never from training/test datasets.
- Guardrails are fully config-driven from YAML policy files.
- Writes are rerun-safe via deterministic `pricing_run_key` and upsert semantics.
- Invalid outputs are blocked before write when strict checks are enabled.
- Audit fields and reason codes support dashboarding and policy review.

## Main commands
- `make pricing-load-policy`
- `make pricing-run`
- `make pricing-run-window PRICE_FORECAST_START_TS='2025-11-03T00:00:00+00:00' PRICE_FORECAST_END_TS='2025-11-03T01:00:00+00:00'`
- `make pricing-validate`
- `make pricing-run-all`
- `make pricing-schedule`
