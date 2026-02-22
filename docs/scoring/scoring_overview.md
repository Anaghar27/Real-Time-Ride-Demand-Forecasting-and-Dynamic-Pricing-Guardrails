# Phase 5 Scoring Overview

## Goal
Generate **future 15-minute ride-demand forecasts** (`pickup_count`) per `zone_id` for a configurable horizon and make them available to downstream dynamic pricing guardrails.

Phase 5 is operational by design:
- **Scheduled** runs via Prefect (deployment + worker).
- **Ad-hoc** runs via Make/CLI for debugging and backfills.
- **Idempotent** writes so reruns update the same forecast window instead of duplicating rows.

## Inputs
- **Model**: MLflow Model Registry champion selected by `model_name` + `model_stage` (default `Staging`).
- **Features (history)**: `fact_demand_features` provides observed `pickup_count` and acts as the authoritative history source.
- **Sparse-zone policy (optional)**: `zone_fallback_policy` is used to adjust confidence. If missing, scoring continues with a warning.

## Outputs
- Postgres contract tables:
  - `demand_forecast` (one row per `zone_id × bucket_start_ts` within the forecast window)
  - `scoring_run_log` (one row per scoring run)
  - `confidence_reference` (quantiles used for prediction intervals)
- Run artifacts under `reports/scoring/<run_id>/`:
  - `forecast_sample.csv`
  - `coverage_summary.csv`
  - `run_summary.json`
  - `confidence_diagnostics.png`

## Time logic
- `scoring_created_at`: the time the job starts (UTC).
- `forecast_start_ts`: **the next 15-minute bucket boundary** at or after `scoring_created_at`.
- `forecast_end_ts`: `forecast_start_ts + horizon_buckets × 15 minutes` (end-exclusive).

## Leakage safety
Forecast features are built with a strict rule:
- future calendar features are derived from the forecast timestamps
- lag/rolling features use **observed historical pickup counts** only
- for multi-step horizons, a **recursive strategy** is used where previous-step predictions are fed into lag/rolling inputs when required (flagged per row)

## Scheduling with Prefect
1. Register/update the deployment:
   - `make score-schedule` (first portion)
2. Start a worker (blocks):
   - `make score-schedule` (continues into worker start)

Concurrency is limited at the work-queue level and a Postgres advisory lock prevents overlap.

