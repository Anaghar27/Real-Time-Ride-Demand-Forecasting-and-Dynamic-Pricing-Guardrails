# Confidence Method (Residual-Based Intervals)

## Why this exists
Pricing guardrails need to know not only *what we predict*, but also *how uncertain the prediction is*.
Phase 5 uses a simple, production-friendly method that works for any regression model: **residual-based prediction intervals**.

## Method summary
1. Build a recent **backtest window** from `fact_demand_features` (default: last 14 days).
2. Score that window with the current champion model.
3. Compute absolute errors: `abs_error = |y_true - y_pred|`.
4. Group errors by:
   - `segment_key` (from `zone_fallback_policy.sparsity_class` if available, otherwise `all`)
   - `hour_of_day`
5. Store quantiles (`q50`, `q90`, `q95`) in `confidence_reference`.

## Interval construction
For each forecast row with prediction `y_pred`:
- choose a half-width quantile `q` (default: `q95_abs_error`)
- bounds:
  - `y_pred_lower = max(0, y_pred - q)`
  - `y_pred_upper = max(0, y_pred + q)`

## Confidence score
A lightweight score in `[0,1]` is derived from the interval width:
- narrow intervals relative to the prediction → higher score
- sparse zones apply a multiplier to reduce confidence (conservative behavior)

## Diagnostics
Each scoring run writes a chart under `reports/scoring/<run_id>/confidence_diagnostics.png` showing `q95_abs_error` by hour and segment.

