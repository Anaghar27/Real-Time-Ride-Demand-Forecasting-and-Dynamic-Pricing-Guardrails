# Guardrail Policy

## Raw multiplier logic
Raw multipliers are computed from forecast rows (`y_pred`, confidence fields, provenance) joined to a baseline demand reference.

Supported methods:
- `demand_ratio_piecewise`: interpolate multiplier from demand ratio breakpoints.
- `threshold_bands`: map demand ratio bands directly to fixed multipliers.

Demand ratio:

`demand_ratio = y_pred / max(baseline_expected_demand, baseline_min_value)`

Low-confidence dampening (optional):
- If `confidence_score` is below threshold or uncertainty band is configured as low confidence, the increase above floor is shrunk by dampening factor.

## Cap precedence (deterministic)
1. Floor clamp (`allow_discounting` aware).
2. Contextual cap clamp (confidence band, sparse-zone class, and optional time-category cap).
3. Global cap clamp as final hard stop.

Diagnostics:
- `cap_applied`, `cap_type`, `cap_reason`, `cap_value`, `pre_cap_multiplier`, `post_cap_multiplier`

## Rate limiter policy
Rate limiting compares candidate multipliers against the previous final multiplier for each zone:
- `max_increase_per_bucket`
- `max_decrease_per_bucket`

Cold start path:
- If no previous multiplier exists for a zone, use configured cold-start multiplier and emit a cold-start reason code.

Optional smoothing:
- EMA: `smoothed = alpha * current + (1-alpha) * previous`
- Post-smoothing clamp re-applies floor/cap bounds if needed.

Diagnostics:
- `previous_final_multiplier`, `candidate_multiplier_before_rate_limit`, `rate_limit_applied`, `rate_limit_direction`, `post_rate_limit_multiplier`, `smoothing_applied`

## Troubleshooting
- Missing forecast rows: verify `demand_forecast` coverage for target window and selection mode.
- Invalid policy config: run `make pricing-load-policy` to fail fast on schema/required keys.
- Rate limiter violations: run `make pricing-validate` and inspect `reports/pricing_guardrails/<run_id>/run_summary.json`.
- Duplicate writes: verify `pricing_run_key` inputs (`policy_version`, `forecast_run_id`, target window).
- Missing previous multipliers: expected for new zones; cold-start reason codes should appear.
