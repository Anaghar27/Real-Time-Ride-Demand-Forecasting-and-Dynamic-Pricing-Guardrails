# Market Evaluation Query Pack

This guide explains how to evaluate whether Phase 6 pricing is market-reasonable after validating technical correctness. The SQL pack is retrospective and diagnostic: it helps quantify signal quality, guardrail intensity, and fairness slices. It does not claim causal impact because demand response to multiplier changes is not explicitly modeled in these queries. Use these metrics as gating diagnostics before policy promotion.

## File
- `sql/pricing_guardrails/market_evaluation_queries.sql`

## Run commands
If `psql` is not installed locally, run via Docker:

```bash
cat sql/pricing_guardrails/market_evaluation_queries.sql \
  | docker compose --env-file .env -f infra/docker-compose.yml exec -T postgres \
      psql -U ride_user -d ride_demand
```

Or with Make:

```bash
make pricing-evaluate
```

## What each section answers
1. Latest run context: confirms run scope and guardrail counters.
2. Join coverage: confirms pricing rows can be matched to realized demand.
3. Forecast error vs baseline: compares `y_pred` accuracy against baseline reference.
4. Lift proxies: compares final/raw/no-surge revenue index proxies.
5. Customer shock: reports multiplier step-change percentiles and demand-weighted shock.
6. Fairness slices: compares cap/rate-limit behavior by zone class.
7. Top shock zones: highlights where multiplier movement was strongest.
8. Bucket trend: shows per-bucket demand and pricing intensity.
9. Realized fare exposure: optional sanity check using historical fare totals.

## Suggested acceptance bands (initial)
- Realized join coverage: `>= 0.95`
- Forecast WAPE improvement vs baseline: `> 0.00`
- Demand-weighted absolute multiplier delta: track trend down after tuning
- Rate-limited row rate: avoid persistent `> 0.70`
- Missing baseline fallback rate: avoid persistent `> 0.20`
- Zone-class fairness: p90 final multiplier gap between classes should be explainable and policy-approved

## Interpretation notes
- High `RATE_LIMIT_INCREASE_CLAMP` share means raw signals are frequently above allowed ramp speed.
- High `MISSING_BASELINE_REFERENCE_FALLBACK` means baseline coverage should be improved.
- High cap usage with low forecast error often indicates policy is stricter than demand signal strength.
- Improve one policy dial at a time and compare latest-run metrics before/after.
