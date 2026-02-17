# Model Card Template

## Model Identity
- Model name:
- Version:
- Registry stage:
- Run id:

## Training Data Contract
- Source table: `fact_demand_features`
- Grain: `zone_id x bucket_start_ts` at 15-minute frequency
- Target: `pickup_count`
- Feature version:
- Policy version:

## Split Strategy
- Train interval:
- Validation interval:
- Test interval:
- Temporal gap:
- Leakage checks passed:

## Global Metrics
- MAE:
- RMSE:
- WAPE:
- sMAPE:

## Slice Metrics
- Peak hours:
- Off-peak hours:
- Weekday:
- Weekend:
- Robust zones:
- Sparse zones:

## Risks and Constraints
- Sparse-zone degradation risk:
- Data freshness constraints:
- Fallback dependency:

## Rollback Plan
- Last stable model version:
- Rollback command:
