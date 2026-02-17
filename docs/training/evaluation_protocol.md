# Evaluation Protocol

## Leakage Safeguards
- Chronological split with train < validation < test windows.
- Optional temporal gap between windows to reduce bleed from rolling statistics.
- Test window is never used during tuning.

## Metrics
- MAE
- RMSE
- WAPE
- sMAPE (zero-safe denominator)

## Slice Analysis
- Peak vs off-peak hours
- Weekday vs weekend
- Robust vs sparse zones

## Comparison Rules
- Every candidate uses the same features, split boundaries, and metric definitions.
- Hyperparameter search uses validation only.
- Final leaderboard uses test window metrics.

## Reproducibility
- Config snapshots and split manifest persisted per run.
- MLflow logs params, metrics, tags, and artifacts for every model run.
