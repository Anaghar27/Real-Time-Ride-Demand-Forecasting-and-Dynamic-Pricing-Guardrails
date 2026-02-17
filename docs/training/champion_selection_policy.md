# Champion Selection Policy

## Priority Metric
Primary metric is `WAPE` on the untouched chronological test window.

## Gate Rules
1. Candidate must beat best baseline by configured minimum percentage.
2. Candidate must not exceed sparse-zone WAPE regression threshold.
3. Candidate stability standard deviation across rolling folds must stay below policy threshold.
4. Candidate latency per prediction row must remain below configured ceiling.
5. MLflow run metadata must be complete (run id, split, feature version, policy version).

## Failure Behavior
If any gate rule fails, no champion is selected and registration is blocked.

## Determinism
For fixed input dataset, fixed split policy, and fixed seeds, gate verdict must be deterministic.
