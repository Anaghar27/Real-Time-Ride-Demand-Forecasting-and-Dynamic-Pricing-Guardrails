# Fallback Policy

## Class Definitions
- robust
- medium
- sparse
- ultra_sparse

## Threshold Table
```yaml
robust:
  min_nonzero_ratio: 0.6
  min_active_days: 5
  min_coverage_ratio: 0.95
medium:
  min_nonzero_ratio: 0.3
  min_active_days: 3
  min_coverage_ratio: 0.9
sparse:
  min_nonzero_ratio: 0.1
  min_active_days: 1
  min_coverage_ratio: 0.8
ultra_sparse:
  min_nonzero_ratio: 0.0
  min_active_days: 0
  min_coverage_ratio: 0.0
```

## Examples
- robust -> zone_model
- medium -> zone_model_conservative_smoothing
- sparse -> borough_seasonal_baseline
- ultra_sparse -> city_seasonal_baseline

## Caveats
- sparse segments may require conservative monitoring windows

## Operational Guidance
- Always query zone_fallback_policy by policy_version and effective window