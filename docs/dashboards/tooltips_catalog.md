# Tooltips Catalog

| Tooltip key | Where it appears | Explanation | Example interpretation |
|---|---|---|---|
| `latest_run_coverage` | Overview run metadata caption | Shows when latest successful run completed and what it covered. | "Latest run is fresh and includes enough rows to trust summary cards." |
| `zones_covered_card` | Overview metric card | Count of unique zones present in filtered pricing data. | "Only 12 zones are included, so this is a narrow slice." |
| `avg_final_multiplier_card` | Overview metric card | Mean of final multipliers after guardrails. | "Average 1.06x means moderate uplift after policy checks." |
| `count_capped_card` | Overview metric card | Number of rows where cap guardrails intervened. | "High capped count means many raw recommendations hit policy ceilings." |
| `count_rate_limited_card` | Overview metric card | Number of rows smoothed by rate-limit logic. | "Frequent smoothing suggests volatile adjacent buckets." |
| `low_confidence_share_card` | Overview metric card | Share of low-confidence forecast rows in the selection. | "If this is high, pricing outputs should be interpreted cautiously." |
| `multiplier_distribution_chart` | Overview histogram | Displays spread and skew of final multipliers. | "Right-skewed histogram indicates more aggressive pricing windows." |
| `final_vs_raw_multiplier` | Pricing Explorer trend | Clarifies raw recommendation versus policy-adjusted final output. | "Raw was 1.22x but final is 1.15x due to cap." |
| `cap_rate_limit_flags` | Pricing Explorer table | Explains guardrail boolean columns in decision rows. | "`cap_applied=true` means policy clipped the recommendation." |
| `confidence_score` | Pricing Explorer notes | Defines confidence score meaning for interpretation. | "0.55 implies lower certainty and more conservative reading." |
| `forecast_interval_band` | Forecast Explorer line/band chart | Defines lower/upper prediction interval range. | "Demand likely lies between lower and upper band bounds." |
| `uncertainty_band` | Forecast Explorer confidence chart | Explains bucketed uncertainty labels. | "A larger low-band share signals unstable forecast confidence." |
| `confidence_conservativeness` | Forecast Explorer notes | Connects confidence quality to pricing conservativeness. | "Lower confidence often maps to safer policy outcomes downstream." |
| `cap_protection` | Guardrail Transparency notes | Explains what cap guardrails protect against. | "Caps reduce extreme price spikes during sudden demand jumps." |
| `rate_limit_protection` | Guardrail Transparency notes | Explains why multiplier change-rate controls exist. | "Rate limiting avoids abrupt bucket-to-bucket swings." |
| `reason_code_existence` | Pricing + Guardrail notes | Explains why reason codes are attached to each decision. | "Reason codes make pricing outcomes auditable and explainable." |
| `cap_by_borough_chart` | Guardrail Transparency chart | Cap-applied rate by borough. | "Boroughs with high cap rate face more cap pressure." |
| `cap_by_hour_chart` | Guardrail Transparency chart | Cap-applied rate by hour of day. | "Hours with high cap rate indicate recurring peak stress." |
| `rate_by_borough_chart` | Guardrail Transparency chart | Rate-limited rate by borough. | "High borough rate-limiting may indicate unstable local demand." |
| `rate_by_hour_chart` | Guardrail Transparency chart | Rate-limited rate by hour of day. | "High rate-limited hours indicate frequent abrupt changes." |
| `reason_code_summary_table` | Guardrail Transparency table | Count and description of reason codes in selection. | "Most rows were tagged `HIGH_DEMAND`, indicating demand-led uplift." |
