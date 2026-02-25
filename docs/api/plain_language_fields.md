# Plain-Language Fields

## Overview
When `API_INCLUDE_PLAIN_LANGUAGE_FIELDS=true`, forecast and pricing rows include readable explanations for non-technical users.

## Pricing Plain-Language Fields
- `zone_name`
- `recommended_price_action`
- `why_this_price`
- `guardrail_note`
- `confidence_note`

### Deterministic Pricing Mapping Rules
- `final_multiplier == 1.00` -> `No price change`
- `1.01 <= final_multiplier <= 1.08` -> `Small increase`
- `1.08 < final_multiplier <= 1.20` -> `Moderate increase`
- `final_multiplier > 1.20` -> `Larger increase`
- `final_multiplier < 1.00` -> `Price decrease`
- `cap_applied=true` -> `guardrail_note` mentions cap behavior
- `rate_limit_applied=true` -> `guardrail_note` mentions rate limiting/smoothing

## Forecast Plain-Language Fields
- `demand_outlook_label`
- `confidence_note`
- `forecast_range_summary`

### Deterministic Forecast Mapping Rules
- `y_pred < 5` -> `low`
- `5 <= y_pred < 15` -> `normal`
- `15 <= y_pred < 30` -> `elevated`
- `y_pred >= 30` -> `high`

## Disabling Plain-Language Fields
Set:
```bash
API_INCLUDE_PLAIN_LANGUAGE_FIELDS=false
```
When disabled, machine fields remain and plain-language fields are omitted from list responses.

## Validation Command
```bash
make api-plain-language-check
```
