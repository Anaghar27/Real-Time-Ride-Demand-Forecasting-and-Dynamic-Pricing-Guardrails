# User Dashboard Guide

## Purpose
The Streamlit dashboard is the decision-support surface for non-technical users. It explains what price is recommended, why guardrails changed it, and how much forecast uncertainty is present.

## Data sources
- Primary source: Phase 7 API (`/api/v1/*`) for plain-language fields like `why_this_price`, `guardrail_note`, and `confidence_note`.
- Fallback source: direct Postgres reads from `pricing_decisions`, `demand_forecast`, `pricing_run_log`, `scoring_run_log`, `dim_zone`, and `reason_code_reference`.
- Single interface: `src/dashboard_user/data_access.py` handles API-first routing and DB fallback.

## Run the dashboard
```bash
make dashboard-user-run
```

For fast iteration:
```bash
make dashboard-user-dev
```

## Navigation and filters
- Pages:
  - Overview
  - Pricing Explorer
  - Forecast Explorer
  - Guardrail Transparency
- Sidebar filters:
  - Start/end UTC timestamps
  - Borough
  - Zone
  - Confidence band
  - `cap_applied` / `rate_limit_applied` / low-confidence toggles
  - Latest pricing run mode (default) or specific `run_id`

## Caching and load control
- Streamlit cache is used for zone catalog, reason code catalog, latest run metadata, and query windows.
- Query page size is bounded by config defaults and max limits.
- Data-access TTL cache adds a second protection layer when users switch tabs quickly.

## Graceful empty and partial behavior
- Empty windows show informative messages, not stack traces.
- If only one of pricing or forecast datasets is present, a partial-coverage warning is shown.

## Testing
```bash
make dashboard-user-test
```
