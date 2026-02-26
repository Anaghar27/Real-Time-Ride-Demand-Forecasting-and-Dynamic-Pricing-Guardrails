# Dashboard Demo Script

## Goal
Show both decision support and technical observability in under 10 minutes.

## Steps
1. Open Streamlit user dashboard and set a 6-hour UTC window around the latest run.
2. On **Overview**, explain coverage, average multiplier, and guardrail counters using tooltips.
3. On **Pricing Explorer**, pick one zone and explain `raw_multiplier` vs `final_multiplier` plus reason codes.
4. On **Forecast Explorer**, show interval band and confidence distribution for the same zone.
5. On **Guardrail Transparency**, highlight cap/rate-limit rates by borough and by hour.
6. Switch to Grafana **Pipeline Overview** and show latest run status/freshness.
7. Open **Scoring Observability** and **Pricing Observability** dashboards to show trends and policy behavior.
8. If enabled, open **API Observability** and review request rate, p95 latency, and error rate.

## Closing narrative
- "The user dashboard explains what happened and why in plain language."
- "The Grafana dashboard proves system health, run freshness, and operational reliability."
