-- Metadata API query pack.
-- These templates mirror the service-layer SQL used by metadata endpoints.

-- Zone catalog.
SELECT
    location_id AS zone_id,
    zone AS zone_name,
    borough,
    service_zone
FROM dim_zone
ORDER BY location_id ASC
LIMIT :limit OFFSET :offset;

-- Reason code catalog.
SELECT
    reason_code,
    category,
    description,
    active_flag
FROM reason_code_reference
ORDER BY reason_code ASC
LIMIT :limit OFFSET :offset;

-- Current policy snapshot.
SELECT
    policy_version,
    effective_from,
    active_flag,
    config_json AS policy_summary
FROM pricing_policy_snapshot
WHERE active_flag = TRUE
ORDER BY effective_from DESC, created_at DESC
LIMIT 1;
