CREATE TABLE IF NOT EXISTS confidence_reference (
    segment_key TEXT NOT NULL,
    hour_of_day SMALLINT NOT NULL,
    q50_abs_error DOUBLE PRECISION NOT NULL,
    q90_abs_error DOUBLE PRECISION NOT NULL,
    q95_abs_error DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    source_window TEXT NOT NULL,
    CONSTRAINT pk_confidence_reference PRIMARY KEY (segment_key, hour_of_day)
);

CREATE INDEX IF NOT EXISTS idx_confidence_reference_updated_at
    ON confidence_reference (updated_at DESC);

