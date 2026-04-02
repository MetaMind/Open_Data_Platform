-- Migration 033: Latency Anomaly Detector event log
-- Fixes W-02: missing migrations for Phase 2 tables

CREATE TABLE IF NOT EXISTS mm_anomaly_events (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    engine      TEXT NOT NULL,
    p99_ms      NUMERIC(12,2) NOT NULL,
    baseline_ms NUMERIC(12,2) NOT NULL,
    z_score     NUMERIC(6,2) NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomaly_events_tenant_ts
    ON mm_anomaly_events (tenant_id, detected_at DESC);

COMMENT ON TABLE mm_anomaly_events IS 'Detected latency anomaly events from Z-score detector.';
