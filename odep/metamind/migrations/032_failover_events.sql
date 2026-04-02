-- Migration 032: Multi-Region Failover event log
-- Fixes W-02: missing migrations for Phase 2 tables

CREATE TABLE IF NOT EXISTS mm_failover_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    original_engine TEXT NOT NULL,
    failover_engine TEXT,
    reason          TEXT,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_failover_events_tenant_ts
    ON mm_failover_events (tenant_id, occurred_at DESC);

COMMENT ON TABLE mm_failover_events IS 'Audit log of multi-region engine failover events.';
