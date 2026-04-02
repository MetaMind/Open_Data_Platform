-- Migration 031: SLA Enforcement Engine tables
-- Fixes W-02: missing migrations for Phase 2 tables

CREATE TABLE IF NOT EXISTS mm_sla_configs (
    id            SERIAL PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    p50_target_ms NUMERIC(10,2) NOT NULL DEFAULT 500,
    p95_target_ms NUMERIC(10,2) NOT NULL DEFAULT 2000,
    p99_target_ms NUMERIC(10,2) NOT NULL DEFAULT 8000,
    breach_action TEXT NOT NULL DEFAULT 'reroute'
                  CHECK (breach_action IN ('reroute', 'queue', 'alert')),
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_sla_configs_tenant
    ON mm_sla_configs (tenant_id)
    WHERE is_active = TRUE;

CREATE TABLE IF NOT EXISTS mm_sla_decisions (
    id              BIGSERIAL PRIMARY KEY,
    query_id        TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    original_engine TEXT NOT NULL,
    final_engine    TEXT NOT NULL,
    risk_level      TEXT NOT NULL CHECK (risk_level IN ('safe', 'at_risk', 'breach')),
    reason          TEXT,
    decided_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sla_decisions_tenant_ts
    ON mm_sla_decisions (tenant_id, decided_at DESC);

COMMENT ON TABLE mm_sla_configs   IS 'Per-tenant SLA latency budget targets.';
COMMENT ON TABLE mm_sla_decisions IS 'Audit log of every SLA enforcement decision.';
