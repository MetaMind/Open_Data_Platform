-- Migration 027: Query Firewall Rules
-- Persistent storage for firewall rules (Redis is the hot cache).

CREATE TABLE IF NOT EXISTS mm_firewall_rules (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT        NOT NULL,
    fingerprint TEXT        NOT NULL,
    list_type   TEXT        NOT NULL CHECK (list_type IN ('deny', 'allow')),
    created_by  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, fingerprint, list_type)
);

CREATE INDEX IF NOT EXISTS idx_firewall_rules_tenant
    ON mm_firewall_rules (tenant_id, list_type);

COMMENT ON TABLE mm_firewall_rules IS
    'Persistent query fingerprint firewall rules; hot cache maintained in Redis.';
