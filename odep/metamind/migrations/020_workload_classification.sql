-- MetaMind Migration 009: F24 Workload Classification
BEGIN;
CREATE TABLE IF NOT EXISTS mm_workload_patterns (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    pattern_hash    VARCHAR(64) NOT NULL,
    workload_type   VARCHAR(50) NOT NULL,
    frequency       INT DEFAULT 1,
    avg_duration_ms FLOAT DEFAULT 0.0,
    avg_rows        BIGINT DEFAULT 0,
    last_seen       TIMESTAMP NOT NULL DEFAULT NOW(),
    features_json   TEXT,
    UNIQUE(tenant_id, pattern_hash)
);

CREATE TABLE IF NOT EXISTS mm_query_templates (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    template_hash   VARCHAR(64) NOT NULL,
    template_sql    TEXT NOT NULL,
    workload_type   VARCHAR(50),
    exec_count      INT DEFAULT 1,
    avg_duration_ms FLOAT DEFAULT 0.0,
    p99_duration_ms FLOAT DEFAULT 0.0,
    last_seen       TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id, template_hash)
);
CREATE INDEX IF NOT EXISTS idx_mm_templates_tenant ON mm_query_templates(tenant_id);
COMMIT;
