-- MetaMind Migration 015: F28 NL Query Mappings
BEGIN;
CREATE TABLE IF NOT EXISTS mm_nl_query_mappings (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    nl_text         TEXT NOT NULL,
    nl_hash         VARCHAR(64) NOT NULL,
    generated_sql   TEXT NOT NULL,
    was_verified    BOOLEAN DEFAULT FALSE,
    was_corrected   BOOLEAN DEFAULT FALSE,
    corrected_sql   TEXT,
    model_version   VARCHAR(50),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mm_nl_tenant ON mm_nl_query_mappings(tenant_id, nl_hash);
COMMIT;
