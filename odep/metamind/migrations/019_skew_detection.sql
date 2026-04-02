-- MetaMind Migration 008: F03 Skew Detection
BEGIN;
CREATE TABLE IF NOT EXISTS mm_skew_tracking (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    table_name      VARCHAR(255) NOT NULL,
    column_name     VARCHAR(255) NOT NULL,
    skew_ratio      FLOAT NOT NULL DEFAULT 0.0,
    top_k_values    JSONB DEFAULT '[]',   -- [{value, freq}]
    is_skewed       BOOLEAN DEFAULT FALSE,
    compensation    VARCHAR(50) DEFAULT 'none',  -- none, broadcast, salt
    analyzed_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id, table_name, column_name)
);
CREATE INDEX IF NOT EXISTS idx_mm_skew_tenant ON mm_skew_tracking(tenant_id, is_skewed);
COMMIT;
