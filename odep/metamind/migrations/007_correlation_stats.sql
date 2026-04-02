-- MetaMind Migration 007: F02 Correlation Statistics
BEGIN;
CREATE TABLE IF NOT EXISTS mm_column_correlations (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    table_name      VARCHAR(255) NOT NULL,
    col_a           VARCHAR(255) NOT NULL,
    col_b           VARCHAR(255) NOT NULL,
    correlation_coef FLOAT NOT NULL DEFAULT 0.0,
    mutual_info     FLOAT NOT NULL DEFAULT 0.0,
    joint_ndv       BIGINT DEFAULT 0,
    sketch_data     BYTEA,
    bayesian_params JSONB DEFAULT '{}',
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id, table_name, col_a, col_b)
);
CREATE INDEX IF NOT EXISTS idx_mm_correlations_tenant ON mm_column_correlations(tenant_id, table_name);
COMMIT;
