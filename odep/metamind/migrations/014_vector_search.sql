-- MetaMind Migration 014: F19 Vector Search
BEGIN;
CREATE TABLE IF NOT EXISTS mm_vector_indexes (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    table_name      VARCHAR(255) NOT NULL,
    column_name     VARCHAR(255) NOT NULL,
    index_type      VARCHAR(50) NOT NULL,  -- ivfflat, hnsw, ivf_sq8
    dimensions      INT NOT NULL,
    backend         VARCHAR(64) NOT NULL,  -- pgvector, lance, duckdb
    index_params    JSONB DEFAULT '{}',
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id, table_name, column_name, backend)
);
COMMIT;
