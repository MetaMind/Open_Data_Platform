-- MetaMind Migration 012: F15 Federated MVs
BEGIN;
CREATE TABLE IF NOT EXISTS mm_federated_mvs (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    mv_name         VARCHAR(255) NOT NULL,
    source_backend  VARCHAR(64) NOT NULL,
    target_backend  VARCHAR(64) NOT NULL,
    source_query    TEXT NOT NULL,
    sync_type       VARCHAR(50) DEFAULT 'full',  -- full, incremental
    sync_schedule   VARCHAR(100),
    last_synced     TIMESTAMP,
    sync_lag_seconds INT DEFAULT 0,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id, mv_name)
);
COMMIT;
