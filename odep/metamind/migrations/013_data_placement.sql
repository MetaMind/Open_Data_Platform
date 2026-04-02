-- MetaMind Migration 013: F16 Data Placement
BEGIN;
CREATE TABLE IF NOT EXISTS mm_placement_recommendations (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    table_name      VARCHAR(255) NOT NULL,
    current_backend VARCHAR(64) NOT NULL,
    recommended_backend VARCHAR(64) NOT NULL,
    reason          TEXT,
    estimated_savings FLOAT DEFAULT 0.0,
    confidence      FLOAT DEFAULT 0.0,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    applied_at      TIMESTAMP,
    is_applied      BOOLEAN DEFAULT FALSE
);
COMMIT;
