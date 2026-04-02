-- MetaMind Migration 006: F01 Learned Cardinality
BEGIN;
CREATE TABLE IF NOT EXISTS mm_cardinality_feedback (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    query_id        VARCHAR(64) NOT NULL,
    table_name      VARCHAR(255) NOT NULL,
    predicates_json TEXT NOT NULL,
    estimated_rows  BIGINT NOT NULL,
    actual_rows     BIGINT NOT NULL,
    estimation_error FLOAT NOT NULL,
    model_version   INT DEFAULT 0,
    collected_at    TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mm_card_fb_tenant ON mm_cardinality_feedback(tenant_id, table_name);
CREATE INDEX IF NOT EXISTS idx_mm_card_fb_time ON mm_cardinality_feedback(collected_at DESC);

CREATE TABLE IF NOT EXISTS mm_learned_models (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    table_name      VARCHAR(255) NOT NULL,
    model_version   INT NOT NULL DEFAULT 1,
    model_type      VARCHAR(50) NOT NULL DEFAULT 'xgboost',
    model_path      TEXT NOT NULL,
    feature_names   JSONB DEFAULT '[]',
    training_rows   INT DEFAULT 0,
    mae             FLOAT DEFAULT 0.0,
    mse             FLOAT DEFAULT 0.0,
    trained_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    is_active       BOOLEAN DEFAULT TRUE,
    UNIQUE(tenant_id, table_name, model_version)
);
CREATE INDEX IF NOT EXISTS idx_mm_models_tenant ON mm_learned_models(tenant_id, table_name, is_active);
COMMIT;
