-- MetaMind Migration 011: F20 Regret Minimization
BEGIN;
CREATE TABLE IF NOT EXISTS mm_optimization_decisions (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    query_id        VARCHAR(64) NOT NULL,
    decision_type   VARCHAR(100) NOT NULL,  -- join_order, index_choice, engine_routing
    chosen_option   TEXT NOT NULL,
    alternatives    JSONB DEFAULT '[]',
    predicted_cost  FLOAT NOT NULL,
    actual_cost     FLOAT,
    regret          FLOAT DEFAULT 0.0,
    decided_at      TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mm_decisions_tenant ON mm_optimization_decisions(tenant_id, decision_type);

CREATE TABLE IF NOT EXISTS mm_regret_scores (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    rule_name       VARCHAR(100) NOT NULL,
    cumulative_regret FLOAT DEFAULT 0.0,
    weight          FLOAT DEFAULT 1.0,
    update_count    INT DEFAULT 0,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id, rule_name)
);
COMMIT;
