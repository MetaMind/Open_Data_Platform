-- MetaMind Migration 010: F23 Cloud Budgets
BEGIN;
CREATE TABLE IF NOT EXISTS mm_cloud_budgets (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL UNIQUE,
    monthly_limit   FLOAT NOT NULL DEFAULT 10000.0,
    current_spend   FLOAT NOT NULL DEFAULT 0.0,
    period_start    DATE NOT NULL DEFAULT CURRENT_DATE,
    alert_threshold FLOAT DEFAULT 0.8,   -- alert at 80% of budget
    enforcement     BOOLEAN DEFAULT FALSE,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mm_query_costs (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    query_id        VARCHAR(64) NOT NULL,
    backend         VARCHAR(64) NOT NULL,
    estimated_cost  FLOAT DEFAULT 0.0,
    actual_cost     FLOAT,
    bytes_scanned   BIGINT DEFAULT 0,
    slots_consumed  FLOAT DEFAULT 0.0,
    executed_at     TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mm_costs_tenant ON mm_query_costs(tenant_id, executed_at DESC);
COMMIT;
