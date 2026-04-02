-- MetaMind Migration 012 — Cloud Budget Tracking (F23)
-- Applies after: 011_synthesis.sql

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------------------------------------------------
-- mm_budget_configs — per-tenant budget limits and alert thresholds
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mm_budget_configs (
    budget_id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id),
    budget_name         VARCHAR(128) NOT NULL,
    budget_limit_usd    NUMERIC(12,4) NOT NULL CHECK (budget_limit_usd > 0),
    billing_cycle       VARCHAR(16) DEFAULT 'monthly'
                        CHECK (billing_cycle IN ('daily','weekly','monthly')),
    alert_threshold_pct INTEGER DEFAULT 80
                        CHECK (alert_threshold_pct BETWEEN 1 AND 100),
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(tenant_id, budget_name)
);

-- -----------------------------------------------------------------------
-- mm_query_costs — per-query cost attribution for billing and enforcement
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mm_query_costs (
    cost_id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           VARCHAR(64) NOT NULL,
    query_id            VARCHAR(128) NOT NULL,
    engine              VARCHAR(64) NOT NULL,
    estimated_cost_usd  NUMERIC(10,6),
    actual_cost_usd     NUMERIC(10,6),
    rows_processed      BIGINT,
    bytes_processed     BIGINT,
    execution_time_ms   INTEGER,
    billed_at           TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------
-- mm_budget_alerts — fired alert records for audit and deduplication
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mm_budget_alerts (
    alert_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id       VARCHAR(64) NOT NULL,
    budget_id       UUID NOT NULL REFERENCES mm_budget_configs(budget_id),
    alert_type      VARCHAR(32) NOT NULL DEFAULT 'threshold_breach',
    threshold_pct   INTEGER NOT NULL,
    current_spend   NUMERIC(12,4) NOT NULL,
    budget_limit    NUMERIC(12,4) NOT NULL,
    pct_used        NUMERIC(5,2) NOT NULL,
    is_resolved     BOOLEAN DEFAULT FALSE,
    fired_at        TIMESTAMPTZ DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

-- -----------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_costs_tenant_time
    ON mm_query_costs(tenant_id, billed_at DESC);

CREATE INDEX IF NOT EXISTS idx_costs_engine
    ON mm_query_costs(engine, billed_at DESC);

CREATE INDEX IF NOT EXISTS idx_budget_tenant
    ON mm_budget_configs(tenant_id)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_budget_alerts_tenant
    ON mm_budget_alerts(tenant_id, fired_at DESC);

-- Trigger to keep updated_at current on mm_budget_configs
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_budget_configs_updated_at'
    ) THEN
        CREATE TRIGGER trg_budget_configs_updated_at
            BEFORE UPDATE ON mm_budget_configs
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END
$$;
