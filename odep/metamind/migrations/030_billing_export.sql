-- Migration 030: Billing Export Tracking

CREATE TABLE IF NOT EXISTS mm_billing_exports (
    export_id       BIGSERIAL    PRIMARY KEY,
    tenant_id       TEXT         NOT NULL,
    period_start    TIMESTAMPTZ  NOT NULL,
    period_end      TIMESTAMPTZ  NOT NULL,
    total_cost_usd  NUMERIC(12,6) NOT NULL DEFAULT 0,
    export_ref      TEXT,
    exported_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_billing_exports_tenant
    ON mm_billing_exports (tenant_id, exported_at DESC);

COMMENT ON TABLE mm_billing_exports IS
    'Record of every billing period export (Stripe or CSV).';
