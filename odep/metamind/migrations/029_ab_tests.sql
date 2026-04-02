-- Migration 029: Query A/B Experiment Tracking

CREATE TABLE IF NOT EXISTS mm_ab_experiments (
    experiment_id  TEXT         PRIMARY KEY,
    name           TEXT         NOT NULL,
    sql_a          TEXT         NOT NULL,
    sql_b          TEXT         NOT NULL,
    tenant_id      TEXT         NOT NULL,
    sample_pct     NUMERIC(5,4) NOT NULL DEFAULT 0.1,
    status         TEXT         NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'complete', 'error')),
    result_json    TEXT,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ab_experiments_tenant
    ON mm_ab_experiments (tenant_id, created_at DESC);

COMMENT ON TABLE mm_ab_experiments IS
    'A/B query experiments comparing two SQL variants for performance and correctness.';
