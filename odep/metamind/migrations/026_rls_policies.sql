-- Migration 026: Row-Level Security Policies
-- Creates the mm_rls_policies table used by RLSRewriter.

CREATE TABLE IF NOT EXISTS mm_rls_policies (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT        NOT NULL,
    table_name  TEXT        NOT NULL,
    filter_expr TEXT        NOT NULL,
    roles       TEXT[]      NOT NULL DEFAULT '{}',
    is_active   BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rls_policies_tenant
    ON mm_rls_policies (tenant_id, is_active);

CREATE INDEX IF NOT EXISTS idx_rls_policies_table
    ON mm_rls_policies (table_name);

COMMENT ON TABLE mm_rls_policies IS
    'Per-tenant row-level security filter expressions applied by RLSRewriter.';

COMMENT ON COLUMN mm_rls_policies.filter_expr IS
    'SQL WHERE clause fragment; may reference :tenant_id and :user_role tokens.';

COMMENT ON COLUMN mm_rls_policies.roles IS
    'Array of role names this policy applies to; empty = applies to all roles.';
