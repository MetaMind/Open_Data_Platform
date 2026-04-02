-- Migration 034: Ensure default tenant exists
-- This prevents FK failures when query logs are written with tenant_id='default'

INSERT INTO mm_tenants (tenant_id, tenant_name, settings, is_active)
VALUES ('default', 'Default Tenant', '{}'::jsonb, TRUE)
ON CONFLICT (tenant_id) DO NOTHING;

