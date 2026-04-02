-- MetaMind Migration 016: Feature Flags Registry
BEGIN;
CREATE TABLE IF NOT EXISTS mm_feature_flags (
    tenant_id       VARCHAR(64) PRIMARY KEY,
    flags           JSONB DEFAULT '{}',
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Insert default flags for system tenant
INSERT INTO mm_feature_flags(tenant_id, flags)
VALUES ('__system__', '{}')
ON CONFLICT (tenant_id) DO NOTHING;
COMMIT;
