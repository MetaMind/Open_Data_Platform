-- Migration 010: Routing Policies and Tenant Quotas
-- Adds tables for routing policy management and tenant resource isolation

-- Routing policies table
CREATE TABLE IF NOT EXISTS mm_routing_policies (
    policy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    policy_type VARCHAR(32) NOT NULL, -- 'cost_based', 'freshness_based', 'load_balanced', 'custom'
    name VARCHAR(128) NOT NULL,
    description TEXT,
    priority INTEGER DEFAULT 5, -- Higher = evaluated first
    
    -- Policy rules (JSON)
    rules JSONB DEFAULT '{}',
    
    -- Conditions for policy to match (JSON)
    conditions JSONB DEFAULT '{}',
    
    -- Target configuration
    target_engine VARCHAR(64) NOT NULL,
    fallback_engine VARCHAR(64),
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(tenant_id, name)
);

CREATE INDEX idx_mm_routing_policies_tenant ON mm_routing_policies(tenant_id);
CREATE INDEX idx_mm_routing_policies_active ON mm_routing_policies(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_mm_routing_policies_priority ON mm_routing_policies(priority DESC);

-- Tenant quotas table
CREATE TABLE IF NOT EXISTS mm_tenant_quotas (
    quota_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL UNIQUE REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    -- Query limits
    max_concurrent_queries INTEGER DEFAULT 10,
    max_queries_per_minute INTEGER DEFAULT 100,
    max_queries_per_hour INTEGER DEFAULT 1000,
    
    -- Resource limits
    max_rows_per_query INTEGER DEFAULT 100000,
    max_bytes_per_query BIGINT DEFAULT 1073741824, -- 1GB
    max_execution_time_seconds INTEGER DEFAULT 300,
    
    -- Cost limits
    max_cost_per_query NUMERIC DEFAULT 1000.0,
    
    -- Cache limits
    cache_quota_mb INTEGER DEFAULT 100,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_mm_tenant_quotas_tenant ON mm_tenant_quotas(tenant_id);

-- Insert default quotas for existing tenants
INSERT INTO mm_tenant_quotas (tenant_id)
SELECT tenant_id FROM mm_tenants
ON CONFLICT (tenant_id) DO NOTHING;

-- Create default routing policies
INSERT INTO mm_routing_policies (
    tenant_id, policy_type, name, description, priority,
    rules, conditions, target_engine, fallback_engine
)
VALUES (
    'default',
    'freshness_based',
    'Realtime Queries to Oracle',
    'Route queries requiring real-time data to Oracle',
    100,
    '{}',
    '{"max_freshness_seconds": 0}',
    'oracle',
    's3'
)
ON CONFLICT (tenant_id, name) DO NOTHING;

INSERT INTO mm_routing_policies (
    tenant_id, policy_type, name, description, priority,
    rules, conditions, target_engine, fallback_engine
)
VALUES (
    'default',
    'cost_based',
    'Large Aggregations to Spark',
    'Route large aggregation queries to Spark',
    90,
    '{}',
    '{"max_estimated_rows": 1000000}',
    'spark',
    'trino'
)
ON CONFLICT (tenant_id, name) DO NOTHING;

-- Trigger to update timestamps
CREATE TRIGGER update_mm_routing_policies_updated_at 
    BEFORE UPDATE ON mm_routing_policies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_mm_tenant_quotas_updated_at 
    BEFORE UPDATE ON mm_tenant_quotas
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
