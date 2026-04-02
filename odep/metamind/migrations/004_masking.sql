-- Migration 004: Data Masking and Security
-- Column-level security and masking rules

CREATE TABLE IF NOT EXISTS mm_masking_policies (
    policy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    policy_name VARCHAR(128) NOT NULL,
    policy_description TEXT,
    
    -- Masking function
    masking_type VARCHAR(64) NOT NULL, -- 'full', 'partial', 'hash', 'null', 'custom'
    masking_function TEXT, -- SQL expression for custom masking
    
    -- Parameters
    show_first_n INTEGER, -- For partial: show first N chars
    show_last_n INTEGER,  -- For partial: show last N chars
    mask_char VARCHAR(1) DEFAULT '*',
    
    -- Conditions
    applies_to_roles TEXT[], -- NULL = applies to all
    exempt_roles TEXT[], -- Roles that see unmasked data
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(tenant_id, policy_name)
);

CREATE INDEX idx_mm_masking_tenant ON mm_masking_policies(tenant_id);
CREATE INDEX idx_mm_masking_active ON mm_masking_policies(is_active) WHERE is_active = TRUE;

-- Column masking rules
CREATE TABLE IF NOT EXISTS mm_masking_rules (
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    policy_id UUID NOT NULL REFERENCES mm_masking_policies(policy_id) ON DELETE CASCADE,
    
    -- Target
    table_id UUID REFERENCES mm_tables(table_id) ON DELETE CASCADE,
    column_id UUID REFERENCES mm_columns(column_id) ON DELETE CASCADE,
    
    -- Can also specify by pattern
    table_pattern VARCHAR(256), -- e.g., '*.customers'
    column_pattern VARCHAR(128), -- e.g., '*email*', '*ssn*', '*password*'
    
    -- Priority (higher = applied first)
    priority INTEGER DEFAULT 5,
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(table_id, column_id, policy_id)
);

CREATE INDEX idx_mm_mask_rules_policy ON mm_masking_rules(policy_id);
CREATE INDEX idx_mm_mask_rules_table ON mm_masking_rules(table_id);
CREATE INDEX idx_mm_mask_rules_col ON mm_masking_rules(column_id);

-- Data classification tags
CREATE TABLE IF NOT EXISTS mm_data_classification (
    classification_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    column_id UUID NOT NULL REFERENCES mm_columns(column_id) ON DELETE CASCADE,
    
    -- Classification
    sensitivity_level VARCHAR(32) NOT NULL, -- 'public', 'internal', 'confidential', 'restricted'
    data_category VARCHAR(64), -- 'pii', 'phi', 'financial', 'credentials'
    
    -- Auto-detection
    detected_by VARCHAR(64), -- 'manual', 'regex', 'ml', 'dlp'
    detection_confidence NUMERIC,
    detection_pattern VARCHAR(256),
    
    -- Review
    reviewed_by VARCHAR(128),
    reviewed_at TIMESTAMP WITH TIME ZONE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(column_id)
);

CREATE INDEX idx_mm_classification_col ON mm_data_classification(column_id);
CREATE INDEX idx_mm_classification_level ON mm_data_classification(sensitivity_level);
CREATE INDEX idx_mm_classification_category ON mm_data_classification(data_category);

-- Audit log for sensitive data access
CREATE TABLE IF NOT EXISTS mm_security_audit_log (
    audit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id),
    
    event_type VARCHAR(64) NOT NULL, -- 'query', 'export', 'mask_bypass', 'policy_change'
    user_id VARCHAR(128) NOT NULL,
    user_roles TEXT[],
    
    -- Resource accessed
    resource_type VARCHAR(64), -- 'table', 'column', 'policy'
    resource_id UUID,
    resource_name VARCHAR(512),
    
    -- Access details
    query_id UUID REFERENCES mm_query_logs(query_id),
    masked_columns TEXT[],
    unmasked_columns TEXT[], -- If user has exemption
    
    -- Context
    client_ip INET,
    user_agent TEXT,
    session_id VARCHAR(128),
    
    event_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Risk assessment
    risk_score INTEGER, -- 0-100
    requires_review BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_mm_audit_tenant ON mm_security_audit_log(tenant_id);
CREATE INDEX idx_mm_audit_user ON mm_security_audit_log(user_id);
CREATE INDEX idx_mm_audit_event ON mm_security_audit_log(event_type);
CREATE INDEX idx_mm_audit_time ON mm_security_audit_log(event_time);
CREATE INDEX idx_mm_audit_risk ON mm_security_audit_log(risk_score) WHERE risk_score > 70;

-- Row-level security policies (for future use)
CREATE TABLE IF NOT EXISTS mm_rls_policies (
    rls_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    policy_name VARCHAR(128) NOT NULL,
    table_id UUID NOT NULL REFERENCES mm_tables(table_id) ON DELETE CASCADE,
    
    -- Filter expression
    filter_column VARCHAR(128) NOT NULL,
    filter_operator VARCHAR(32) NOT NULL, -- '=', 'IN', 'LIKE', etc.
    filter_value TEXT,
    
    -- Applies to
    applies_to_roles TEXT[],
    applies_to_users TEXT[],
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(tenant_id, policy_name, table_id)
);

CREATE INDEX idx_mm_rls_tenant ON mm_rls_policies(tenant_id);
CREATE INDEX idx_mm_rls_table ON mm_rls_policies(table_id);
