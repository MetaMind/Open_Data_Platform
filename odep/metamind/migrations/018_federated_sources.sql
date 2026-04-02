-- Migration 007: Federated Sources
-- Cross-cloud and external data source management

CREATE TABLE IF NOT EXISTS mm_federated_sources (
    source_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    -- Source identification
    source_name VARCHAR(128) NOT NULL,
    source_alias VARCHAR(64), -- Short alias for queries
    
    -- Cloud/Platform
    platform VARCHAR(32) NOT NULL, -- 'aws', 'gcp', 'azure', 'snowflake', 'databricks'
    region VARCHAR(32),
    
    -- Connection
    connection_type VARCHAR(32) NOT NULL, -- 'jdbc', 'odbc', 'rest', 'native'
    connection_string TEXT,
    connection_params JSONB DEFAULT '{}', -- Encrypted credentials reference
    
    -- Security
    auth_type VARCHAR(32) NOT NULL, -- 'iam', 'oauth', 'basic', 'token', 'kerberos'
    credential_vault_ref VARCHAR(256), -- Reference to secrets manager
    
    -- Capabilities
    supports_pushdown BOOLEAN DEFAULT TRUE,
    supported_operations TEXT[], -- ['select', 'join', 'aggregate', 'filter']
    
    -- Cost
    cost_per_tb_scanned NUMERIC, -- Cost per TB for cost-based optimization
    cost_per_query NUMERIC,
    
    -- State
    is_active BOOLEAN DEFAULT TRUE,
    is_reachable BOOLEAN DEFAULT TRUE,
    last_health_check_at TIMESTAMP WITH TIME ZONE,
    health_check_status VARCHAR(32),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(tenant_id, source_name)
);

CREATE INDEX idx_mm_fed_src_tenant ON mm_federated_sources(tenant_id);
CREATE INDEX idx_mm_fed_src_platform ON mm_federated_sources(platform);
CREATE INDEX idx_mm_fed_src_active ON mm_federated_sources(is_active) WHERE is_active = TRUE;

-- Federated source tables
CREATE TABLE IF NOT EXISTS mm_federated_tables (
    fed_table_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID NOT NULL REFERENCES mm_federated_sources(source_id) ON DELETE CASCADE,
    
    -- Table info
    remote_schema VARCHAR(128) NOT NULL,
    remote_table VARCHAR(128) NOT NULL,
    local_alias VARCHAR(128),
    
    -- Statistics (may be stale)
    estimated_row_count BIGINT,
    estimated_size_bytes BIGINT,
    column_statistics JSONB,
    
    -- Sync
    last_synced_at TIMESTAMP WITH TIME ZONE,
    sync_frequency VARCHAR(32), -- 'realtime', 'hourly', 'daily', 'manual'
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(source_id, remote_schema, remote_table)
);

CREATE INDEX idx_mm_fed_tbl_source ON mm_federated_tables(source_id);

-- Cross-cloud query execution log
CREATE TABLE IF NOT EXISTS mm_federated_query_log (
    fq_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID REFERENCES mm_query_logs(query_id),
    tenant_id VARCHAR(64) NOT NULL,
    
    -- Execution details
    subquery_id VARCHAR(64), -- ID within the federated query plan
    target_source UUID REFERENCES mm_federated_sources(source_id),
    
    -- Query
    pushed_down_sql TEXT,
    local_processing_sql TEXT,
    
    -- Performance
    data_volume_bytes BIGINT,
    data_volume_rows BIGINT,
    network_transfer_bytes BIGINT,
    
    -- Timing
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_ms INTEGER,
    
    -- Cost
    estimated_cost NUMERIC,
    actual_cost NUMERIC,
    
    -- Status
    status VARCHAR(32) DEFAULT 'pending',
    error_message TEXT
);

CREATE INDEX idx_mm_fed_qry_query ON mm_federated_query_log(query_id);
CREATE INDEX idx_mm_fed_qry_source ON mm_federated_query_log(target_source);
CREATE INDEX idx_mm_fed_qry_status ON mm_federated_query_log(status);

-- Data movement tracking (for expensive cross-cloud operations)
CREATE TABLE IF NOT EXISTS mm_data_movement (
    movement_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL,
    
    -- Source and destination
    source_source_id UUID REFERENCES mm_federated_sources(source_id),
    dest_source_id UUID REFERENCES mm_federated_sources(source_id),
    
    -- What moved
    movement_type VARCHAR(32) NOT NULL, -- 'cache', 'replicate', 'temp', 'materialize'
    table_refs TEXT[],
    
    -- Location
    source_location VARCHAR(512),
    destination_location VARCHAR(512),
    
    -- Volume
    bytes_moved BIGINT,
    rows_moved BIGINT,
    files_moved INTEGER,
    
    -- Performance
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_ms INTEGER,
    throughput_mbps NUMERIC,
    
    -- Cost
    egress_cost NUMERIC,
    storage_cost NUMERIC,
    
    -- Lifecycle
    expires_at TIMESTAMP WITH TIME ZONE,
    is_expired BOOLEAN DEFAULT FALSE,
    
    triggered_by_query UUID REFERENCES mm_query_logs(query_id)
);

CREATE INDEX idx_mm_data_mov_tenant ON mm_data_movement(tenant_id);
CREATE INDEX idx_mm_data_mov_expires ON mm_data_movement(expires_at);
CREATE INDEX idx_mm_data_mov_expired ON mm_data_movement(is_expired) WHERE is_expired = FALSE;
