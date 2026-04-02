-- Migration 002: Partition Management
-- Adds partition tracking for partitioned tables

CREATE TABLE IF NOT EXISTS mm_partitions (
    partition_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_id UUID NOT NULL REFERENCES mm_tables(table_id) ON DELETE CASCADE,
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    -- Partition identification
    partition_spec JSONB NOT NULL, -- { "year": "2024", "month": "01" }
    partition_key TEXT GENERATED ALWAYS AS (partition_spec::text) STORED,
    
    -- Storage location
    location_uri VARCHAR(1024),
    format VARCHAR(32), -- 'parquet', 'orc', 'delta', 'iceberg'
    
    -- Statistics
    row_count BIGINT,
    size_bytes BIGINT,
    file_count INTEGER,
    
    -- Lifecycle
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    modified_at TIMESTAMP WITH TIME ZONE,
    last_accessed_at TIMESTAMP WITH TIME ZONE,
    
    -- State
    is_active BOOLEAN DEFAULT TRUE,
    is_compacted BOOLEAN DEFAULT FALSE,
    compaction_time TIMESTAMP WITH TIME ZONE,
    
    -- Optimization
    min_value JSONB, -- Min values for partition columns
    max_value JSONB, -- Max values for partition columns
    null_count JSONB -- Null counts per column
);

CREATE INDEX idx_mm_partitions_table ON mm_partitions(table_id);
CREATE INDEX idx_mm_partitions_tenant ON mm_partitions(tenant_id);
CREATE INDEX idx_mm_partitions_active ON mm_partitions(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_mm_partitions_key ON mm_partitions(partition_key);

-- Partition pruning log for query optimization
CREATE TABLE IF NOT EXISTS mm_partition_pruning (
    pruning_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID REFERENCES mm_query_logs(query_id) ON DELETE CASCADE,
    table_id UUID NOT NULL REFERENCES mm_tables(table_id),
    
    -- Pruning details
    total_partitions INTEGER NOT NULL,
    pruned_partitions INTEGER NOT NULL,
    selected_partitions INTEGER NOT NULL,
    
    -- Pruning predicates
    partition_predicates JSONB, -- ["year = 2024", "month >= 1"]
    
    -- Performance
    pruning_time_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_mm_pruning_query ON mm_partition_pruning(query_id);
CREATE INDEX idx_mm_pruning_table ON mm_partition_pruning(table_id);

-- Partition recommendations for optimization
CREATE TABLE IF NOT EXISTS mm_partition_recommendations (
    recommendation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_id UUID NOT NULL REFERENCES mm_tables(table_id) ON DELETE CASCADE,
    
    recommendation_type VARCHAR(64) NOT NULL, -- 'compaction', 'archive', 'split'
    priority INTEGER DEFAULT 5, -- 1 (highest) to 10 (lowest)
    
    -- Details
    affected_partitions UUID[],
    estimated_size_bytes BIGINT,
    estimated_benefit_ms NUMERIC,
    
    -- Recommendation
    recommended_action TEXT,
    recommended_ddl TEXT,
    
    -- Status
    status VARCHAR(32) DEFAULT 'pending', -- pending, approved, rejected, completed
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- ML confidence
    confidence_score NUMERIC,
    model_version VARCHAR(32)
);

CREATE INDEX idx_mm_part_rec_table ON mm_partition_recommendations(table_id);
CREATE INDEX idx_mm_part_rec_status ON mm_partition_recommendations(status);
