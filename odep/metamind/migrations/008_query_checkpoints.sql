-- Migration 008: Query Checkpoints and Re-optimization
-- Mid-query re-optimization support

CREATE TABLE IF NOT EXISTS mm_query_checkpoints (
    checkpoint_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID NOT NULL REFERENCES mm_query_logs(query_id) ON DELETE CASCADE,
    tenant_id VARCHAR(64) NOT NULL,
    
    -- Checkpoint info
    checkpoint_number INTEGER NOT NULL,
    checkpoint_name VARCHAR(128),
    
    -- State
    operator_id VARCHAR(64), -- ID of operator being checkpointed
    operator_type VARCHAR(64),
    
    -- Statistics at checkpoint
    rows_processed BIGINT,
    bytes_processed BIGINT,
    cardinality_actual BIGINT,
    cardinality_estimated BIGINT,
    
    -- Deviation detection
    deviation_ratio NUMERIC, -- actual / estimated
    deviation_significant BOOLEAN DEFAULT FALSE,
    
    -- State storage
    state_location VARCHAR(512), -- S3 path or memory reference
    state_size_bytes BIGINT,
    
    -- Timing
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    recovery_time_ms INTEGER,
    
    UNIQUE(query_id, checkpoint_number)
);

CREATE INDEX idx_mm_chkpt_query ON mm_query_checkpoints(query_id);
CREATE INDEX idx_mm_chkpt_deviation ON mm_query_checkpoints(deviation_significant) 
    WHERE deviation_significant = TRUE;

-- Re-optimization decisions
CREATE TABLE IF NOT EXISTS mm_reoptimization_log (
    reopt_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID NOT NULL REFERENCES mm_query_logs(query_id),
    checkpoint_id UUID REFERENCES mm_query_checkpoints(checkpoint_id),
    
    -- Trigger
    trigger_type VARCHAR(64) NOT NULL, -- 'cardinality_deviation', 'resource_pressure', 'timeout', 'manual'
    trigger_details JSONB,
    
    -- Original plan
    original_plan JSONB NOT NULL,
    original_cost_estimate NUMERIC,
    
    -- New plan
    new_plan JSONB,
    new_cost_estimate NUMERIC,
    
    -- Decision
    reoptimized BOOLEAN DEFAULT FALSE,
    reason TEXT,
    
    -- Performance
    reoptimization_time_ms INTEGER,
    
    -- Impact
    actual_improvement_ms NUMERIC,
    actual_improvement_percent NUMERIC,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_mm_reopt_query ON mm_reoptimization_log(query_id);
CREATE INDEX idx_mm_reopt_trigger ON mm_reoptimization_log(trigger_type);

-- Adaptive statistics updates
CREATE TABLE IF NOT EXISTS mm_adaptive_stats (
    stat_update_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL,
    
    -- What was updated
    table_id UUID REFERENCES mm_tables(table_id),
    column_id UUID REFERENCES mm_columns(column_id),
    stat_type VARCHAR(64) NOT NULL,
    
    -- Update details
    previous_value NUMERIC,
    new_value NUMERIC,
    update_reason VARCHAR(64), -- 'feedback', 'sample', 'checkpoint', 'manual'
    
    -- Source
    source_query_id UUID REFERENCES mm_query_logs(query_id),
    confidence NUMERIC, -- Confidence in the update
    
    -- When
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Impact tracking
    queries_benefited INTEGER DEFAULT 0,
    avg_improvement_percent NUMERIC
);

CREATE INDEX idx_mm_adapt_stats_tenant ON mm_adaptive_stats(tenant_id);
CREATE INDEX idx_mm_adapt_stats_table ON mm_adaptive_stats(table_id);
CREATE INDEX idx_mm_adapt_stats_time ON mm_adaptive_stats(updated_at);

-- Query plan alternatives (for plan comparison)
CREATE TABLE IF NOT EXISTS mm_query_plan_alternatives (
    alt_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID NOT NULL REFERENCES mm_query_logs(query_id) ON DELETE CASCADE,
    
    alternative_number INTEGER NOT NULL,
    plan_type VARCHAR(32) NOT NULL, -- 'original', 'reoptimized', 'fallback'
    
    -- Plan details
    plan_json JSONB NOT NULL,
    estimated_cost NUMERIC,
    estimated_cardinality BIGINT,
    
    -- Physical properties
    target_engine VARCHAR(32),
    partition_strategy VARCHAR(32),
    join_order TEXT[],
    
    -- Selection
    was_selected BOOLEAN DEFAULT FALSE,
    selection_reason TEXT,
    
    UNIQUE(query_id, alternative_number)
);

CREATE INDEX idx_mm_plan_alt_query ON mm_query_plan_alternatives(query_id);
CREATE INDEX idx_mm_plan_alt_selected ON mm_query_plan_alternatives(was_selected) WHERE was_selected = TRUE;
