-- Migration 003: Materialized Views
-- Tracks materialized views and their usage for query rewriting

CREATE TABLE IF NOT EXISTS mm_materialized_views (
    mv_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    -- Identification
    mv_name VARCHAR(256) NOT NULL,
    source_tables TEXT[] NOT NULL, -- ['schema.table1', 'schema.table2']
    
    -- Definition
    definition_sql TEXT NOT NULL,
    rewritten_sql TEXT, -- Optimized/rewritten version
    
    -- Storage
    storage_location VARCHAR(1024),
    storage_format VARCHAR(32) DEFAULT 'parquet',
    
    -- Statistics
    row_count BIGINT,
    size_bytes BIGINT,
    
    -- Refresh strategy
    refresh_strategy VARCHAR(32) DEFAULT 'manual', -- 'manual', 'incremental', 'full', 'realtime'
    refresh_cron VARCHAR(64), -- Cron expression for scheduled refresh
    last_refresh_at TIMESTAMP WITH TIME ZONE,
    next_refresh_at TIMESTAMP WITH TIME ZONE,
    refresh_duration_ms INTEGER,
    
    -- Incremental refresh
    incremental_column VARCHAR(128), -- Column for incremental detection
    incremental_watermark TIMESTAMP WITH TIME ZONE,
    
    -- State
    is_valid BOOLEAN DEFAULT TRUE,
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Usage tracking
    query_count INTEGER DEFAULT 0,
    last_used_at TIMESTAMP WITH TIME ZONE,
    total_time_saved_ms BIGINT DEFAULT 0,
    
    UNIQUE(tenant_id, mv_name)
);

CREATE INDEX idx_mm_mv_tenant ON mm_materialized_views(tenant_id);
CREATE INDEX idx_mm_mv_tables ON mm_materialized_views USING GIN(source_tables);
CREATE INDEX idx_mm_mv_enabled ON mm_materialized_views(is_enabled) WHERE is_enabled = TRUE;
CREATE INDEX idx_mm_mv_refresh ON mm_materialized_views(next_refresh_at);

-- MV usage log for pattern analysis
CREATE TABLE IF NOT EXISTS mm_mv_usage_log (
    usage_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    mv_id UUID NOT NULL REFERENCES mm_materialized_views(mv_id) ON DELETE CASCADE,
    query_id UUID REFERENCES mm_query_logs(query_id),
    
    -- Match details
    original_query TEXT NOT NULL,
    rewritten_query TEXT NOT NULL,
    match_confidence NUMERIC,
    
    -- Performance
    original_cost_ms NUMERIC,
    mv_cost_ms NUMERIC,
    time_saved_ms NUMERIC,
    
    used_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_mm_mv_usage_mv ON mm_mv_usage_log(mv_id);
CREATE INDEX idx_mm_mv_usage_query ON mm_mv_usage_log(query_id);
CREATE INDEX idx_mm_mv_usage_time ON mm_mv_usage_log(used_at);

-- MV refresh history
CREATE TABLE IF NOT EXISTS mm_mv_refresh_history (
    refresh_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    mv_id UUID NOT NULL REFERENCES mm_materialized_views(mv_id) ON DELETE CASCADE,
    
    refresh_type VARCHAR(32) NOT NULL, -- 'full', 'incremental', 'forced'
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_ms INTEGER,
    
    rows_before BIGINT,
    rows_after BIGINT,
    size_bytes_before BIGINT,
    size_bytes_after BIGINT,
    
    status VARCHAR(32) DEFAULT 'running', -- running, success, failed
    error_message TEXT,
    
    triggered_by VARCHAR(128) -- 'schedule', 'manual', 'cdc', 'system'
);

CREATE INDEX idx_mm_mv_refresh_mv ON mm_mv_refresh_history(mv_id);
CREATE INDEX idx_mm_mv_refresh_status ON mm_mv_refresh_history(status);
CREATE INDEX idx_mm_mv_refresh_time ON mm_mv_refresh_history(started_at);

-- Trigger to update MV usage count
CREATE OR REPLACE FUNCTION update_mv_usage_count()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE mm_materialized_views
    SET query_count = query_count + 1,
        last_used_at = NOW(),
        total_time_saved_ms = total_time_saved_ms + NEW.time_saved_ms
    WHERE mv_id = NEW.mv_id;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER trigger_update_mv_usage
    AFTER INSERT ON mm_mv_usage_log
    FOR EACH ROW EXECUTE FUNCTION update_mv_usage_count();
