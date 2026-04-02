-- Migration 009: CDC Status and Freshness Tracking
-- Tracks data replication lag between OLTP and OLAP

CREATE TABLE IF NOT EXISTS mm_cdc_status (
    status_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    -- Source identification
    source_id VARCHAR(128) NOT NULL, -- e.g., 'oracle_prod_orders', 's3_iceberg_orders'
    table_name VARCHAR(256) NOT NULL,
    
    -- CDC position
    last_cdc_timestamp TIMESTAMP WITH TIME ZONE,
    last_s3_timestamp TIMESTAMP WITH TIME ZONE,
    
    -- Lag metrics
    lag_seconds INTEGER,
    messages_behind INTEGER DEFAULT 0,
    
    -- Kafka offsets
    kafka_topic VARCHAR(256),
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    
    -- Processing
    last_processed_at TIMESTAMP WITH TIME ZONE,
    processing_rate_per_second NUMERIC,
    
    -- Health
    is_healthy BOOLEAN DEFAULT TRUE,
    health_status VARCHAR(32) DEFAULT 'healthy', -- 'healthy', 'warning', 'critical', 'stalled'
    
    -- Metadata
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(tenant_id, source_id, table_name)
);

CREATE INDEX idx_mm_cdc_tenant ON mm_cdc_status(tenant_id);
CREATE INDEX idx_mm_cdc_table ON mm_cdc_status(table_name);
CREATE INDEX idx_mm_cdc_health ON mm_cdc_status(health_status);
CREATE INDEX idx_mm_cdc_lag ON mm_cdc_status(lag_seconds);

-- CDC events log (for debugging and replay)
CREATE TABLE IF NOT EXISTS mm_cdc_events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL,
    
    -- Event details
    source_table VARCHAR(256) NOT NULL,
    event_type VARCHAR(16) NOT NULL, -- 'insert', 'update', 'delete'
    
    -- Position
    cdc_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    scn NUMBER, -- Oracle System Change Number
    
    -- Key for deduplication
    event_key VARCHAR(256), -- Primary key value or unique identifier
    
    -- Data (optional, for debugging)
    before_data JSONB,
    after_data JSONB,
    
    -- Processing
    processed_at TIMESTAMP WITH TIME ZONE,
    processing_status VARCHAR(32) DEFAULT 'pending', -- pending, processed, failed, skipped
    error_message TEXT,
    
    -- Iceberg metadata
    iceberg_snapshot_id VARCHAR(64),
    iceberg_partition_path VARCHAR(512),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_mm_cdc_events_table ON mm_cdc_events(source_table);
CREATE INDEX idx_mm_cdc_events_time ON mm_cdc_events(cdc_timestamp);
CREATE INDEX idx_mm_cdc_events_status ON mm_cdc_events(processing_status);
CREATE INDEX idx_mm_cdc_events_key ON mm_cdc_events(event_key);

-- CDC connector configuration
CREATE TABLE IF NOT EXISTS mm_cdc_connectors (
    connector_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    connector_name VARCHAR(128) NOT NULL,
    connector_type VARCHAR(32) NOT NULL, -- 'debezium', 'maxwell', 'aws_dms'
    
    -- Source database
    source_database VARCHAR(128) NOT NULL,
    source_tables TEXT[] NOT NULL,
    
    -- Configuration
    config_json JSONB NOT NULL,
    
    -- Kafka settings
    kafka_topic_prefix VARCHAR(128),
    kafka_consumer_group VARCHAR(128),
    
    -- State
    is_active BOOLEAN DEFAULT TRUE,
    is_running BOOLEAN DEFAULT FALSE,
    
    -- Metrics
    events_captured_total BIGINT DEFAULT 0,
    events_failed_total BIGINT DEFAULT 0,
    
    last_started_at TIMESTAMP WITH TIME ZONE,
    last_stopped_at TIMESTAMP WITH TIME ZONE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(tenant_id, connector_name)
);

CREATE INDEX idx_mm_cdc_conn_tenant ON mm_cdc_connectors(tenant_id);
CREATE INDEX idx_mm_cdc_conn_active ON mm_cdc_connectors(is_active) WHERE is_active = TRUE;

-- Freshness requirements by query pattern
CREATE TABLE IF NOT EXISTS mm_freshness_requirements (
    requirement_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    -- Pattern matching
    table_pattern VARCHAR(256), -- e.g., '*.orders'
    query_pattern VARCHAR(256), -- Regex for query matching
    
    -- Freshness requirement
    freshness_seconds INTEGER NOT NULL,
    
    -- Source preference
    preferred_source VARCHAR(32), -- 'oracle', 's3', 'auto'
    
    -- Business context
    description TEXT,
    business_unit VARCHAR(128),
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_mm_fresh_tenant ON mm_freshness_requirements(tenant_id);
CREATE INDEX idx_mm_fresh_active ON mm_freshness_requirements(is_active) WHERE is_active = TRUE;

-- Function to update CDC health status based on lag
CREATE OR REPLACE FUNCTION update_cdc_health_status()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.lag_seconds IS NULL THEN
        NEW.health_status := 'stalled';
        NEW.is_healthy := FALSE;
    ELSIF NEW.lag_seconds <= 300 THEN -- 5 minutes
        NEW.health_status := 'healthy';
        NEW.is_healthy := TRUE;
    ELSIF NEW.lag_seconds <= 600 THEN -- 10 minutes
        NEW.health_status := 'warning';
        NEW.is_healthy := TRUE;
    ELSIF NEW.lag_seconds <= 1800 THEN -- 30 minutes
        NEW.health_status := 'critical';
        NEW.is_healthy := FALSE;
    ELSE
        NEW.health_status := 'stalled';
        NEW.is_healthy := FALSE;
    END IF;
    
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER trigger_update_cdc_health
    BEFORE INSERT OR UPDATE OF lag_seconds ON mm_cdc_status
    FOR EACH ROW EXECUTE FUNCTION update_cdc_health_status();

-- Insert default tenant
INSERT INTO mm_tenants (tenant_id, tenant_name, settings)
VALUES ('default', 'Default Tenant', '{}')
ON CONFLICT (tenant_id) DO NOTHING;
