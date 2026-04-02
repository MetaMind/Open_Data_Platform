-- Migration 001: Core Schema
-- Creates the foundational tables for MetaMind metadata catalog

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Tenants table for multi-tenancy
CREATE TABLE IF NOT EXISTS mm_tenants (
    tenant_id VARCHAR(64) PRIMARY KEY,
    tenant_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    settings JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT TRUE
);

-- Tables metadata
CREATE TABLE IF NOT EXISTS mm_tables (
    table_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    source_id VARCHAR(128) NOT NULL,  -- e.g., 'oracle_prod', 's3_analytics'
    source_type VARCHAR(32) NOT NULL, -- 'oracle', 'trino', 'spark', 'delta'
    schema_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    full_name VARCHAR(512) GENERATED ALWAYS AS (
        source_id || '.' || schema_name || '.' || table_name
    ) STORED,
    row_count BIGINT,
    size_bytes BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_analyzed TIMESTAMP WITH TIME ZONE,
    table_properties JSONB DEFAULT '{}',
    is_partitioned BOOLEAN DEFAULT FALSE,
    partition_columns TEXT[],
    UNIQUE(tenant_id, source_id, schema_name, table_name)
);

CREATE INDEX idx_mm_tables_tenant ON mm_tables(tenant_id);
CREATE INDEX idx_mm_tables_source ON mm_tables(source_id);
CREATE INDEX idx_mm_tables_full_name ON mm_tables(full_name);

-- Columns metadata
CREATE TABLE IF NOT EXISTS mm_columns (
    column_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_id UUID NOT NULL REFERENCES mm_tables(table_id) ON DELETE CASCADE,
    column_name VARCHAR(128) NOT NULL,
    ordinal_position INTEGER NOT NULL,
    data_type VARCHAR(128) NOT NULL,
    source_data_type VARCHAR(128),
    is_nullable BOOLEAN DEFAULT TRUE,
    is_primary_key BOOLEAN DEFAULT FALSE,
    is_foreign_key BOOLEAN DEFAULT FALSE,
    referenced_table VARCHAR(512),
    referenced_column VARCHAR(128),
    statistics JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_id, column_name)
);

CREATE INDEX idx_mm_columns_table ON mm_columns(table_id);
CREATE INDEX idx_mm_columns_name ON mm_columns(column_name);

-- Table statistics
CREATE TABLE IF NOT EXISTS mm_statistics (
    stat_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_id UUID NOT NULL REFERENCES mm_tables(table_id) ON DELETE CASCADE,
    column_id UUID REFERENCES mm_columns(column_id) ON DELETE CASCADE,
    stat_type VARCHAR(64) NOT NULL, -- 'distinct', 'null_fraction', 'min', 'max', 'histogram'
    stat_value NUMERIC,
    stat_data JSONB DEFAULT '{}',
    sample_size BIGINT,
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_id, column_id, stat_type)
);

CREATE INDEX idx_mm_statistics_table ON mm_statistics(table_id);
CREATE INDEX idx_mm_statistics_col ON mm_statistics(column_id);

-- Query logs for auditing and ML training
CREATE TABLE IF NOT EXISTS mm_query_logs (
    query_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    user_id VARCHAR(128) NOT NULL,
    session_id VARCHAR(128),
    original_sql TEXT NOT NULL,
    rewritten_sql TEXT,
    target_source VARCHAR(128),
    execution_strategy VARCHAR(32),
    
    -- Timing
    submitted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    routing_decision_ms INTEGER,
    execution_start_at TIMESTAMP WITH TIME ZONE,
    execution_end_at TIMESTAMP WITH TIME ZONE,
    total_time_ms INTEGER,
    
    -- Results
    row_count INTEGER,
    bytes_processed BIGINT,
    cache_hit BOOLEAN DEFAULT FALSE,
    
    -- ML features
    query_features JSONB DEFAULT '{}',
    predicted_cost_ms NUMERIC,
    actual_cost_ms NUMERIC,
    
    -- Status
    status VARCHAR(32) DEFAULT 'pending', -- pending, running, success, failed, cancelled
    error_message TEXT,
    
    -- Freshness
    freshness_tolerance_seconds INTEGER,
    actual_freshness_seconds INTEGER
);

CREATE INDEX idx_query_logs_tenant ON mm_query_logs(tenant_id);
CREATE INDEX idx_query_logs_user ON mm_query_logs(user_id);
CREATE INDEX idx_query_logs_submitted ON mm_query_logs(submitted_at);
CREATE INDEX idx_query_logs_status ON mm_query_logs(status);
CREATE INDEX idx_query_logs_source ON mm_query_logs(target_source);

-- Update trigger for timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_mm_tenants_updated_at BEFORE UPDATE ON mm_tenants
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_mm_tables_updated_at BEFORE UPDATE ON mm_tables
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_mm_columns_updated_at BEFORE UPDATE ON mm_columns
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
