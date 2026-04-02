-- Migration 005: Metadata Versioning
-- Tracks schema changes and metadata versioning

CREATE TABLE IF NOT EXISTS mm_metadata_versions (
    version_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    -- Version info
    version_number INTEGER NOT NULL,
    version_type VARCHAR(32) NOT NULL, -- 'schema', 'statistics', 'partition', 'mv'
    
    -- What changed
    entity_type VARCHAR(64) NOT NULL, -- 'table', 'column', 'partition', 'mv'
    entity_id UUID NOT NULL,
    entity_name VARCHAR(512),
    
    -- Change details
    change_type VARCHAR(32) NOT NULL, -- 'create', 'alter', 'drop', 'refresh'
    change_summary TEXT,
    change_details JSONB,
    
    -- Before/after
    previous_state JSONB,
    new_state JSONB,
    
    -- Tracking
    changed_by VARCHAR(128),
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- For rollback
    rollback_sql TEXT,
    is_rollbackable BOOLEAN DEFAULT FALSE,
    
    UNIQUE(tenant_id, version_type, version_number)
);

CREATE INDEX idx_mm_meta_ver_tenant ON mm_metadata_versions(tenant_id);
CREATE INDEX idx_mm_meta_ver_entity ON mm_metadata_versions(entity_id);
CREATE INDEX idx_mm_meta_ver_time ON mm_metadata_versions(changed_at);
CREATE INDEX idx_mm_meta_ver_type ON mm_metadata_versions(version_type, change_type);

-- Schema drift detection
CREATE TABLE IF NOT EXISTS mm_schema_drift (
    drift_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    table_id UUID NOT NULL REFERENCES mm_tables(table_id) ON DELETE CASCADE,
    
    -- Drift details
    drift_type VARCHAR(64) NOT NULL, -- 'column_added', 'column_removed', 'type_changed', 'nullable_changed'
    drift_detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Expected vs actual
    expected_schema JSONB,
    actual_schema JSONB,
    
    -- Resolution
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolved_by VARCHAR(128),
    resolution_action VARCHAR(64), -- 'synced', 'ignored', 'alerted'
    
    -- Impact
    affected_queries INTEGER, -- Number of queries potentially affected
    affected_mvs UUID[] -- Affected materialized views
);

CREATE INDEX idx_mm_drift_tenant ON mm_schema_drift(tenant_id);
CREATE INDEX idx_mm_drift_table ON mm_schema_drift(table_id);
CREATE INDEX idx_mm_drift_unresolved ON mm_schema_drift(is_resolved) WHERE is_resolved = FALSE;

-- Function to auto-increment version number
CREATE OR REPLACE FUNCTION get_next_version_number(p_tenant_id VARCHAR, p_version_type VARCHAR)
RETURNS INTEGER AS $$
DECLARE
    next_version INTEGER;
BEGIN
    SELECT COALESCE(MAX(version_number), 0) + 1
    INTO next_version
    FROM mm_metadata_versions
    WHERE tenant_id = p_tenant_id AND version_type = p_version_type;
    
    RETURN next_version;
END;
$$ language 'plpgsql';

-- Trigger function to log table changes
CREATE OR REPLACE FUNCTION log_table_change()
RETURNS TRIGGER AS $$
DECLARE
    v_version_number INTEGER;
    v_change_type VARCHAR(32);
    v_previous_state JSONB;
    v_new_state JSONB;
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_change_type := 'create';
        v_previous_state := NULL;
        v_new_state := to_jsonb(NEW);
    ELSIF TG_OP = 'UPDATE' THEN
        v_change_type := 'alter';
        v_previous_state := to_jsonb(OLD);
        v_new_state := to_jsonb(NEW);
    ELSIF TG_OP = 'DELETE' THEN
        v_change_type := 'drop';
        v_previous_state := to_jsonb(OLD);
        v_new_state := NULL;
    END IF;
    
    v_version_number := get_next_version_number(NEW.tenant_id, 'schema');
    
    INSERT INTO mm_metadata_versions (
        tenant_id, version_number, version_type,
        entity_type, entity_id, entity_name,
        change_type, change_summary,
        previous_state, new_state,
        changed_by, changed_at
    ) VALUES (
        NEW.tenant_id, v_version_number, 'schema',
        'table', NEW.table_id, NEW.full_name,
        v_change_type, TG_OP || ' on mm_tables',
        v_previous_state, v_new_state,
        current_user, NOW()
    );
    
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Attach trigger to mm_tables
CREATE TRIGGER trigger_log_table_changes
    AFTER INSERT OR UPDATE OR DELETE ON mm_tables
    FOR EACH ROW EXECUTE FUNCTION log_table_change();
