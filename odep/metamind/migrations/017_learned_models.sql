-- Migration 006: ML Model Registry
-- Tracks learned cost models and their performance

CREATE TABLE IF NOT EXISTS mm_learned_models (
    model_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES mm_tenants(tenant_id) ON DELETE CASCADE,
    
    -- Model identification
    model_name VARCHAR(128) NOT NULL,
    model_version VARCHAR(32) NOT NULL,
    model_type VARCHAR(64) NOT NULL, -- 'cost_prediction', 'routing', 'cardinality', 'cache_admission'
    
    -- Algorithm
    algorithm VARCHAR(64) NOT NULL, -- 'xgboost', 'neural_network', 'linear', 'ensemble'
    algorithm_params JSONB DEFAULT '{}',
    
    -- Storage
    model_path VARCHAR(512),
    model_size_bytes INTEGER,
    
    -- Training
    training_started_at TIMESTAMP WITH TIME ZONE,
    training_completed_at TIMESTAMP WITH TIME ZONE,
    training_duration_seconds INTEGER,
    training_samples INTEGER,
    training_features TEXT[],
    
    -- Performance metrics
    metrics JSONB DEFAULT '{}', -- { "mae": 0.15, "rmse": 0.22, "r2": 0.89 }
    
    -- Validation
    validation_samples INTEGER,
    validation_metrics JSONB,
    cross_validation_folds INTEGER,
    cross_validation_scores NUMERIC[],
    
    -- Status
    status VARCHAR(32) DEFAULT 'training', -- training, validating, active, deprecated, failed
    is_active BOOLEAN DEFAULT FALSE,
    
    -- Deployment
    deployed_at TIMESTAMP WITH TIME ZONE,
    deployed_by VARCHAR(128),
    
    -- A/B testing
    ab_test_group VARCHAR(32), -- 'control', 'treatment', NULL
    ab_test_traffic_percent INTEGER, -- 0-100
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(tenant_id, model_name, model_version)
);

CREATE INDEX idx_mm_models_tenant ON mm_learned_models(tenant_id);
CREATE INDEX idx_mm_models_type ON mm_learned_models(model_type);
CREATE INDEX idx_mm_models_active ON mm_learned_models(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_mm_models_status ON mm_learned_models(status);

-- Model feature importance
CREATE TABLE IF NOT EXISTS mm_model_features (
    feature_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_id UUID NOT NULL REFERENCES mm_learned_models(model_id) ON DELETE CASCADE,
    
    feature_name VARCHAR(128) NOT NULL,
    feature_type VARCHAR(32) NOT NULL, -- 'numeric', 'categorical', 'embedding'
    importance_score NUMERIC, -- Feature importance from model
    correlation_with_target NUMERIC,
    
    -- Statistics
    mean_value NUMERIC,
    std_value NUMERIC,
    null_fraction NUMERIC,
    
    UNIQUE(model_id, feature_name)
);

CREATE INDEX idx_mm_model_features_model ON mm_model_features(model_id);

-- Model predictions log (for drift detection)
CREATE TABLE IF NOT EXISTS mm_model_predictions (
    prediction_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_id UUID NOT NULL REFERENCES mm_learned_models(model_id) ON DELETE CASCADE,
    query_id UUID REFERENCES mm_query_logs(query_id),
    
    -- Input
    features JSONB NOT NULL,
    
    -- Output
    predicted_value NUMERIC NOT NULL,
    confidence NUMERIC,
    prediction_time_ms INTEGER,
    
    -- Actual (for learning)
    actual_value NUMERIC,
    error NUMERIC, -- predicted - actual
    absolute_error NUMERIC,
    squared_error NUMERIC,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_mm_model_pred_model ON mm_model_predictions(model_id);
CREATE INDEX idx_mm_model_pred_query ON mm_model_predictions(query_id);
CREATE INDEX idx_mm_model_pred_time ON mm_model_predictions(created_at);

-- Model drift tracking
CREATE TABLE IF NOT EXISTS mm_model_drift (
    drift_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_id UUID NOT NULL REFERENCES mm_learned_models(model_id) ON DELETE CASCADE,
    
    -- Drift metrics
    drift_type VARCHAR(64) NOT NULL, -- 'data_drift', 'concept_drift', 'performance_drift'
    drift_score NUMERIC NOT NULL,
    is_significant BOOLEAN DEFAULT FALSE,
    
    -- Details
    affected_features TEXT[],
    feature_drift_scores JSONB,
    
    -- Performance impact
    accuracy_drop NUMERIC,
    latency_increase_percent NUMERIC,
    
    -- Detection
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    detection_window_start TIMESTAMP WITH TIME ZONE,
    detection_window_end TIMESTAMP WITH TIME ZONE,
    
    -- Resolution
    status VARCHAR(32) DEFAULT 'open', -- open, investigating, retraining, resolved
    resolution_notes TEXT
);

CREATE INDEX idx_mm_model_drift_model ON mm_model_drift(model_id);
CREATE INDEX idx_mm_model_drift_significant ON mm_model_drift(is_significant) WHERE is_significant = TRUE;
CREATE INDEX idx_mm_model_drift_status ON mm_model_drift(status);

-- Trigger to update model active status
CREATE OR REPLACE FUNCTION update_model_active_status()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.is_active = TRUE THEN
        -- Deactivate other versions of same model
        UPDATE mm_learned_models
        SET is_active = FALSE,
            status = 'deprecated',
            updated_at = NOW()
        WHERE tenant_id = NEW.tenant_id
          AND model_name = NEW.model_name
          AND model_id != NEW.model_id
          AND is_active = TRUE;
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER trigger_update_model_active
    BEFORE UPDATE OF is_active ON mm_learned_models
    FOR EACH ROW WHEN (NEW.is_active = TRUE)
    EXECUTE FUNCTION update_model_active_status();
