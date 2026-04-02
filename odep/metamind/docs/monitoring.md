# MetaMind Monitoring & Observability Guide

## Overview

MetaMind provides comprehensive observability through metrics, logs, and distributed tracing. This guide covers setting up and using the monitoring stack.

## Metrics

### Prometheus Metrics

MetaMind exposes metrics at `/metrics` endpoint in Prometheus format.

#### Query Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `metamind_queries_total` | Counter | Total queries processed |
| `metamind_query_duration_seconds` | Histogram | Query execution duration |
| `metamind_routing_decision_seconds` | Histogram | Routing decision latency |
| `metamind_query_rows_total` | Histogram | Rows returned per query |

#### Cache Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `metamind_cache_hits_total` | Counter | Cache hit count |
| `metamind_cache_misses_total` | Counter | Cache miss count |
| `metamind_cache_size_bytes` | Gauge | Cache size in bytes |
| `metamind_cache_evictions_total` | Counter | Cache evictions |

#### CDC Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `metamind_cdc_lag_seconds` | Gauge | CDC replication lag |
| `metamind_cdc_events_total` | Counter | CDC events processed |
| `metamind_cdc_tables_lagging` | Gauge | Number of lagging tables |

#### Engine Health Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `metamind_oracle_circuit_breaker` | Gauge | Circuit breaker state (0=closed, 1=open) |
| `metamind_engine_health` | Gauge | Engine health score (0-1) |
| `metamind_engine_queries_active` | Gauge | Active queries per engine |

#### ML Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `metamind_ml_prediction_accuracy` | Gauge | Model prediction accuracy |
| `metamind_ml_drift_detected` | Gauge | Drift detection flag |
| `metamind_ml_training_duration_seconds` | Histogram | Model training duration |

### Metric Labels

All metrics include common labels:

| Label | Description |
|-------|-------------|
| `tenant_id` | Tenant identifier |
| `target_source` | Target engine (oracle, trino, spark) |
| `execution_strategy` | Strategy used (direct, hybrid, cached) |
| `status` | Query status (success, failed) |

### Prometheus Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'metamind'
    static_configs:
      - targets: ['localhost:8000']
    scrape_interval: 15s
    metrics_path: /metrics
```

## Logging

### Log Levels

| Level | Use Case |
|-------|----------|
| `DEBUG` | Detailed debugging info |
| `INFO` | Normal operations |
| `WARNING` | Recoverable issues |
| `ERROR` | Failed operations |
| `CRITICAL` | System failures |

### Log Format

Structured JSON logging:

```json
{
  "timestamp": "2024-03-04T12:34:56.789Z",
  "level": "INFO",
  "logger": "metamind.core.router",
  "message": "Query routed successfully",
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "tenant_id": "default",
  "target_source": "s3_analytics",
  "execution_time_ms": 145
}
```

### Log Configuration

```python
# logging.yaml
version: 1
handlers:
  console:
    class: logging.StreamHandler
    formatter: json
  file:
    class: logging.handlers.RotatingFileHandler
    filename: /var/log/metamind/app.log
    maxBytes: 104857600  # 100MB
    backupCount: 10
formatters:
  json:
    class: pythonjsonlogger.jsonlogger.JsonFormatter
    format: '%(timestamp)s %(level)s %(name)s %(message)s'
```

## Distributed Tracing

### OpenTelemetry Integration

MetaMind supports distributed tracing via OpenTelemetry.

#### Trace Spans

```
[Query Execution]
├── parse_sql (5ms)
├── route_query (15ms)
│   ├── check_cache (2ms)
│   ├── extract_features (5ms)
│   └── predict_cost (8ms)
├── execute_query (145ms)
│   └── trino_execute (140ms)
└── cache_result (3ms)
```

#### Jaeger Configuration

```yaml
# docker-compose.yml
jaeger:
  image: jaegertracing/all-in-one:1.50
  ports:
    - "16686:16686"  # UI
    - "4317:4317"    # OTLP gRPC
  environment:
    - COLLECTOR_OTLP_ENABLED=true
```

#### Environment Variables

| Variable | Description |
|----------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint |
| `OTEL_SERVICE_NAME` | Service name |
| `OTEL_TRACES_SAMPLER` | Sampling strategy |

## Grafana Dashboards

### Pre-configured Dashboards

1. **Query Performance**
   - Query latency percentiles
   - Routing decisions
   - Cache hit rates

2. **CDC Health**
   - Replication lag by table
   - Event processing rate
   - Lagging tables

3. **Cache Statistics**
   - Hit/miss rates by tier
   - Cache size
   - Eviction rate

4. **System Resources**
   - CPU/Memory usage
   - Database connections
   - Network I/O

### Dashboard Import

```bash
# Import dashboards
curl -X POST http://grafana:3000/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d @monitoring/grafana/dashboards/query-performance.json
```

## Alerting

### Prometheus Alert Rules

```yaml
# alerts.yml
groups:
  - name: metamind
    rules:
      # High CDC Lag
      - alert: HighCDCLag
        expr: metamind_cdc_lag_seconds > 600
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High CDC lag detected"
          description: "CDC lag is {{ $value }}s for table {{ $labels.table }}"
          
      # Circuit Breaker Open
      - alert: OracleCircuitBreakerOpen
        expr: metamind_oracle_circuit_breaker == 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Oracle circuit breaker is open"
          
      # Low Cache Hit Rate
      - alert: LowCacheHitRate
        expr: |
          (
            sum(rate(metamind_cache_hits_total[5m])) /
            sum(rate(metamind_cache_hits_total[5m]) + rate(metamind_cache_misses_total[5m]))
          ) < 0.5
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Cache hit rate below 50%"
          
      # High Query Latency
      - alert: HighQueryLatency
        expr: histogram_quantile(0.95, rate(metamind_query_duration_seconds_bucket[5m])) > 5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "P95 query latency > 5s"
          
      # Model Drift Detected
      - alert: ModelDriftDetected
        expr: metamind_ml_drift_detected == 1
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "ML model drift detected"
```

### Alertmanager Configuration

```yaml
# alertmanager.yml
global:
  slack_api_url: 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'

route:
  receiver: 'default'
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty'
    - match:
        severity: warning
      receiver: 'slack'

receivers:
  - name: 'default'
    slack_configs:
      - channel: '#alerts'
        
  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: 'your-pagerduty-key'
        
  - name: 'slack'
    slack_configs:
      - channel: '#warnings'
```

## Health Checks

### Endpoint Health

```bash
# Check overall health
curl http://localhost:8000/api/v1/health
```

### Component Health

| Component | Check Command |
|-----------|---------------|
| Database | `pg_isready -h $METAMIND_DB__HOST` |
| Redis | `redis-cli -h $METAMIND_REDIS__HOST ping` |
| Trino | `curl $METAMIND_TRINO__COORDINATOR_URL/v1/info` |

## Query Tracing

### Enable Query Tracing

```bash
# Trace specific query
curl -X POST http://localhost:8000/api/v1/query \
  -H "X-Trace-Enabled: true" \
  -d '{"sql": "SELECT * FROM orders"}'
```

### Query Trace Response

```json
{
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "trace_id": "abc123def456",
  "spans": [
    {"name": "parse_sql", "duration_ms": 5},
    {"name": "route_query", "duration_ms": 15},
    {"name": "execute_query", "duration_ms": 145}
  ]
}
```

## Model Drift Detection

### Drift Types

| Type | Description | Action |
|------|-------------|--------|
| Data Drift | Feature distribution changed | Retrain model |
| Concept Drift | Prediction accuracy dropped | Retrain model |
| Performance Drift | Latency patterns changed | Adjust thresholds |

### PSI (Population Stability Index)

```
PSI < 0.1: No significant change
PSI 0.1-0.25: Moderate change (monitor)
PSI > 0.25: Significant change (retrain)
```

### Drift Alert Example

```json
{
  "alert_type": "model_drift",
  "drift_type": "data_drift",
  "psi_score": 0.32,
  "affected_features": ["table_size", "join_count"],
  "recommended_action": "retrain_model"
}
```

## Performance Monitoring

### Key Performance Indicators

| KPI | Target | Alert Threshold |
|-----|--------|-----------------|
| Query P50 latency | < 100ms | > 500ms |
| Query P95 latency | < 1s | > 5s |
| Query P99 latency | < 5s | > 10s |
| Cache hit rate | > 80% | < 60% |
| CDC lag | < 5min | > 10min |
| Error rate | < 1% | > 5% |

### Performance Dashboard Queries

```promql
# P95 query latency
histogram_quantile(0.95, rate(metamind_query_duration_seconds_bucket[5m]))

# Cache hit rate
sum(rate(metamind_cache_hits_total[5m])) / 
sum(rate(metamind_cache_hits_total[5m]) + rate(metamind_cache_misses_total[5m]))

# Queries per second
rate(metamind_queries_total[1m])

# Error rate
rate(metamind_queries_total{status="failed"}[5m]) / 
rate(metamind_queries_total[5m])
```

## Log Analysis

### Common Log Queries

```bash
# Find slow queries
jq 'select(.execution_time_ms > 1000)' /var/log/metamind/app.log

# Find routing decisions
jq 'select(.message | contains("routed"))' /var/log/metamind/app.log

# Find errors
jq 'select(.level == "ERROR")' /var/log/metamind/app.log
```

### ELK Stack Integration

```yaml
# filebeat.yml
filebeat.inputs:
  - type: log
    paths:
      - /var/log/metamind/*.log
    json.keys_under_root: true
    json.add_error_key: true

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
```

## Best Practices

1. **Set appropriate retention** for metrics and logs
2. **Use sampling** for high-volume traces
3. **Create actionable alerts** with runbooks
4. **Monitor the monitors** (alert on alert failures)
5. **Regular dashboard reviews** with stakeholders
6. **Document known issues** and workarounds
7. **Test alerts** regularly (chaos engineering)

## Troubleshooting

### Missing Metrics

```bash
# Check Prometheus scraping
curl http://localhost:8000/metrics

# Verify target is up in Prometheus
http://prometheus:9090/targets
```

### High Cardinality

```bash
# Check metric cardinality
curl http://localhost:8000/metrics | grep -c "tenant_id"

# Reduce labels if needed
```

### Tracing Not Working

```bash
# Check Jaeger UI
http://jaeger:16686

# Verify OTEL configuration
echo $OTEL_EXPORTER_OTLP_ENDPOINT
```
