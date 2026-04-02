# MetaMind Troubleshooting Guide

## Common Issues

### Query Routing Issues

#### Query Not Routing to Expected Source

**Symptoms**: Query routes to Oracle when S3 should be used, or vice versa.

**Diagnosis**:
```bash
# Check CDC lag for tables
curl http://localhost:8000/api/v1/cdc/status

# Check routing decision
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "YOUR_QUERY",
    "freshness_tolerance_seconds": 300
  }'
```

**Solutions**:
1. Verify CDC lag is within tolerance
2. Check freshness requirements in query
3. Review ML cost model predictions

#### Hybrid Query Not Working

**Symptoms**: UNION ALL query not generated for mixed freshness needs.

**Diagnosis**:
```bash
# Check if time column is detected
curl -X POST http://localhost:8000/api/v1/query \
  -d '{"sql": "SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '"'"'1 hour'"'"'"}'
```

**Solutions**:
1. Ensure table has standard time column (created_at, updated_at, timestamp)
2. Add explicit time filter to query

### Oracle Connection Issues

#### Circuit Breaker Open

**Symptoms**: All Oracle queries failing with "Circuit breaker is open".

**Diagnosis**:
```bash
# Check Oracle health
curl http://localhost:8000/api/v1/health

# Check Oracle connectivity from pod
kubectl exec -it deployment/metamind -- nc -zv oracle-host 1521
```

**Solutions**:
1. Check Oracle database is accessible
2. Verify credentials are correct
3. Review Oracle alert logs
4. Wait for circuit breaker timeout (60s) or restart pod

#### Connection Pool Exhausted

**Symptoms**: Queries queued or rejected with "ResourceExhausted".

**Diagnosis**:
```bash
# Check current connections
kubectl exec -it deployment/metamind -- \
  python -c "from metamind.bootstrap import get_context; ..."
```

**Solutions**:
1. Increase `METAMIND_ORACLE__MAX_SESSIONS`
2. Reduce query concurrency
3. Optimize slow queries

### CDC Pipeline Issues

#### High CDC Lag

**Symptoms**: CDC lag > 5 minutes, queries routing to Oracle unexpectedly.

**Diagnosis**:
```bash
# Check Kafka lag
kubectl exec -it kafka-0 -- kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group debezium-oracle

# Check Spark streaming status
kubectl logs deployment/spark-streaming --tail 100
```

**Solutions**:
1. Increase Kafka partitions for high-volume tables
2. Scale Spark workers
3. Check for errors in Debezium connector
4. Verify Oracle CDC log retention

#### CDC Stopped

**Symptoms**: No new CDC events, lag increasing continuously.

**Diagnosis**:
```bash
# Check Debezium connector status
curl http://debezium:8083/connectors/oracle-connector/status

# Check Kafka topic
curl http://kafka:9092/topics
```

**Solutions**:
1. Restart Debezium connector
2. Check Oracle logminer configuration
3. Verify Kafka broker health

### Cache Issues

#### Low Cache Hit Rate

**Symptoms**: Cache hit rate < 50%.

**Diagnosis**:
```bash
# Check cache stats
curl http://localhost:8000/api/v1/cache/stats
```

**Solutions**:
1. Increase cache TTL
2. Pre-populate cache with common queries
3. Review cache key generation

#### Cache Not Working

**Symptoms**: No cache hits, always querying source.

**Diagnosis**:
```bash
# Check Redis connectivity
kubectl exec -it redis-0 -- redis-cli ping

# Check cache configuration
curl http://localhost:8000/api/v1/health
```

**Solutions**:
1. Verify Redis is accessible
2. Check cache is enabled in config
3. Clear and rebuild cache

### Performance Issues

#### Slow Query Routing

**Symptoms**: Routing decision takes > 10ms.

**Diagnosis**:
```bash
# Enable debug logging
export METAMIND_OBSERVABILITY__LOG_LEVEL=DEBUG

# Check metrics
curl http://localhost:8000/metrics | grep routing_decision
```

**Solutions**:
1. Optimize metadata catalog queries
2. Enable metadata caching
3. Review ML model inference time

#### Slow Query Execution

**Symptoms**: Queries taking longer than expected.

**Diagnosis**:
```bash
# Check query logs
psql -h $DB_HOST -U metamind -c "
  SELECT original_sql, total_time_ms, target_source
  FROM mm_query_logs
  ORDER BY total_time_ms DESC
  LIMIT 10;
"
```

**Solutions**:
1. Optimize source queries
2. Add indexes to source tables
3. Consider materialized views
4. Review partition pruning

### ML Model Issues

#### Poor Cost Predictions

**Symptoms**: Actual query time differs significantly from predicted.

**Diagnosis**:
```bash
# Check model metrics
ls -la models/*_metrics.json
cat models/s3_iceberg_metrics.json
```

**Solutions**:
1. Retrain model with recent data
2. Add more features
3. Increase training samples

#### Model Not Loading

**Symptoms**: Falling back to heuristic predictions.

**Diagnosis**:
```bash
# Check model files exist
ls -la models/

# Check logs for model load errors
kubectl logs deployment/metamind | grep -i "model"
```

**Solutions**:
1. Train and save models
2. Verify model path is correct
3. Check model file permissions

## Debug Commands

### General Debugging

```bash
# Check all service health
curl http://localhost:8000/api/v1/health

# Get recent logs
kubectl logs deployment/metamind --tail 100 -f

# Check resource usage
kubectl top pods -n metamind
```

### Database Debugging

```bash
# Connect to PostgreSQL
kubectl exec -it postgres-0 -- psql -U metamind

# Check query logs
SELECT * FROM mm_query_logs ORDER BY submitted_at DESC LIMIT 10;

# Check CDC status
SELECT table_name, lag_seconds, health_status FROM mm_cdc_status;
```

### Redis Debugging

```bash
# Connect to Redis
kubectl exec -it redis-0 -- redis-cli

# Check cache keys
KEYS l2:*

# Get cache stats
INFO stats
```

### Trino Debugging

```bash
# Check Trino cluster
kubectl exec -it trino-0 -- trino --execute "SELECT * FROM system.runtime.nodes"

# Check queries
kubectl exec -it trino-0 -- trino --execute "SELECT * FROM system.runtime.queries ORDER BY created DESC LIMIT 10"
```

## Log Analysis

### Key Log Patterns

```bash
# Find routing decisions
grep "Routed query" /var/log/metamind/app.log

# Find errors
grep "ERROR" /var/log/metamind/app.log

# Find slow queries
grep "execution_time_ms" /var/log/metamind/app.log | jq '.execution_time_ms'
```

### Structured Logging

MetaMind uses structured JSON logging. Parse with:

```bash
# Pretty print logs
jq '.' /var/log/metamind/app.log

# Filter by level
jq 'select(.level == "ERROR")' /var/log/metamind/app.log

# Filter by component
jq 'select(.logger == "metamind.core.router")' /var/log/metamind/app.log
```

## Emergency Procedures

### Complete Outage

1. Check infrastructure health
   ```bash
   kubectl get pods -n metamind
   kubectl get svc -n metamind
   ```

2. Check database connectivity
   ```bash
   kubectl exec -it postgres-0 -- pg_isready
   ```

3. Restart services if needed
   ```bash
   kubectl rollout restart deployment/metamind -n metamind
   ```

### Data Corruption

1. Stop CDC pipeline
2. Restore from backup
3. Replay CDC from last checkpoint

### Security Incident

1. Revoke compromised credentials
2. Rotate all secrets
3. Review audit logs
4. Enable additional monitoring

## Getting Help

### Collecting Diagnostic Information

```bash
# Generate diagnostic bundle
./scripts/collect-diagnostics.sh

# This creates diagnostics.tar.gz with:
# - Logs
# - Configuration
# - Metrics
# - Database dumps
```

### Support Channels

- GitHub Issues: https://github.com/metamind/metamind/issues
- Documentation: https://docs.metamind.io
- Email: support@metamind.io
