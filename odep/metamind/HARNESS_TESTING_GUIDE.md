# MetaMind Harness Testing Guide (v104)

Date: 2026-03-16

## 1) Is sample data auto-loaded?
Short answer: **partially**.

What migrations seed automatically:
- `migrations/009_cdc_status.sql`: inserts tenant `default` into `mm_tenants`
- `migrations/016_feature_flags.sql`: inserts `__system__` into `mm_feature_flags`
- `migrations/021_routing_policies.sql`: inserts default quotas and two default routing policies for `default`

What is **not** auto-seeded:
- Real source metadata in `mm_tables`/`mm_columns`
- Query history in `mm_query_logs`
- Business datasets (`orders`, `customers`, `products`, etc.) in connected engines

So for full harness testing, you should add test metadata + run synthetic queries to populate `mm_query_logs`.

## 2) Current known state
- API health endpoint works (`/api/v1/health` returns 200)
- SQLAlchemy startup compatibility was fixed
- Runtime query-log schema mismatch was patched in code (`backend_used/executed_at/duration_ms/table_name` -> current schema)
- `/metrics` endpoint was added for Prometheus scraping

## 3) Minimal pre-checks (before harness)
Run:

```bash
curl -i http://localhost:8000/api/v1/health
curl -i http://localhost:8000/docs
curl -i http://localhost:8000/openapi.json
curl -i http://localhost:8000/metrics
```

Expected:
- `health` -> `200`
- `docs/openapi` -> `200`
- `metrics` -> `200` (Prometheus text)

## 4) Seed SQL for harness baseline
Use this to ensure enough catalog/query data for non-trivial checks.

```sql
-- 1) Tenant (idempotent)
INSERT INTO mm_tenants (tenant_id, tenant_name, settings)
VALUES ('harness', 'Harness Tenant', '{}')
ON CONFLICT (tenant_id) DO NOTHING;

-- 2) Table metadata (idempotent)
INSERT INTO mm_tables (tenant_id, source_id, source_type, schema_name, table_name, row_count)
VALUES
  ('harness', 'oracle_prod', 'oracle', 'public', 'orders', 100000),
  ('harness', 'trino_lake', 'trino', 'analytics', 'orders_agg', 500000)
ON CONFLICT (tenant_id, source_id, schema_name, table_name) DO NOTHING;

-- 3) Query log seed (optional; useful for anomaly/MV logic)
INSERT INTO mm_query_logs (
  tenant_id, user_id, original_sql, target_source, execution_strategy,
  total_time_ms, row_count, status, query_features
)
VALUES
  ('harness', 'tester', 'SELECT COUNT(*) FROM orders', 'oracle', 'route', 120, 1, 'success', '{"table_name":"orders"}'),
  ('harness', 'tester', 'SELECT region, COUNT(*) FROM orders GROUP BY region', 'trino', 'route', 340, 12, 'success', '{"table_name":"orders"}')
ON CONFLICT DO NOTHING;
```

## 5) Functional harness matrix

### A. API contract
1. `GET /api/v1/health`
- Expect: `200`, body contains `status`, `version`, `checks`

2. `GET /docs`, `GET /openapi.json`
- Expect: `200`

3. `GET /metrics`
- Expect: `200`, Prometheus payload

### B. Query endpoint behavior
1. Validation
```bash
curl -i -X POST http://localhost:8000/api/v1/query -H 'Content-Type: application/json' -d '{}'
```
- Expect: `422`

2. Invalid SQL
```bash
curl -i -X POST http://localhost:8000/api/v1/query -H 'Content-Type: application/json' -d '{"sql":"INVALID SQL","tenant_id":"harness"}'
```
- Expect: controlled `4xx` (not 500 crash)

3. Basic query
```bash
curl -i -X POST http://localhost:8000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT 1","tenant_id":"harness","freshness_tolerance_seconds":300,"use_cache":true}'
```
- Expect: `200` or controlled `4xx` depending on connected engines
- Must include stable response schema

### C. Cache behavior
1. Warm then hit
- Run same query twice with `use_cache=true`
- Check `GET /api/v1/cache/stats`
- Expect: hit stats improve and second response generally faster

2. Invalidate
```bash
curl -i -X POST 'http://localhost:8000/api/v1/cache/invalidate?pattern=harness'
```
- Expect: success response

### D. CDC/freshness
1. `GET /api/v1/cdc/status?tenant_id=harness`
- Expect: 200 with status fields

2. Compare routing with strict vs relaxed `freshness_tolerance_seconds`
- Expect: routing decision differences when applicable

### E. Query history/audit
- Generate 10-20 query calls
- Verify rows in `mm_query_logs` with expected `status`, `target_source`, `total_time_ms`

## 6) Load harness
Note: existing `tests/load/locustfile.py` uses old paths (`/v1/query`, `/health`).
For current API, either patch it to `/api/v1/query` and `/api/v1/health`, or use a new locust file.

Run example:

```bash
locust -f tests/load/locustfile.py --host http://localhost:8000
```

SLO checker:

```bash
python tests/load/report_checker.py <locust_stats.csv>
```

Also align SLO endpoint names in `report_checker.py` with `/api/v1/*` if needed.

## 7) Resilience harness
1. Stop one dependency (e.g., trino) and execute queries
- Expect: controlled fallback/error, API process remains healthy

2. Burst traffic (20-100 concurrent requests)
- Expect: no crash, predictable rate-limiting behavior (`429` where applicable)

## 8) Useful SQL checks

```sql
-- Query log volume
SELECT COUNT(*) FROM mm_query_logs;

-- Recent failures
SELECT submitted_at, tenant_id, status, error_message
FROM mm_query_logs
WHERE status IN ('failed','cancelled')
ORDER BY submitted_at DESC
LIMIT 20;

-- Average query latency by target source
SELECT target_source, AVG(total_time_ms), COUNT(*)
FROM mm_query_logs
GROUP BY target_source
ORDER BY COUNT(*) DESC;
```

## 9) Pass criteria for harness sign-off
- Health/docs/openapi/metrics all reachable
- Query endpoint handles valid + invalid payloads without unhandled exceptions
- Cache endpoints work; cache stats move with repeated queries
- CDC status endpoint stable
- `mm_query_logs` captures executions
- Load run finishes with acceptable error rate/latency thresholds
- No repeating schema errors in Postgres logs

## 10) Important note on warnings
Current warnings like these are non-blocking for API harness pass:
- `TrinoSettings.schema shadows BaseSettings attribute`
- `strawberry-graphql not installed` (only GraphQL endpoint disabled)
