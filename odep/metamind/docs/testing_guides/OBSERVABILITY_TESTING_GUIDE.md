# Observability Testing Guide

## Scope
Validates health/metrics/logging observability contract.

## 1. Health contract
```bash
curl -sS http://localhost:8000/api/v1/health
```

Pass criteria:
- JSON with per-component checks (`database`, `redis`, `trino`, etc.)

## 2. Metrics endpoint
```bash
curl -sS http://localhost:8000/metrics | head -n 40
```

Pass criteria:
- Prometheus text format output
- includes process/python/metamind metrics

## 3. Query + metrics correlation
```bash
curl -sS -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status","tenant_id":"default","use_cache":true}' >/dev/null

curl -sS http://localhost:8000/metrics | grep -E "metamind_queries_total|metamind_query_duration|metamind_routing_decision"
```

Pass criteria:
- query succeeds
- metric families are present

## 4. Container log sanity
```bash
sudo docker compose logs --tail=120 metamind
```

Pass criteria:
- no repeated fatal stack traces
- requests and startup lifecycle are visible
