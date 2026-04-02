# Query Routing and Pipeline Testing Guide

## Scope
Validates unified request path:
- health -> query execution -> query logging/history
- error handling and stable response schema

## 1. Health check
```bash
curl -i http://localhost:8000/api/v1/health
```

Pass criteria:
- HTTP `200`
- JSON with `status`, `version`, `checks`

## 2. Valid query execution
```bash
curl -i -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY status","tenant_id":"default","use_cache":true}'
```

Pass criteria:
- HTTP `200`
- fields include `query_id`, `status=success`, `row_count`, `data`

## 3. Validation failure path
```bash
curl -i -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{}'
```

Pass criteria:
- HTTP `4xx`
- no server crash

## 4. Query history consistency
```bash
curl -sS "http://localhost:8000/api/v1/query/history?tenant_id=default&limit=5"
```

Pass criteria:
- recent executed query appears in history
- `execution_time_ms` and `submitted_at` are populated

## 5. DB log consistency check
```bash
sudo docker compose exec postgres psql -U metamind -d metamind -c \
"SELECT status, COUNT(*) FROM mm_query_logs GROUP BY status ORDER BY status;"
```

Pass criteria:
- rows exist for `success` and/or expected `failed`
