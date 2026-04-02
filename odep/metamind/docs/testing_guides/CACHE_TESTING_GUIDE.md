# Cache Testing Guide

## Scope
Validates cache endpoints and basic cache behavior visibility.

## 1. Baseline stats
```bash
curl -sS http://localhost:8000/api/v1/cache/stats
```

Pass criteria:
- JSON response with cache stat fields

## 2. Execute identical query multiple times
```bash
for i in 1 2 3; do
  curl -sS -X POST http://localhost:8000/api/v1/query \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY status","tenant_id":"default","use_cache":true}'
  echo
done
```

Pass criteria:
- all responses successful
- `cache_hit` may remain false if cache feature flags/path are disabled

## 3. Re-check stats
```bash
curl -sS http://localhost:8000/api/v1/cache/stats
```

Pass criteria:
- endpoint remains healthy
- counters are logically consistent (no negative values)

## 4. Invalidate cache
```bash
curl -sS -X POST "http://localhost:8000/api/v1/cache/invalidate?pattern=default"
```

Pass criteria:
- success JSON returned
- no server error
