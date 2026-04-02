# CDC and Freshness Testing Guide

## Scope
Validates CDC status API and lag visibility behavior.

## 1. CDC status endpoint
```bash
curl -sS "http://localhost:8000/api/v1/cdc/status?tenant_id=default"
```

Pass criteria:
- HTTP `200`
- JSON includes `total_tables`, `healthy`, `warning`, `critical`, `overall_status`

## 2. Metrics visibility for CDC
```bash
curl -sS http://localhost:8000/metrics | grep -E "metamind_cdc_"
```

Pass criteria:
- CDC metric series exist (values may be zero in minimal setup)

## 3. Optional DB check
```bash
sudo docker compose exec postgres psql -U metamind -d metamind -c \
"SELECT tenant_id, source_id, table_name, lag_seconds, health_status FROM mm_cdc_status WHERE tenant_id='default' ORDER BY lag_seconds DESC LIMIT 20;"
```

Pass criteria:
- query runs without schema error
- rows present if CDC status has been populated
