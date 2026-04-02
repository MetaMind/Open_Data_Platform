# ML Testing Guide

## Scope
Validates ML and adaptive intelligence paths:
- cost prediction inputs/signals
- synthesis cycle execution
- training sample accumulation
- regret/adaptive feedback tables

## Prerequisites
- Stack is up
- Synthetic data loaded
- API reachable at `http://localhost:8000`

## 1. Generate workload signals
```bash
for i in {1..30}; do
  curl -sS -X POST http://localhost:8000/api/v1/query \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status","tenant_id":"default","use_cache":true}' >/dev/null
done
```

Pass criteria:
- command completes without HTTP 5xx

## 2. Validate query history signal creation
```bash
curl -sS "http://localhost:8000/api/v1/query/history?tenant_id=default&limit=10"
```

Pass criteria:
- `count > 0`
- entries contain `execution_time_ms`, `status`, `submitted_at`

## 3. Check synthesis status
```bash
curl -sS "http://localhost:8000/api/v1/synthesis/status"
```

Pass criteria:
- endpoint returns JSON
- no server error

## 4. Trigger one synthesis cycle
```bash
curl -sS -X POST "http://localhost:8000/api/v1/synthesis/run?tenant_id=default"
```

Pass criteria:
- response contains cycle fields (rules generated/retired/retrained or equivalent)

## 5. Validate DB artifacts
```bash
sudo docker compose exec postgres psql -U metamind -d metamind -c \
"SELECT tenant_id, rules_generated, rules_retired, retrained, completed_at FROM mm_synthesis_cycles ORDER BY completed_at DESC LIMIT 5;"

sudo docker compose exec postgres psql -U metamind -d metamind -c \
"SELECT tenant_id, engine, COUNT(*) FROM mm_training_samples GROUP BY tenant_id, engine ORDER BY COUNT(*) DESC;"

sudo docker compose exec postgres psql -U metamind -d metamind -c \
"SELECT tenant_id, decision_type, COUNT(*) FROM mm_optimization_decisions GROUP BY tenant_id, decision_type;"

sudo docker compose exec postgres psql -U metamind -d metamind -c \
"SELECT tenant_id, rule_name, cumulative_regret, weight, update_count FROM mm_regret_scores ORDER BY updated_at DESC LIMIT 20;"
```

Pass criteria:
- synthesis tables query successfully
- no schema errors
- counts are non-zero after sustained workload (except optional paths not enabled)

## Failure triage
- If synthesis endpoints fail: check `metamind` logs for missing feature flags/dependencies
- If tables are empty: increase workload volume and rerun synthesis
- If DB relation missing: re-apply migrations
