# MetaMind End-to-End Guide (Layman Friendly)

## 1) What is MetaMind in simple words?
MetaMind is a smart traffic controller for data queries.

Think of it like this:
- Your team asks questions to data using SQL.
- You have multiple data engines (fast for some tasks, slow for others).
- MetaMind decides where to send each query so it is safer, faster, and more reliable.

It does not replace your databases. It sits in front of them and helps choose the best path.

---

## 2) Why teams use it
- Faster query responses for mixed workloads.
- Better reliability when one engine is weak or unavailable.
- Centralized logging and monitoring of query behavior.
- Policy and security controls (headers, firewall behavior, cancellation hooks).
- Learning loop that can improve decisions over time.

---

## 3) Main building blocks (non-technical view)

## API Layer
This is the front door (`http://localhost:8000`).
Apps and users send query requests here.

## Routing + Decision Layer
MetaMind inspects each query and decides where to run it.

## Execution Engines
Typical engines in this stack:
- Trino
- Spark
- Oracle (optional in local setup)

## Metadata + Logs
MetaMind stores what happened (query logs, tenant metadata, status, etc.) in PostgreSQL.

## Cache + Fast state
Redis stores quick-access state used by the platform.

## Monitoring
Prometheus/Grafana track health and performance over time.

---

## 4) What “healthy” looks like
Run:
```bash
curl -sS http://localhost:8000/api/v1/health
```

Typical local result:
- `database=true`
- `redis=true`
- `trino=true`
- `oracle=false` (often expected locally)
- `spark=false` (often expected locally)

`status: degraded` can still be acceptable in local/dev if optional engines are not configured.

---

## 5) End-to-end user journey (simple)

1. User sends query to MetaMind.
2. MetaMind validates and applies policies.
3. MetaMind chooses an execution strategy.
4. Query runs and result is returned.
5. Query is logged for history, metrics, and learning.

Test query:
```bash
curl -sS -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status","tenant_id":"default","use_cache":true}'
```

---

## 6) Key endpoints every member should know
- `GET /api/v1/health` -> platform health
- `POST /api/v1/query` -> execute query
- `GET /api/v1/query/history` -> recent query logs
- `GET /api/v1/cache/stats` -> cache overview
- `GET /api/v1/cdc/status` -> freshness/CDC status
- `GET /metrics` -> Prometheus metrics
- `GET /docs` -> interactive API docs

---

## 7) What synthesis/ML means here (plain English)
MetaMind has a learning subsystem called synthesis.

It can:
- observe query patterns,
- build training samples,
- run a cycle to improve optimization signals.

Important note:
- `active_tenants: 0` with `background_running: true` can still happen.
- Manual run is valid and may show `cycles_completed` increasing.

You already validated this flow successfully.

---

## 8) How to run quick daily checks (5 minutes)

```bash
curl -sS http://localhost:8000/api/v1/health
curl -sS "http://localhost:8000/api/v1/query/history?tenant_id=default&limit=5"
curl -sS http://localhost:8000/metrics | head -n 20
```

Optional one-command smoke runner:
```bash
./scripts/RUN_ALL_TEST_GUIDES.sh
```

---

## 9) Common confusion and correct interpretation

## “Why is health degraded?”
In local setups, Oracle/Spark may be intentionally unavailable.

## “Why are some ML counters zero?”
No enough workload yet, or cycle has not run enough times.

## “Why query history empty?”
Usually tenant/logging/migration mismatch. In your current state this is fixed.

## “Why security header errors happened earlier?”
A middleware implementation bug was fixed; headers are now correctly applied.

---

## 10) Team responsibilities (simple split)

## Product / BI
Define query use cases and expected latency/freshness.

## Backend
Maintain API contracts and query pipeline correctness.

## Data/Platform
Maintain engines, CDC path, and metadata consistency.

## QA
Run feature guides and smoke scripts per release.

## SRE
Monitor health/metrics/logs and enforce deployment runbooks.

---

## 11) Where to read next
- `README.md` (main technical overview)
- `README_UNIQUE_FEATURES.md` (unique value summary)
- `docs/testing_guides/TESTING_GUIDES_INDEX.md` (feature-wise validation)
- `ACTIONABLE_FIX_TRACKER_v104.md` (open/closed remediation items)
- `SESSION_HANDOFF_v104.md` (latest operational context)

---

## 12) Bottom line
MetaMind is working as a smart query control plane in your environment.
You can already:
- run and log queries,
- track metrics,
- enforce API security headers,
- run synthesis cycles manually,
- execute structured feature testing using the new guide set.
