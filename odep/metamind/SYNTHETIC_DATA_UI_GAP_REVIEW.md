# MetaMind v104: Synthetic Data, UI Readiness, and GAP Review

Date: 2026-03-16

## 1) Synthetic data loader (5000+ rows/table)
A new loader script is added:
- `scripts/load_synthetic_data.py`

### What it loads
It creates/populates **at least 5000 rows per table** for these test tables:
- Business tables (created if missing):
  - `customers`
  - `products`
  - `orders`
  - `order_items`
  - `payments`
- MetaMind tables:
  - `mm_tenants`
  - `mm_tables`
  - `mm_columns`
  - `mm_query_logs`
  - `mm_cdc_status`

### Run commands
From repo root (`metamind_complete`), run either from host or inside container.

#### Option A: from host (Postgres published on 5432)
```bash
python3 scripts/load_synthetic_data.py \
  --host 127.0.0.1 --port 5432 \
  --dbname metamind --user metamind --password metamind \
  --rows 5000 --prefix synthetic --reset
```

#### Option B: inside `metamind` container
```bash
docker compose exec metamind python scripts/load_synthetic_data.py \
  --host postgres --port 5432 \
  --dbname metamind --user metamind --password metamind \
  --rows 5000 --prefix synthetic --reset
```

### Notes
- `--reset` removes previously generated rows for the same prefix before insert.
- Script prints final row counts at the end.

---

## 2) UI availability and solidity

## Do we have UI?
Yes:
- Main React frontend under `frontend/src`
- Trace UI at `metamind/frontend/trace_ui.html` (served via `/traces` route)

## Is frontend solid?
**Partially. Not fully solid yet.**

Reasons from code review:
- Multiple frontend modules still call legacy `/v1/*` endpoints while backend serves `/api/v1/*`.
- API response contracts are inconsistent with some UI expectations.
- `App.tsx` had broken imports for `QueryHistory` and `AdminPanel` (fixed now).

Frontend fixes applied now:
- `frontend/src/App.tsx`: corrected imports to existing component paths.
- `frontend/src/api/client.ts`: updated core endpoints to `/api/v1/*` for query/health/cache/features.
- `frontend/src/modules/QueryWorkbench/QueryWorkbench.tsx`: adapted to current query response fields (`routed_to`, `execution_strategy`, `execution_time_ms`, `data`, etc.).

Still pending in UI:
- Modules like NL/Vector/WhatIf still have legacy `/v1/*` fetch URLs and need migration.

---

## 3) GAP_ANALYSIS_v104 review and fixes applied
Reviewed file:
- `GAP_ANALYSIS_v104.md` (actual filename in repo: `GAP_ANALYSIS_v104.md`)

### Fixed items (from gap list)
- **G-01** migration numbering continuity:
  - Added `migrations/024_reserved.sql`
  - Added `migrations/025_reserved.sql`
- **G-03** MV refresh path mismatch:
  - Already fixed in current code path (`auto_refresh` executes using DB engine directly).
- **G-06** bare `except: pass` in result cache:
  - Replaced with warning log in `metamind/core/cache/result_cache.py`.
- **G-09** cancelled task suppression in MV scheduler:
  - `except asyncio.CancelledError: pass` -> `return`.
- **G-10** SLA interval binding/query columns:
  - Fixed SQL in `metamind/core/workload/sla_enforcer.py`:
    - `backend_used` -> `target_source`
    - `executed_at` -> `submitted_at`
    - `duration_ms` -> `total_time_ms`
    - proper interval binding with `:mins * INTERVAL '1 minute'`
- **G-21** JWT settings-name mismatch:
  - Fixed `metamind/api/auth.py` to use nested `settings.security.*` fields.
- **G-14/G-30/G-37** migration-runner gap:
  - Added `scripts/apply_migrations.sh` (sorted SQL runner).
  - Updated `Makefile` `migrate` target to use this script.
  - Updated `.github/workflows/ci.yml` migration step to use this script.
- **G-17/G-33/G-40** no shared test fixtures:
  - Added `tests/conftest.py` with base fixtures (`mock_redis`, `mock_db_engine`, `test_tenant_id`).

### High/critical items not fully fixed in this pass
- All `asyncio.get_event_loop()` migrations (G-07): broad cross-module change.
- Duplicate rate limiter class cleanup (G-16): safe but requires wider import audit.
- Security hardening items like mandatory JWT secret and GraphQL admin role checks (G-22, G-23): should be done with deployment policy alignment to avoid breaking existing envs.
- Full frontend API-path migration in every module.

---

## 4) Quick validation after these changes

```bash
# Rebuild API image
docker compose build metamind

# Restart API
docker compose up -d metamind

# Smoke checks
curl -i http://localhost:8000/api/v1/health
curl -i http://localhost:8000/metrics
```

Then run synthetic load command and verify counts in script output.
