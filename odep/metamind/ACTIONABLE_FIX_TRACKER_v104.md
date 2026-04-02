# MetaMind v104 Actionable Fix Tracker

Date: 2026-03-16  
Source analysis: `GAP_ANALYSIS_FULL_CODEBASE_v104.md`

## Execution strategy
1. Finish all `P0` items first (hard-failure prevention).
2. Do `P1` contract-unification as one controlled branch.
3. Do `P2` cleanup only after regression suite is stable.

## Current status
- `P0.1`: Completed on 2026-03-16 (quarantined legacy bootstrap route files).
- `P0.2`: Completed on 2026-03-16 for active runtime paths (`query_routes.py`, `graphql_gateway.py`, `audit_exporter.py`, `rule_generator.py`, `workload_profiler.py`, `index_recommender.py`, `feature_store.py`).
- `P0.3`: Completed on 2026-03-16 in code (`bootstrap.py` startup guard + `migrations/034_seed_default_tenant.sql`).
- `P1.1`: Completed on 2026-03-16 (namespace migration applied in Python client, CLI shell, frontend modules, unit/load tests).
- `P1.2`: Completed on 2026-03-16 (`SecurityHeadersMiddleware` mounted; duplicate rate limiter class renamed to `LegacyRateLimitMiddleware`).
- `P1.3`: Completed on 2026-03-16 (orphan route modules quarantined + `metamind/api/ROUTER_MAP.md` added).
- `P1.4`: Completed on 2026-03-16 (`scripts/setup_test_env.sh`, `make test-setup`, `.env.test.example`, README test setup section).
- `P2.1`: Completed on 2026-03-16 (removed obsolete `version` key from `docker-compose.yml`).
- `P2.2`: Completed on 2026-03-16 (Hive metastore now uses existing Postgres DB/user: `metamind`).
- `P2.3`: Completed on 2026-03-16 (`asyncio.get_event_loop()` replaced in active async code paths; `spark_engine` thread timing switched to `time.monotonic()`).

## P0 - Critical (Do first)

## P0.1 Remove or quarantine broken legacy bootstrap route files
- Problem: files depend on non-existent `Bootstrap`/`get_bootstrap` contracts.
- Patch order:
  1. Move `metamind/api/routes_phase2.py` to `metamind/api/legacy/routes_phase2.py`.
  2. Move `metamind/api/federation_router.py` to `metamind/api/legacy/federation_router.py` or refactor to `AppContext`.
  3. Add a short `metamind/api/legacy/README.md` describing non-runtime status.
- Verify:
  - `rg -n "from metamind.bootstrap import Bootstrap|get_bootstrap" metamind/api`
  - Expect no hits in active runtime modules.

## P0.2 Normalize `mm_query_logs` schema usage everywhere
- Problem: old columns (`executed_at`, `duration_ms`, `backend_used`, `sql_text`, `rows_returned`) still referenced.
- Canonical columns (from migrations): `original_sql`, `target_source`, `total_time_ms`, `row_count`, `submitted_at`.
- Patch order:
  1. Fix `metamind/api/query_routes.py` selects and response mapping.
  2. Fix `metamind/api/graphql_gateway.py` query + GraphQL type fields.
  3. Fix `metamind/core/io/audit_exporter.py` time window fields to `submitted_at`.
  4. Run `rg` check for old column names.
- Verify:
  - `rg -n "executed_at|duration_ms|backend_used|sql_text|rows_returned" metamind | grep -v "trace|span|docs"`
  - `curl -sS "http://localhost:8000/api/v1/query/history?tenant_id=default&limit=5"`

## P0.3 Make tenant bootstrap deterministic
- Problem: query logging fails if tenant row missing (`mm_tenants` FK).
- Patch order:
  1. Add idempotent tenant seed migration: ensure `default` tenant exists.
  2. Add startup safety check in bootstrap (warn + create if missing in non-prod/dev).
- Verify:
  - Insert/query execution no longer throws FK on `mm_query_logs`.
  - `SELECT tenant_id FROM mm_tenants WHERE tenant_id='default';`

## P1 - High

## P1.1 Unify API namespace to `/api/v1/*` across clients/tests/frontend
- Patch order:
  1. `metamind/client/python_client.py` endpoints.
  2. `metamind/cli/shell.py` endpoints.
  3. `frontend/src/api/client.ts` and modules using `/v1/*`.
  4. `tests/unit/test_api.py`, `tests/load/*`, any remaining `/v1/*`.
- Verify:
  - `rg -n '"/v1/|/v1/' metamind frontend tests | cat`
  - Expect only intentional third-party paths (e.g., Trino `/v1/statement`).

## P1.2 Consolidate middleware implementations
- Problem: duplicate `RateLimitMiddleware` definitions with different semantics.
- Patch order:
  1. Keep one implementation (recommended: `metamind/api/middleware.py`).
  2. Remove/rename duplicate in `metamind/api/security_middleware.py`.
  3. Explicitly mount `SecurityHeadersMiddleware` in `server.py`.
- Verify:
  - `rg -n "class RateLimitMiddleware" metamind/api`
  - Expect exactly one definition.
  - `curl -I http://localhost:8000/api/v1/health` includes security headers.

## P1.3 Reconcile orphan route modules with real runtime router map
- Patch order:
  1. Decide active router architecture in `server.py`.
  2. Register intended routers or archive stale files.
  3. Update docs to match actual mounted endpoints.
- Verify:
  - `rg -n "include_router\\(" metamind/api/server.py`
  - Compare against route inventory from OpenAPI: `curl -sS http://localhost:8000/openapi.json`.

## P1.4 Restore deterministic local test workflow
- Patch order:
  1. Add `make test-setup` to install dev deps.
  2. Ensure `pytest`/`pytest-asyncio` versions satisfy `pyproject.toml`.
  3. Add minimal `.env.test` and documented test command.
- Verify:
  - `python3 -m pytest -q tests/unit/test_api.py`
  - Must collect and run (no `ModuleNotFoundError`/config warnings).

## P2 - Medium

## P2.1 Compose cleanup
- Remove obsolete `version:` from `docker-compose.yml`.
- Verify:
  - `docker compose config` should not warn about obsolete version.

## P2.2 Hive/Postgres role mismatch
- Patch options:
  1. Create `hive` role/database in init SQL, or
  2. Change hive metastore connection to existing `metamind` role.
- Verify:
  - Postgres logs no `Role "hive" does not exist`.

## P2.3 Async loop modernization
- Replace `asyncio.get_event_loop()` with `asyncio.get_running_loop()` in async contexts.
- Verify:
  - `rg -n "get_event_loop\\(" metamind`
  - trend toward zero in active code paths.

## Suggested implementation sequence (safe order)
1. P0.1 -> P0.2 -> P0.3  
2. P1.1 -> P1.2 -> P1.3 -> P1.4  
3. P2.1 -> P2.2 -> P2.3

## Definition of done
- Query execution, history, metrics, and cache endpoints all work on `/api/v1/*`.
- No active code references legacy bootstrap contracts.
- No active SQL uses stale `mm_query_logs` columns.
- Frontend/CLI/Python SDK hit only current API namespace.
- Unit tests can be collected and run with one documented command.
