# MetaMind v104 Full Codebase Gap Analysis

Date: 2026-03-16
Scope reviewed: `metamind_complete` (backend, frontend, tests, Docker/ops, migrations)
Method: static code scan + targeted runtime evidence from current session

## Executive summary
The platform is partially functional (health, metrics, and basic query path are working), but the codebase still contains high-impact drift between:
- current runtime contract (`/api/v1/*`, `mm_query_logs` current schema)
- legacy modules/tests/clients still coded for older contracts (`/v1/*`, old column names)

Primary risk: future regressions and non-obvious breakage when modules are enabled, imported, or tested outside the currently exercised happy path.

## Priority findings

## P0 (critical)

1. Legacy API modules reference non-existent bootstrap API/types and can fail if wired/imported
- Evidence:
  - `metamind/api/routes_phase2.py` imports `Bootstrap` and `get_bootstrap` that do not exist in current server/bootstrap model.
  - `metamind/api/federation_router.py` imports `Bootstrap` and lazy-loads `get_bootstrap` from server, but server has no `get_bootstrap`.
  - `metamind/bootstrap.py` provides `AppContext` + `bootstrap()` (no `Bootstrap` class).
- Impact:
  - Enabling these routers or reusing these files will cause immediate runtime/import errors.
  - Creates false confidence because files exist but are not executable in current architecture.
- Files:
  - `metamind/api/routes_phase2.py`
  - `metamind/api/federation_router.py`
  - `metamind/bootstrap.py`
  - `metamind/api/server.py`

2. SQL schema contract drift still present in several modules (old `mm_query_logs` columns)
- Evidence (examples):
  - `metamind/api/query_routes.py` queries `sql_text`, `execution_time_ms`, `rows_returned`.
  - `metamind/api/graphql_gateway.py` queries `sql`, `duration_ms`, `executed_at`.
  - `metamind/core/io/audit_exporter.py` filters by `executed_at`.
  - Canonical schema in `migrations/001_core.sql` uses `original_sql`, `total_time_ms`, `row_count`, `submitted_at`, `target_source`.
- Impact:
  - Module-level runtime failures whenever these paths are called.
  - Broken audit/reporting paths even when core query endpoint works.
- Files:
  - `metamind/api/query_routes.py`
  - `metamind/api/graphql_gateway.py`
  - `metamind/core/io/audit_exporter.py`
  - `migrations/001_core.sql`

3. Query logging integrity depends on tenant seed; missing tenant causes FK violations
- Evidence from runtime logs in this session:
  - inserts into `mm_query_logs` failed due FK on `tenant_id` when `default` tenant absent.
- Impact:
  - Query history/audit may silently fail while queries still execute.
- Files/process:
  - `migrations/*` (seed/data bootstrap missing explicit default tenant guarantee)
  - `metamind/api/query_logger.py`

## P1 (high)

1. API contract drift across SDK/CLI/frontend/tests (`/v1/*` vs `/api/v1/*`)
- Evidence:
  - Python client uses `/v1/query`, `/v1/tables`, `/v1/features`.
  - CLI shell uses `/v1/query`, `/v1/tables`, `/v1/features`, `/v1/backends`.
  - Frontend modules still call `/v1/nl/*`, `/v1/vector/*`, `/v1/replay/*`, `/v1/tables`.
  - Unit/load tests still target `/v1/*` paths.
- Impact:
  - Broken UX/features outside the endpoints currently tested.
  - Test suite does not validate real API contract.
- Files (representative):
  - `metamind/client/python_client.py`
  - `metamind/cli/shell.py`
  - `frontend/src/api/client.ts`
  - `frontend/src/modules/NLQueryInterface/ConversationView.tsx`
  - `frontend/src/modules/VectorSearch/VectorSearchPanel.tsx`
  - `frontend/src/modules/WhatIfSimulator/WhatIfSimulator.tsx`
  - `tests/unit/test_api.py`
  - `tests/load/locustfile.py`

2. Legacy/duplicate route stacks exist but are not registered in current server
- Evidence:
  - `server.py` registers admin/audit/ab/admin_ext/onboarding/billing/trace + metadata routes.
  - `query_routes.py`, `routes_phase2.py`, `routes_phase5.py`, `federation_router.py` are present but not registered by `server.py`.
- Impact:
  - Orphaned code increases maintenance cost and confusion.
  - Potentially stale logic diverges from production behavior.
- Files:
  - `metamind/api/server.py`
  - `metamind/api/query_routes.py`
  - `metamind/api/routes_phase2.py`
  - `metamind/api/routes_phase5.py`
  - `metamind/api/federation_router.py`

3. Rate limiting/security middleware split and inconsistent implementations
- Evidence:
  - `metamind/api/middleware.py` and `metamind/api/security_middleware.py` both define `RateLimitMiddleware` with different signatures and semantics.
  - `server.py` imports only `metamind.api.middleware.RateLimitMiddleware`.
  - `SecurityHeadersMiddleware` exists but is not mounted in server.
- Impact:
  - Security posture depends on accidental import path.
  - Engineers may edit wrong middleware class.
- Files:
  - `metamind/api/middleware.py`
  - `metamind/api/security_middleware.py`
  - `metamind/api/server.py`

4. Local testability gap due missing dependency/bootstrap tooling
- Evidence:
  - `python3 -m pytest tests/unit/test_api.py` fails at collection with `ModuleNotFoundError: redis`.
  - same command warns unknown pytest config key `asyncio_mode` (version mismatch).
- Impact:
  - Cannot run reliable local quality gate without manual environment bootstrapping.
- Files:
  - `pyproject.toml`
  - `requirements.txt`
  - local env/runtime setup docs/scripts

## P2 (medium)

1. `docker-compose.yml` still uses obsolete `version:` key
- Evidence:
  - Docker warns: attribute `version` is obsolete.
- Impact:
  - Noise and possible confusion for operators; low runtime risk.
- File:
  - `docker-compose.yml`

2. Hive/Postgres role mismatch in compose defaults
- Evidence:
  - Hive metastore uses `ConnectionUserName=hive`; Postgres service initializes only `metamind` role by default.
  - runtime logs showed `Role "hive" does not exist`.
- Impact:
  - Hive metastore auth failures unless manually provisioned.
- File:
  - `docker-compose.yml`

3. Async loop API usage is not modernized (`asyncio.get_event_loop()` in many modules)
- Evidence:
  - multiple hits in execution/storage/core modules.
- Impact:
  - Future Python compatibility and subtle behavior differences.
- Files (representative):
  - `metamind/execution/spark_engine.py`
  - `metamind/execution/trino_engine.py`
  - `metamind/storage/s3.py`
  - `metamind/core/physical/execution_graph.py`

4. Duplicate codebase copy in workspace causes operational confusion
- Evidence:
  - both `metamind_complete` and `metamind_complete (copy)` exist.
- Impact:
  - edits/builds may target wrong directory.
- Path:
  - `MetaMind_v104/metamind_complete (copy)`

## P3 (low)

1. GraphQL feature is optional and disabled unless dependency installed
- Evidence:
  - gateway warns if `strawberry-graphql` missing.
- Impact:
  - expected behavior, but should be clearly documented in runbook.
- File:
  - `metamind/api/graphql_gateway.py`

2. Configuration warning for `TrinoSettings.schema` shadowing base attribute
- Evidence:
  - warning observed in runtime logs.
- Impact:
  - not blocking but should be cleaned.
- File:
  - `metamind/config/settings.py`

## Functional status snapshot (current)
- Working:
  - `GET /api/v1/health`
  - `GET /metrics`
  - `POST /api/v1/query` for tested aggregate query path
- Degraded/known non-functional areas:
  - Oracle/Spark health in default setup (expected without full dependencies)
  - Cache stats not reflecting query-path usage in current exercised flow
  - Legacy endpoints/modules (`/v1/*` stacks)
  - Some analytics/audit/federation modules due schema/API drift

## Recommended remediation plan (ordered)
1. Freeze canonical contracts
- Decide and document one API namespace (`/api/v1/*`) and one `mm_query_logs` schema contract.

2. Remove or quarantine legacy modules
- Move orphan route stacks (`routes_phase2.py`, legacy `query_routes.py`, etc.) under `legacy/` or delete after migration.

3. Complete contract migration
- Update Python SDK, CLI, frontend modules, and tests to canonical endpoints/response schema.

4. Schema query hardening
- Replace old column references (`executed_at`, `duration_ms`, `backend_used`, `sql_text`, `rows_returned`) with current schema fields.

5. Stabilize bootstrap/test workflow
- Provide one local test bootstrap script (venv + dev deps + minimal env vars).
- Pin pytest/pytest-asyncio versions compatible with `pyproject.toml` options.

6. Ops cleanup
- Remove compose `version` key.
- Add explicit SQL/init for Hive role or align Hive metastore DB creds with existing role.

7. Security middleware consolidation
- Keep one `RateLimitMiddleware` implementation and explicitly mount `SecurityHeadersMiddleware`.

## Quick verification checklist after remediation
- `pytest -q` runs locally without import/config collection errors.
- `curl /api/v1/query/history` returns rows using current schema fields.
- Frontend main flows (Query, NL, Vector, Replay) call `/api/v1/*` only.
- SDK/CLI smoke tests pass against live API.
- No module imports `Bootstrap`/`get_bootstrap` unless those APIs truly exist.

