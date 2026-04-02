# Active Router Map (v104)

The active runtime router wiring is defined in `metamind/api/server.py`.

## Mounted in runtime
- `admin_router` at `/api/v1`
- `audit_router` at `/api/v1`
- `ab_router` at `/api/v1`
- `admin_ext_router` at `/api/v1`
- `onboarding_router` at `/api/v1`
- `billing_router` at `/api/v1`
- `trace_router` (contains `/api/v1/traces` and `/traces`)
- metadata routes via `register_metadata_routes(app, ...)`
- inline server routes:
  - `POST /api/v1/query`
  - `GET /api/v1/health`
  - `GET /api/v1/cdc/status`
  - `GET /api/v1/cache/stats`
  - `POST /api/v1/cache/invalidate`
  - `GET /metrics`

## Not mounted (quarantined or test-only)
- `metamind/api/routes_phase2.py` (quarantined shim; archived under `api/legacy/`)
- `metamind/api/federation_router.py` (quarantined shim; archived under `api/legacy/`)
- `metamind/api/routes_phase5.py` (quarantined shim; archived under `api/legacy/`)
- `metamind/api/query_routes.py` (test-only compatibility module, not mounted)

