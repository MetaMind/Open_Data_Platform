# MetaMind v104 - Unique Features Guide

Date: 2026-03-16
Audience: Engineering, Data, Platform, SRE, QA, Product

## What makes MetaMind different
MetaMind is not just a SQL API. It is an adaptive query intelligence control plane that sits above multiple execution engines and continuously improves routing, optimization, and reliability.

## Core unique capabilities

## 1) AI control-plane over heterogeneous engines
- Routes one SQL workload across Oracle, Trino, Spark, and internal execution strategies.
- Decouples optimization decisions from physical execution engine.
- Uses workload-aware decisioning (latency/cost/freshness context).

## 2) Unified query pipeline (single execution path)
- One pipeline handles: validation -> firewall -> policy controls -> routing -> execution -> telemetry.
- Reduces split-brain logic from separate router/engine code paths.
- Produces consistent response metadata (`query_id`, strategy, cache hit, timing).

## 3) Adaptive optimization stack
- Learns from execution outcomes and updates behavior over time.
- Supports synthesis and rule generation from observed query patterns.
- Includes anomaly-aware and cost-aware decision hooks.

## 4) CDC/freshness-aware architecture
- Tracks freshness and lag signals for downstream routing quality.
- Integrates data-lake path (S3/Iceberg) with source-system awareness.
- Enables explicit stale-data risk handling instead of blind routing.

## 5) Multi-layer observability by default
- Built-in `/metrics` endpoint with Prometheus-formatted telemetry.
- Query-level logging and history endpoints for audit/debug.
- Health registry model reports per-engine status (`database`, `redis`, `trino`, etc.).

## 6) Security-first query surface
- Query firewall support with allow/deny/fingerprint controls.
- API middleware now injects production-grade security headers:
  - `Strict-Transport-Security`
  - `Content-Security-Policy`
  - `X-Frame-Options`
  - `X-Content-Type-Options`
  - `Referrer-Policy`
  - `Permissions-Policy`
- Cancellation and governance hooks for safer runtime control.

## 7) Production-oriented data model
- Central metadata schema (`mm_*`) for tenants, catalog, logs, budgets, CDC, features, synthesis.
- Strong relational integrity (`tenant`-scoped query log constraints).
- Structured migrations and migration runner workflow.

## 8) Synthetic harness and practical testability
- Synthetic data loader for high-row-count test scenarios (`5000+` rows/table).
- Deterministic local test setup path:
  - `make test-setup`
  - `.env.test.example`
- Useful for repeatable QA, regression, and performance checks.

## 9) Team-friendly operational stack
- Docker compose stack includes API + Postgres + Redis + Trino + Spark + monitoring.
- Grafana/Prometheus integrated for fast runtime diagnostics.
- Router-map documentation added to prevent route drift and confusion.

## 10) Frontend + API alignment direction
- API namespace standardized around `/api/v1/*`.
- Frontend modules and SDK/CLI moving toward one canonical contract.
- Reduces integration ambiguity across teams.

## Feature map by team

## Engineering
- Unified pipeline contracts, modular routing/execution, schema-driven telemetry.

## SRE/Platform
- Health + metrics endpoints, containerized stack, predictable startup and migration paths.

## QA
- Synthetic dataset tooling, stable endpoint contracts, query-history visibility.

## Security
- Firewall framework + hardened response headers + tenant-scoped logging integrity.

## Product/Data
- Cross-engine intelligence, freshness-aware behavior, explainable routing metadata.

## Key endpoints to know
- `GET /api/v1/health`
- `POST /api/v1/query`
- `GET /api/v1/query/history?tenant_id=<id>&limit=<n>`
- `GET /api/v1/cache/stats`
- `POST /api/v1/cache/invalidate`
- `GET /metrics`

## Important docs in this repo
- `README.md` - product and setup overview
- `LAYMAN_END_TO_END_GUIDE.md` - plain-English end-to-end guide
- `MARKET_COMPARISON_AND_HOLDING_ADVANTAGES.md` - market comparison and strategic rationale
- `GTM_FOCUS_GUIDE.md` - ICP, positioning, value pillars, pilot motion
- `GTM_STRATEGY_AND_EXECUTION_GUIDE.md` - 30-60-90 plan, KPI model, execution cadence
- `GTM_EXEC_SUMMARY.md` - one-page executive/board GTM summary
- `GAP_ANALYSIS_FULL_CODEBASE_v104.md` - full gap review
- `ACTIONABLE_FIX_TRACKER_v104.md` - prioritized fix tracker
- `SESSION_HANDOFF_v104.md` - execution history and handoff notes
- `HARNESS_TESTING_GUIDE.md` - functional/harness test flow
- `docs/testing_guides/TESTING_GUIDES_INDEX.md` - feature-wise testing index
- `docs/testing_guides/ML_TESTING_GUIDE.md` - ML/adaptive testing
- `docs/testing_guides/QUERY_ROUTING_PIPELINE_TESTING_GUIDE.md` - routing/pipeline testing
- `docs/testing_guides/CACHE_TESTING_GUIDE.md` - cache testing
- `docs/testing_guides/CDC_FRESHNESS_TESTING_GUIDE.md` - CDC/freshness testing
- `docs/testing_guides/SECURITY_TESTING_GUIDE.md` - security testing
- `docs/testing_guides/OBSERVABILITY_TESTING_GUIDE.md` - metrics/logging testing

## Current known boundaries
- Health may report `degraded` when optional engines (Oracle/Spark) are not fully provisioned.
- Some advanced modules remain intentionally quarantined as legacy until fully migrated.

## Quick start for new members
1. Start stack and validate health.
2. Load synthetic data.
3. Run sample queries and verify history/metrics.
4. Use fix tracker for current priority work.
