# MetaMind Architecture Guide

## System Overview

MetaMind is a multi-tenant, cross-engine query optimization platform that sits above
database engines (Postgres, DuckDB, Snowflake, etc.) and provides adaptive query
optimization, cost modeling, and intelligent query routing.

```
┌─────────────────────────────────────────────────────────────────┐
│                     CLIENT APPLICATIONS                         │
│          REST API  │  TypeScript SDK  │  CLI  │  Dashboard      │
├─────────────────────────────────────────────────────────────────┤
│  Layer 12: Natural Language Interface (F28)                     │
│     NL→SQL Generator │ Conversation Mgr │ Schema Auto-Discovery│
├─────────────────────────────────────────────────────────────────┤
│  Layer 11: Query Rewrite Engine (F29)                           │
│     Anti-Pattern Detector │ Rewrite Suggester │ Improvement Est │
├─────────────────────────────────────────────────────────────────┤
│  Layer 10: What-If Simulation (F30)                             │
│     Replay Recorder │ Optimization Simulator │ Scenario Mgr     │
├─────────────────────────────────────────────────────────────────┤
│  Layer 9: Vector Intelligence (F19)                             │
│     Vector Search Planner │ ANN Cost Model │ Index Manager      │
├─────────────────────────────────────────────────────────────────┤
│  Layer 8: Workload Intelligence (F24–F26)                       │
│     Classifier │ Fair-Share Scheduler │ Pre-warming             │
├─────────────────────────────────────────────────────────────────┤
│  Layer 7: Adaptive Feedback Loop (F20)                          │
│     Regret Tracker │ Cardinality Deviation │ Online Learning    │
├─────────────────────────────────────────────────────────────────┤
│  Layer 6: Cross-Engine Federation (F14–F18)                     │
│     Transfer Costs │ Placement │ MV Freshness │ Routing         │
├─────────────────────────────────────────────────────────────────┤
│  Layer 5: Compiled Execution (F11)                              │
│     PyArrow Batch Processor │ Vector Ops │ Columnar Pipeline    │
├─────────────────────────────────────────────────────────────────┤
│  Layer 4: Cascades Optimizer (F04, F12)                         │
│     Memo Groups │ Branch-and-Bound │ DPccp │ Plan Tiering       │
├─────────────────────────────────────────────────────────────────┤
│  Layer 3: Cost Model (F01)                                      │
│     CostVector │ CostWeights │ Multi-Objective │ Calibration    │
├─────────────────────────────────────────────────────────────────┤
│  Layer 2: Metadata Layer                                        │
│     mm_tables │ mm_columns │ mm_statistics │ Tenant Isolation   │
├─────────────────────────────────────────────────────────────────┤
│  Layer 1: Backend Connectors                                    │
│     Postgres │ DuckDB │ Snowflake │ pgvector │ Lance            │
└─────────────────────────────────────────────────────────────────┘
```

## Component Deep Dives

### Metadata Layer

The metadata layer is MetaMind's central registry for all schema information, statistics,
and index metadata. It provides tenant-isolated access to table definitions, column
statistics, and vector indexes.

**Core Tables:**

The metadata catalog manages four categories of information per tenant: table metadata
(`mm_tables`) storing table names, schemas, row counts, and backend assignments; column
metadata (`mm_columns`) storing column names, data types, nullability, distinct counts,
and average widths; statistics (`mm_statistics`) storing histograms, most-common-values,
and correlation data; and vector index metadata (`mm_vector_indexes`) storing index type,
dimensions, distance metric, and build parameters.

**Tenant Isolation:**

Every operation on the metadata catalog requires an explicit `tenant_id` parameter. The
catalog maintains separate dictionaries per tenant, ensuring complete isolation. There is
no global state — a tenant cannot access another tenant's schema, statistics, or indexes.
The `CatalogSnapshot` class provides immutable, deep-copyable snapshots of catalog state
for what-if simulations.

### Cascades Optimizer

The optimizer implements the Cascades framework with top-down branch-and-bound search.
Given a logical query plan, it explores alternative physical plans using transformation
rules, estimates costs via the cost model, and returns the cheapest plan.

**Memo Groups:** Each logical expression is stored in a memo group. The optimizer avoids
redundant work by checking if an equivalent expression has already been optimized. Groups
are keyed by a canonical representation of the logical expression.

**Branch-and-Bound:** The optimizer maintains an upper bound on the best known plan cost.
When exploring a subtree, if the partial cost exceeds the bound, the subtree is pruned.
This dramatically reduces the search space for complex queries.

**DPccp Join Enumeration (F04):** For join ordering, MetaMind implements the DPccp
algorithm (Dynamic Programming connected subgraph Complement Pairs). DPccp considers
both left-deep and bushy join trees, finding the optimal join order in O(3^n) time for
n tables — a significant improvement over the O(n!) naive approach.

**Plan Tiering:** Plans are classified into tiers based on complexity. Simple single-table
queries skip the full Cascades search and use heuristic rules. Medium queries (2-4 tables)
use a limited search. Complex queries (5+ tables) use the full optimizer with DPccp.

### Cost Model

The cost model estimates execution cost as a multi-dimensional `CostVector` with four
components: CPU (processing cost), I/O (disk access cost), Network (data transfer cost),
and Memory (buffer/hash table cost).

**CostWeights** allow tuning the relative importance of each dimension. For example, a
memory-constrained deployment might increase `memory_weight` to prefer lower-memory plans.

**Calibration:** The cost model calibrates its estimates using feedback from actual query
execution. The adaptive feedback loop (F20) compares predicted vs. actual cardinalities
and adjusts cost factors accordingly. Over time, cost estimates converge toward actual
execution characteristics.

**Per-Node Estimation:**

Each operator type has a specific cost formula. Sequential scans cost O(rows × avg_width),
reflecting full table I/O. Index scans cost O(log(rows) + rows × selectivity), reflecting
B-tree traversal plus selective I/O. Hash joins cost O(left + right) CPU plus O(right)
memory for the hash table. Sorts cost O(n × log(n)) CPU. Aggregates cost O(n) CPU.

### Backend Connector Framework

MetaMind supports multiple database engines through the `BackendConnector` abstract base
class. Each connector implements three methods: `engine_name()` returning the engine
identifier, `execute_sql()` for query execution, and `capabilities()` returning a
dictionary of supported features.

**Capability Matrix:**

| Capability        | Postgres | DuckDB | Snowflake | pgvector | Lance |
|-------------------|----------|--------|-----------|----------|-------|
| SQL execution     | Yes      | Yes    | Yes       | Yes      | No    |
| Vector search     | No       | Partial| No        | Yes      | Yes   |
| HNSW index        | No       | No     | No        | Yes      | Yes   |
| IVFFlat index     | No       | No     | No        | Yes      | No    |
| Columnar storage  | No       | Yes    | Yes       | No       | Yes   |
| Transactions      | Yes      | Yes    | Yes       | Yes      | No    |

**Dialect Translation:** The `VectorSearchPlanner` handles dialect differences across
backends. For pgvector, it generates operator syntax (`<=>` for cosine, `<->` for L2,
`<#>` for inner product). For DuckDB, it uses `array_cosine_distance()` functions. For
Lance, it generates the Lance DSL format.

### Adaptive Feedback Loop

The regret tracker (F20) implements online learning using multiplicative weights. After
each query execution, it compares predicted cost to actual execution metrics. If the
prediction was significantly off, it adjusts the cost model's parameters.

**Cardinality Deviation Detection:** When actual row counts differ from estimates by more
than a configurable threshold (default: 2x), the system flags the query for reoptimization.
Persistent deviations trigger automatic statistics refresh on affected tables.

**Convergence:** The multiplicative weights algorithm guarantees that cumulative regret
grows at most O(sqrt(T × log(N))) over T queries with N alternatives. In practice,
MetaMind's cost predictions converge within a few hundred queries.

## Data Flow

The complete data flow for a query through MetaMind:

```
SQL Input
    │
    ▼
┌──────────┐     ┌──────────────┐     ┌──────────────┐
│  Parse   │────▶│  Rewrite     │────▶│  Optimize    │
│ (sqlglot)│     │  Suggestions │     │  (Cascades)  │
└──────────┘     │  (F29)       │     └──────┬───────┘
                 └──────────────┘            │
                                             ▼
                                    ┌──────────────┐
                                    │  Plan        │
                                    │  Selection   │
                                    └──────┬───────┘
                                           │
                      ┌────────────────────┼────────────────┐
                      ▼                    ▼                ▼
               ┌────────────┐     ┌──────────────┐  ┌────────────┐
               │  Postgres  │     │   DuckDB     │  │  pgvector  │
               │  Backend   │     │   Backend    │  │  Backend   │
               └─────┬──────┘     └──────┬───────┘  └─────┬──────┘
                     │                   │                 │
                     └───────────┬───────┘                 │
                                 ▼                         │
                        ┌──────────────┐                   │
                        │  Results     │◀──────────────────┘
                        │  Merge       │
                        └──────┬───────┘
                               │
                               ▼
                      ┌──────────────┐
                      │  Feedback    │
                      │  Loop (F20)  │
                      └──────────────┘
```

## Concurrency Model

**Multi-Tenant Isolation:** Each tenant operates in its own namespace. The metadata
catalog, cache keys, and ML models are all keyed by `tenant_id`. There is no shared
mutable state between tenants.

**Redis Cache Namespace:** Cache keys follow the pattern `mm:{tenant_id}:{feature}:{key}`.
This ensures tenants cannot access each other's cached data, and allows per-tenant cache
invalidation without affecting other tenants.

**Connection Pooling:** Each backend connector maintains its own connection pool. Pool
sizes are configurable per-backend and per-tenant. The default pool size is 5 connections
with a max overflow of 10.

## Failure Modes and Recovery

**Redis Down:** If Redis is unavailable, the system degrades gracefully. Cache reads
return misses, and cache writes are silently dropped. The optimizer falls back to
computing plans from scratch, which is slower but correct.

**Backend Unreachable:** If a backend connector fails to connect, the backend registry
marks it as unavailable. The vector search planner falls back to the next available
backend. The federation layer reroutes queries to available engines.

**Query Timeout:** Queries that exceed the configured timeout are cancelled. The feedback
loop records the timeout as a cost signal, increasing the estimated cost for similar
future queries. This prevents the optimizer from repeatedly choosing plans that lead
to timeouts.

**Catalog Corruption:** The `CatalogSnapshot` mechanism provides point-in-time recovery.
Snapshots can be serialized to JSON and stored in the storage backend (S3/GCS/Azure).
Recovery involves loading the most recent valid snapshot and replaying any recorded
optimization decisions.

## Vector Intelligence Architecture (F19)

The vector search subsystem provides native ANN search across three backends with
cost-based index selection.

**VectorCostModel** estimates the computational cost of different search strategies.
Exact scan costs O(rows x dimensions). IVFFlat costs O(nlist x dims + (rows/nlist) x
dims x n_probe). HNSW costs O(log(rows) x ef_search x dims).

**VectorSearchPlanner** routes queries to the optimal backend. It checks the metadata
catalog for existing vector indexes, estimates costs for each available strategy, and
generates backend-specific SQL/DSL.

**VectorBatchProcessor** provides vectorized distance computation using numpy. It
supports cosine distance (1 - cos(theta)), L2 distance (Euclidean), and inner product
distance (negative dot product for ASC sorting).

## What-If Simulation Architecture (F30)

The what-if system operates in two phases: recording and simulation.

**Recording Phase (Always On):** The `ReplayRecorder` captures optimization context for
every query: the SQL text, logical plan, optimization decisions, and cost estimates. This
recording has near-zero overhead — one database write per query.

**Simulation Phase (On Demand):** The `OptimizationSimulator` replays historical queries
against a hypothetically modified catalog. Supported modifications include adding/removing
indexes, updating statistics, changing backends, and enabling features. All simulations
are strictly read-only: they operate on deep copies of the catalog and never modify the
real metadata.

**Safety Guarantees:** The `_apply_hypothetical_changes()` method always starts with
`catalog.snapshot().deep_copy()`. The original catalog reference is never passed to the
simulation loop. This ensures that even if the simulation crashes, the production catalog
is unaffected.

## Natural Language Interface Architecture (F28)

The NL-to-SQL pipeline consists of four stages: schema discovery, example retrieval,
prompt construction, and validation.

**Schema Auto-Discovery:** The `SchemaAutoDiscovery` class uses TF-IDF-style term
matching between query words and table/column names. Query text is tokenized, stopwords
are removed, and remaining terms are scored against each table's metadata. The top-N
tables by score are included in the LLM prompt.

**Few-Shot Examples:** The `NLFeedbackCollector` stores verified (NL to SQL) pairs from
user feedback. When generating a new query, up to 5 verified examples are included in
the prompt as few-shot demonstrations.

**Multi-Provider Support:** The generator supports OpenAI (GPT-4), Anthropic (Claude),
and Ollama (local models). Provider dispatch is handled by a simple string match on
the `provider` parameter. Each provider has its own HTTP request builder that handles
authentication and response parsing.

**Confidence Scoring:** Generated SQL is assigned a confidence score from 0.0 to 1.0
based on five heuristics: all referenced tables exist in schema (+0.3), all column
references are valid (+0.3), no syntax errors (+0.2), query has WHERE clause (+0.1),
and a verified example was used (+0.1).

## Query Rewrite Architecture (F29)

The rewrite engine scans SQL for eight categories of anti-patterns. Each detector
operates independently and returns a `RewriteSuggestion` with the rewritten SQL and
an estimated improvement percentage.

**Detector Pipeline:** Detectors run sequentially: select_star, implicit_cross_join,
or_to_in, function_on_indexed_column, correlated_subquery, missing_limit,
count_distinct_hll, non_sargable_like. Results are sorted by estimated improvement
descending so the highest-impact suggestions appear first.

**Improvement Estimation:** Each rule has calibrated improvement estimates based on
data characteristics. For example, SELECT * on a 100-column table estimates 70%
improvement, while OR-to-IN with an available index estimates 30%.
