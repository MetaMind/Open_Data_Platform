# ODEP — Open Data Engineering Platform

A modular, open-source data engineering platform for the complete data lifecycle.
Engine-agnostic at every layer — swap metadata, orchestration, or compute backends
without touching pipeline code.

---

## Quick Start

```bash
# 1. Create and activate a virtual environment
python3 -m venv .venv && source .venv/bin/activate

# 2. Install
pip install -e .

# 3. Scaffold a new project
odep init my-platform --engine=duckdb

# 4. Run a query locally (no Docker needed)
python3 -c "
from odep.adapters.duckdb.adapter import DuckDbAdapter
from odep.config import ExecutionConfig
from odep.models import JobConfig, EngineType
a = DuckDbAdapter(ExecutionConfig())
h = a.submit(JobConfig(engine=EngineType.SQL, code='SELECT 42 as answer'))
r = a.wait_for_completion(h)
print('rows:', r.records_processed, 'success:', r.success)
"
```

---

## Architecture

```
User Interface Layer
  CLI (odep)  ·  Python SDK  ·  Web Console (React)
        │
  API Gateway (FastAPI)
  JWT Auth  ·  Rate Limiting  ·  Audit Logging  ·  OTel Tracing
        │
  ┌─────┴──────────────────────────────────────┐
  │  MetadataService Protocol                  │
  │  OpenMetaAdapter (default)                 │
  │  DataHub · OpenLineage · GE · OPA          │
  └─────┬──────────────────────────────────────┘
        │
  ┌─────┴──────────────────────────────────────┐
  │  Orchestrator Protocol                     │
  │  AirflowAdapter (default)                  │
  │  DagsterAdapter · PrefectAdapter · Temporal│
  └─────┬──────────────────────────────────────┘
        │
  ┌─────┴──────────────────────────────────────┐
  │  ExecutionEngine Protocol                  │
  │  DuckDbAdapter (local default)             │
  │  SparkAdapter · TrinoAdapter               │
  │  FlinkAdapter · DbtAdapter                 │
  │  MetaMindAdapter (AI-driven opt-in)        │
  └─────┬──────────────────────────────────────┘
        │
  Storage Layer
  Iceberg · Delta Lake · Hudi
  MinIO / S3 · GCS · Azure Blob
```

---

## Installation

```bash
pip install -e .

# Optional engine dependencies
pip install pyspark          # Spark engine
pip install trino            # Trino engine
pip install duckdb           # DuckDB engine (included by default)
```

---

## Execution Engines

### DuckDB (default — no Docker needed)

Runs in-process. Zero setup.

```bash
odep config set execution.default_engine=duckdb
```

```python
from odep.adapters.duckdb.adapter import DuckDbAdapter
from odep.config import ExecutionConfig
from odep.models import JobConfig, EngineType

adapter = DuckDbAdapter(ExecutionConfig())
handle = adapter.submit(JobConfig(engine=EngineType.SQL, code="SELECT 1 as id"))
result = adapter.wait_for_completion(handle)
# result.success == True, result.records_processed == 1
```

---

### Apache Spark

Two modes:

| Mode | When to use | Master URL |
|---|---|---|
| `local[*]` | Development, no Docker | `local[*]` (default) |
| Cluster | Production / Docker | `spark://localhost:7077` |

**Start the Spark cluster:**

```bash
docker compose --profile spark up -d
# Master UI: http://localhost:8081
# Submit port: localhost:7077
```

**Configure:**

```bash
odep config set execution.default_engine=spark
odep config set execution.spark_master=spark://localhost:7077
odep config set execution.spark_rest_url=http://localhost:8081
```

**Use:**

```python
from odep.adapters.spark.adapter import SparkAdapter
from odep.config import ExecutionConfig
from odep.models import JobConfig, EngineType

config = ExecutionConfig()  # reads from .odep.env
adapter = SparkAdapter(config)

# SQL job
handle = adapter.submit(JobConfig(
    engine=EngineType.SQL,
    code="SELECT id, name FROM my_table WHERE active = true"
))
result = adapter.wait_for_completion(handle)
print(result.records_processed)
print(result.metrics)  # backend_used, spark_master, app_id, execution_time_ms

# Python job (spark session available as `spark`)
handle = adapter.submit(JobConfig(
    engine=EngineType.PYTHON,
    code="""
df = spark.range(100)
df.createOrReplaceTempView("nums")
_row_count = spark.sql("SELECT COUNT(*) FROM nums").collect()[0][0]
"""
))
result = adapter.wait_for_completion(handle)

# Async execution
handle = adapter.submit(config, async_run=True)
# ... do other work ...
result = adapter.wait_for_completion(handle, timeout_sec=300)
```

**Metrics returned by `get_metrics()`:**

```python
{
    "execution_time_ms": 1234,
    "rows_processed": 500,
    "backend_used": "spark",
    "spark_master": "spark://localhost:7077",
    "app_id": "app-20240101-001"
}
```

---

### Trino

Distributed SQL query engine. Connects via the DBAPI2 interface (`trino-python-client`).

**Start Trino:**

```bash
docker compose --profile trino up -d
# Trino UI: http://localhost:8082/ui
# No credentials required for local dev
```

**Configure:**

```bash
odep config set execution.default_engine=trino
odep config set execution.trino_host=localhost
odep config set execution.trino_port=8082
odep config set execution.trino_catalog=tpch
odep config set execution.trino_schema=tiny
```

**Use:**

```python
from odep.adapters.trino.adapter import TrinoAdapter
from odep.config import ExecutionConfig
from odep.models import JobConfig, EngineType

adapter = TrinoAdapter(ExecutionConfig())

# Health check
print(adapter.health_check())  # True when Trino is running

# Run a query against the built-in tpch catalog
handle = adapter.submit(JobConfig(
    engine=EngineType.SQL,
    code="SELECT orderkey, totalprice FROM tpch.tiny.orders LIMIT 10"
))
result = adapter.wait_for_completion(handle)
print(result.records_processed)  # 10
print(result.metrics)

# Async execution
handle = adapter.submit(config, async_run=True)
status = adapter.get_status(handle)   # {"status": "RUNNING", "query_id": "..."}
result = adapter.wait_for_completion(handle, timeout_sec=120)

# Cancel a running query
adapter.cancel(handle)  # calls DELETE /v1/query/{query_id} on coordinator
```

**Metrics returned by `get_metrics()`:**

```python
{
    "execution_time_ms": 450,
    "rows_processed": 10,
    "backend_used": "trino",
    "trino_host": "localhost",
    "trino_port": 8082,
    "query_id": "20240101_120000_00001_abcde",
    "catalog": "tpch",
    "schema": "tiny",
    "elapsed_ms": 430,
    "cpu_ms": 120,
    "peak_memory_bytes": 2097152
}
```

**Available catalogs out of the box:**

| Catalog | Description | Ready |
|---|---|---|
| `tpch` | Built-in benchmark data (orders, customers, lineitem…) | Immediately |
| `memory` | In-memory tables, great for testing | Immediately |
| `iceberg` | Iceberg tables (configure metastore in `trino/catalog/iceberg.properties`) | After setup |

**Add your own catalog** — drop a `.properties` file into `trino/catalog/`:

```properties
# trino/catalog/postgresql.properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/mydb
connection-user=myuser
connection-password=mypassword
```

Then restart: `docker compose --profile trino restart trino`

---

### MetaMind (AI-driven opt-in)

Routes SQL through MetaMind v2.0's Cascades optimizer, Redis plan cache, RLS rewriter,
and workload classifier before dispatching to the best backend automatically.

```bash
odep config set execution.default_engine=metamind
# Set via environment or .odep.env:
# ODEP_METAMIND__METAMIND_URL=https://metamind.internal
# ODEP_METAMIND__API_TOKEN=<token>
# ODEP_METAMIND__TENANT_ID=my-tenant
```

```python
from odep.adapters.metamind.adapter import MetaMindAdapter
from odep.config import MetaMindConfig

adapter = MetaMindAdapter(MetaMindConfig())
handle = adapter.submit(JobConfig(engine=EngineType.SQL, code="SELECT ..."))
result = adapter.wait_for_completion(handle)
metrics = adapter.get_metrics(handle)
# metrics contains: optimization_tier, cache_hit, workload_type,
#                   backend_used, optimization_ms, plan_cost, flags_used
```

---

## Docker Compose Profiles

| Profile | Services started | RAM |
|---|---|---|
| `spark` | spark-master, spark-worker | ~2 GB |
| `trino` | trino | ~1.5 GB |
| `minimal` | marquez only | < 1 GB |
| `full` | everything | ~8 GB |

```bash
# Spark only
docker compose --profile spark up -d

# Trino only
docker compose --profile trino up -d

# Spark + Trino together
docker compose --profile spark --profile trino up -d

# Full stack (Airflow + DataHub + Marquez + Spark + Trino + MinIO)
docker compose --profile full up -d

# Stop everything
docker compose down
docker compose down --volumes   # also removes data volumes
```

---

## CLI Reference

```
odep init [project-name] [--engine ENGINE]
    Scaffold .odep.env, odep.yaml, docker-compose.yml

odep local up [--profile full|minimal]
    Start Docker Compose stack

odep local down [--volumes]
    Stop Docker Compose stack

odep deploy PATH [--env ENV] [--schedule CRON]
    Deploy pipeline to orchestrator, register in catalog, record lineage

odep run JOB_ID [--backfill --start DATE --end DATE]
    Trigger a run or backfill

odep logs RUN_ID [--tail N]
    Fetch run logs

odep test PATH [--suite SUITE]
    Run data quality suite

odep lineage URN
    Show upstream lineage ASCII graph

odep cost
    Print cost estimation hint

odep config get KEY
    Read config value (dot notation, e.g. metadata.engine)

odep config set KEY=VALUE
    Write to .odep.env

odep template list
    List available Cookiecutter templates

odep template use NAME [--name PIPELINE_NAME]
    Generate pipeline from template
```

---

## Configuration

All config is loaded from `.odep.env` (or environment variables).

```bash
# .odep.env
ODEP_METADATA__ENGINE=openmeta
ODEP_ORCHESTRATION__ENGINE=airflow
ODEP_ORCHESTRATION__AIRFLOW_URL=http://localhost:8090

# Execution engine selection
ODEP_EXECUTION__DEFAULT_ENGINE=duckdb   # or spark, trino, flink, dbt

# Spark
ODEP_EXECUTION__SPARK_MASTER=local[*]
ODEP_EXECUTION__SPARK_REST_URL=http://localhost:8081

# Trino
ODEP_EXECUTION__TRINO_HOST=localhost
ODEP_EXECUTION__TRINO_PORT=8082
ODEP_EXECUTION__TRINO_USER=odep
ODEP_EXECUTION__TRINO_CATALOG=tpch
ODEP_EXECUTION__TRINO_SCHEMA=tiny

# MetaMind (opt-in)
ODEP_METAMIND__METAMIND_URL=https://metamind.internal
ODEP_METAMIND__TENANT_ID=my-tenant
ODEP_METAMIND__API_TOKEN=<secret>
```

---

## Data Quality Engine

ODEP has a dedicated DQ engine with 13 built-in rule types, a `NativeQualityEngine` (pandas + DuckDB, no external service), and an optional `GreatExpectationsAdapter`.

### Rule Types

| Rule | Description |
|---|---|
| `not_null` | Column has no null values |
| `unique` | Column has no duplicate values |
| `min` | Column minimum >= threshold |
| `max` | Column maximum <= threshold |
| `min_length` | String column min length |
| `max_length` | String column max length |
| `regex` | Values match a regex pattern |
| `accepted_values` | Values are in an allowed set |
| `row_count_min` | Table has at least N rows |
| `row_count_max` | Table has at most N rows |
| `freshness` | Latest timestamp is within N hours |
| `completeness` | % non-null >= threshold |
| `custom_sql` | Arbitrary SQL expression returns True |

Each rule has a `severity`: `BLOCKING` (raises `QualityGateFailure`) or `WARNING` (records but continues).

### Quick Start

```python
import pandas as pd
from odep.dq.models import QualitySuite, QualityRule, Severity
from odep.dq.runner import run_quality_suite

df = pd.DataFrame({
    "order_id": [1, 2, 3],
    "amount": [100.0, 200.0, 50.0],
    "status": ["active", "active", "inactive"],
})

urn = "urn:li:dataset:(duckdb,orders,prod)"
suite = QualitySuite(name="orders_suite", dataset_urn=urn)
suite.add_rule(QualityRule.not_null("order_id"))
suite.add_rule(QualityRule.unique("order_id"))
suite.add_rule(QualityRule.min_value("amount", 0.0))
suite.add_rule(QualityRule.accepted_values("status", ["active", "inactive"]))
suite.add_rule(QualityRule.row_count_min(1))
suite.add_rule(QualityRule.custom_sql("no_negatives", "SELECT COUNT(*) = 0 FROM data WHERE amount < 0"))
suite.add_rule(QualityRule.regex("status", r"^[a-z]+$", severity=Severity.WARNING))

result = run_quality_suite(suite, df)
print(f"Score: {result.quality_score:.1f}%  Passed: {result.passed}/{result.total_rules}")
```

### Persist to Metadata Catalog

```python
from odep.adapters.openmeta.adapter import OpenMetaAdapter
from odep.config import MetadataConfig

metadata = OpenMetaAdapter(MetadataConfig())
result = run_quality_suite(suite, df, metadata_adapter=metadata)
score = metadata.get_quality_score(urn)  # 0.0–100.0
```

### Great Expectations Adapter

```python
from odep.dq.ge_adapter import GreatExpectationsAdapter
# pip install great-expectations
engine = GreatExpectationsAdapter()
result = engine.run_suite(suite, df)
```

### DQ Module Structure

```
odep/dq/
├── models.py       QualityRule, QualitySuite, CheckResult, SuiteResult, RuleType, Severity
├── engine.py       DataQualityEngine Protocol + NativeQualityEngine (pandas + DuckDB)
├── ge_adapter.py   GreatExpectationsAdapter (optional)
└── runner.py       run_quality_suite() — runs suite + persists to metadata catalog
```

---

## Pipeline SDK

```python
from odep.sdk.pipeline import Pipeline
from odep.sdk.deploy import deploy_pipeline
from odep.sdk.execute import execute_with_quality_gate

# Load from YAML
pipeline = Pipeline.from_file("pipelines/user_etl.yaml")
pipeline.validate()                        # local, no network calls
job_def = pipeline.to_job_definition("prod")
edges = pipeline.extract_lineage_edges()   # List[LineageEdge]

# Deploy (registers in catalog + records lineage)
job_id = deploy_pipeline("pipelines/user_etl.yaml", env="prod")

# Execute with quality gate
result = execute_with_quality_gate(job_id, run_conf={"date": "2024-01-01"})
```

**Pipeline YAML format:**

```yaml
name: user_events_etl
description: Daily user events ETL
schedule: "0 2 * * *"
sources:
  - urn: "urn:li:dataset:(trino,tpch.tiny.orders,prod)"
    name: orders
sinks:
  - urn: "urn:li:dataset:(duckdb,analytics.fact_orders,prod)"
    name: fact_orders
transforms:
  - name: clean
    sql: "SELECT orderkey, totalprice FROM orders WHERE totalprice > 0"
quality_rules:
  - name: not_null_orderkey
    column: orderkey
    type: not_null
    is_blocking: true
lineage:
  column_level: false
```

---

## Metadata & Lineage

```python
from odep.adapters.openmeta.adapter import OpenMetaAdapter
from odep.config import MetadataConfig
from odep.models import DatasetMetadata, LineageEdge

adapter = OpenMetaAdapter(MetadataConfig())

# Register a dataset
urn = adapter.register_dataset(DatasetMetadata(
    urn="urn:li:dataset:(trino,tpch.tiny.orders,prod)",
    name="orders", platform="trino", env="prod",
    schema=[{"name": "orderkey", "type": "BIGINT"}],
    owner="data-team",
))

# Record lineage
adapter.create_lineage([LineageEdge(
    source_urn="urn:li:dataset:(trino,tpch.tiny.orders,prod)",
    target_urn="urn:li:dataset:(duckdb,analytics.fact_orders,prod)",
)])

# Traverse upstream
graph = adapter.get_full_upstream("urn:li:dataset:(duckdb,analytics.fact_orders,prod)")

# Quality
adapter.record_quality_check(urn, "row_count", passed=True, metrics={"rows": 1500})
score = adapter.get_quality_score(urn)  # 100.0

# Governance
adapter.apply_tag(urn, "PII")           # triggers encryption policy
adapter.grant_access(urn, "alice", "read")
adapter.check_access("alice", urn, "read")  # True
```

---

## API Gateway

Start the API server:

```bash
pip install uvicorn
uvicorn odep.api.app:app --reload --port 8000
```

Endpoints:

```
GET  /health                          — liveness check
GET  /metrics                         — Prometheus metrics
GET  /metadata/dataset/{urn}          — get dataset (JWT required)
GET  /metadata/search?q=...           — search catalog (JWT required)
DELETE /metadata/dataset/{urn}        — soft-delete (JWT required)
POST /orchestration/run/{job_id}      — trigger run (JWT required)
GET  /orchestration/status/{run_id}   — get run status (JWT required)
POST /execution/submit                — submit job (JWT required)
GET  /execution/status/{job_handle}   — get job status (JWT required)
```

All mutating operations emit structured audit log entries via `odep.audit` logger.

---

## Pipeline Templates

```bash
odep template list
# batch-pipeline        Daily batch ETL pipeline with DuckDB/Spark
# streaming-pipeline    Real-time streaming pipeline with Flink/Kafka
# ml-feature-pipeline   ML feature engineering pipeline with feature store
# dbt-project           dbt project with ODEP integration

odep template use batch-pipeline --name=my_etl
```

Templates live in `odep/templates/` and use Cookiecutter.

---

## Running Tests

```bash
# All integration tests (no Docker needed)
pytest tests/integration/ -v

# Specific test
pytest tests/integration/test_full_pipeline_cycle.py -v
```

All 4 integration tests pass with in-memory adapters — no external services required.

---

## Adapter Status

| Layer | Adapter | Status | Docker Profile |
|---|---|---|---|
| Metadata | OpenMetaAdapter | Full implementation | `full` |
| Orchestration | AirflowAdapter | Full implementation | `full` |
| Orchestration | DagsterAdapter | Full implementation (GraphQL API) | `dagster` |
| Orchestration | PrefectAdapter | Full implementation (REST API) | `prefect` |
| Orchestration | TemporalAdapter | Full implementation (SDK — `pip install temporalio`) | `temporal` |
| Execution | DuckDbAdapter | Full implementation | none (in-process) |
| Execution | SparkAdapter | Full implementation | `spark` |
| Execution | TrinoAdapter | Full implementation | `trino` |
| Execution | FlinkAdapter | Full implementation (REST + SQL Gateway) | `flink` |
| Execution | DbtAdapter | Full implementation (CLI subprocess) | none (install dbt-core) |
| Execution | ClickHouseAdapter | Full implementation (HTTP API) | `clickhouse` |
| Execution | MetaMindAdapter | Full implementation (REST API) | none (external service) |

---

## Cloud Deployment (Terraform)

Modules for AWS, GCP, and Azure under `infra/terraform/`:

```bash
# AWS
cd infra/terraform/aws
terraform init
terraform apply -var="metadata_engine=openmeta" -var="orchestration_engine=airflow"

# GCP
cd infra/terraform/gcp
terraform apply -var="gcp_project=my-project" -var="orchestration_engine=airflow"

# Azure
cd infra/terraform/azure
terraform apply -var="resource_group_name=odep-rg"
```

All modules expose a unified `metadata_endpoint` output regardless of engine.

---

## Project Structure

```
odep/
├── interfaces/         Protocol definitions (MetadataService, Orchestrator, ExecutionEngine)
├── models.py           Pydantic data models (DatasetMetadata, LineageEdge, JobConfig, …)
├── config.py           Pydantic settings (OdepConfig, ExecutionConfig, …)
├── exceptions.py       Typed exceptions (AdapterNotFoundError, QualityGateFailure, …)
├── factory.py          AdapterFactory — runtime engine resolver
├── adapters/
│   ├── openmeta/       MetadataService — DataHub + OpenLineage + GE + OPA
│   ├── airflow/        Orchestrator — Airflow REST API v1
│   ├── duckdb/         ExecutionEngine — in-process DuckDB
│   ├── spark/          ExecutionEngine — Apache Spark (local or cluster)
│   ├── trino/          ExecutionEngine — Trino distributed SQL
│   ├── metamind/       ExecutionEngine — MetaMind v2.0 AI optimizer
│   ├── dagster/        Stub
│   ├── prefect/        Stub
│   ├── temporal/       Stub
│   ├── flink/          Stub
│   ├── dbt/            Stub
│   └── clickhouse/     Stub
├── sdk/
│   ├── pipeline.py     Pipeline.from_file(), validate(), extract_lineage_edges()
│   ├── deploy.py       deploy_pipeline() — Algorithm 2
│   └── execute.py      execute_with_quality_gate() — Algorithm 3
├── cli/
│   ├── main.py         odep CLI entry point
│   └── commands/       local, template, config command groups
├── api/
│   ├── app.py          FastAPI application
│   ├── auth.py         JWT authentication (python-jose)
│   ├── middleware.py   Rate limiting
│   ├── audit.py        Audit logging
│   ├── observability.py OTel tracing + Prometheus metrics
│   └── routes/         metadata, orchestration, execution routers
└── templates/
    ├── batch-pipeline/
    ├── streaming-pipeline/
    ├── ml-feature-pipeline/
    └── dbt-project/

infra/
├── terraform/aws/      AWS Terraform module (MWAA, ECS, Secrets Manager)
├── terraform/gcp/      GCP Terraform module (Cloud Composer, GKE)
└── terraform/azure/    Azure Terraform module (AKS, Key Vault)

trino/
└── catalog/            Trino catalog config files (tpch, memory, iceberg)

tests/
└── integration/        4 integration tests (all passing, no Docker needed)

docs/
└── TEST_PLAN.md        130 test cases with inputs and expected outcomes
```
