<div align="center">

# ⚡ ODEP — Open Data Engineering Platform

**A modular, batteries-included data engineering platform built for the complete data lifecycle.**

Engine-agnostic by design. Every component has a default open-source implementation and a stable Python Protocol interface — swap any layer without touching pipeline code.

[![Python](https://img.shields.io/badge/python-3.10%2B-blue?logo=python)](https://www.python.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-ruff-black)](https://github.com/astral-sh/ruff)

</div>

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
- [Execution Engines](#execution-engines)
- [Orchestration](#orchestration)
- [Metadata & Lineage](#metadata--lineage)
- [Data Quality](#data-quality)
- [MetaMind Integration](#metamind-integration)
- [Python SDK](#python-sdk)
- [Local Development Stack](#local-development-stack)
- [Configuration](#configuration)
- [Cloud Deployment](#cloud-deployment)
- [Pipeline Templates](#pipeline-templates)
- [Security](#security)
- [Observability](#observability)
- [Testing](#testing)
- [Project Structure](#project-structure)

---

## Overview

ODEP provides a unified interface across the entire data stack — from ingestion and transformation to quality, lineage, and governance. The platform is built around three stable Python Protocol interfaces:

| Layer | Protocol | Default | Alternatives |
|---|---|---|---|
| Metadata | `MetadataService` | OpenMeta (DataHub + OpenLineage + GE + OPA) | — |
| Orchestration | `Orchestrator` | Airflow | Dagster, Prefect, Temporal |
| Execution | `ExecutionEngine` | DuckDB (local) | Spark, Flink, dbt, Trino, ClickHouse, MetaMind |

**Core philosophy:** Batteries-included but swappable. Change your execution engine with one config line — no pipeline code changes required.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    User Interface Layer                      │
│         CLI (odep)    │    Python SDK    │    Web UI         │
└──────────────────────────────┬──────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│              API Gateway (FastAPI)                           │
│     OIDC/OAuth2 Auth  │  Rate Limiting  │  Audit Logging    │
└──────────────────────────────┬──────────────────────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ Metadata Layer │  │Orchestration Layer│  │ Execution Layer  │
│  MetadataService│  │   Orchestrator   │  │ ExecutionEngine  │
│  Protocol      │  │   Protocol       │  │ Protocol         │
│                │  │                  │  │                  │
│  OpenMeta      │  │  Airflow ✓       │  │  DuckDB ✓        │
│  (DataHub +    │  │  Dagster         │  │  Spark           │
│   OpenLineage +│  │  Prefect         │  │  Flink           │
│   GE + OPA)    │  │  Temporal        │  │  dbt             │
└────────────────┘  └──────────────────┘  │  Trino           │
                                          │  ClickHouse      │
                                          │  MetaMind v2.0   │
                                          └──────────────────┘
                                                   │
                               ┌───────────────────▼──────────────────┐
                               │           Storage Layer               │
                               │  Lakehouse: Iceberg / Delta / Hudi    │
                               │  Formats:  Parquet / ORC / Avro / JSON│
                               │  Object:   MinIO / S3 / GCS / Azure   │
                               └──────────────────────────────────────┘
```

---

## Quick Start

### Install

```bash
pip install odep
# or from source
git clone https://github.com/your-org/odep && cd odep
pip install -e ".[dev]"
```

### Initialize a project

```bash
odep init my-platform --engine=duckdb
cd my-platform
```

### Start the local stack

```bash
# Full stack: Airflow + DataHub + Marquez + Spark + MinIO (~8GB RAM)
odep local up

# Minimal stack: Marquez only (~1GB RAM)
odep local up --profile=minimal
```

### Deploy and run your first pipeline

```bash
odep deploy pipelines/user_events.yaml --env=local
odep run user_events_etl
```

---

## CLI Reference

```
odep [COMMAND] [OPTIONS]
```

| Command | Description |
|---|---|
| `odep init [name]` | Scaffold a new project with `.odep.env`, `odep.yaml`, `docker-compose.yml` |
| `odep deploy [path]` | Deploy a pipeline to the configured orchestrator |
| `odep run [job-id]` | Trigger a pipeline run |
| `odep run [job-id] --backfill --start DATE --end DATE` | Trigger a date-range backfill |
| `odep logs [run-id]` | Stream logs for a run |
| `odep test [path] --suite [suite.yaml]` | Run a data quality suite |
| `odep lineage [urn]` | Show upstream lineage for a dataset |
| `odep cost` | Estimate pipeline run cost |
| `odep local up [--profile]` | Start the local Docker Compose stack |
| `odep local down [--volumes]` | Stop the local stack |
| `odep template list` | List available pipeline templates |
| `odep template use [name] --name [pipeline]` | Generate a pipeline from a template |
| `odep config get [key]` | Read a config value |
| `odep config set [key=value]` | Write a config value to `.odep.env` |
| `odep dq generate [data]` | Auto-generate a quality rule suite from a data file |
| `odep dq run [data] --suite [file]` | Run a quality suite |

### Examples

```bash
# Deploy with a cron schedule
odep deploy pipelines/orders.yaml --env=prod --schedule="0 2 * * *"

# Backfill January 2024
odep run orders_etl --backfill --start=2024-01-01 --end=2024-02-01

# Stream last 200 log lines
odep logs run_abc123 --tail=200

# Run quality checks with LLM evaluation
odep test orders.csv --suite rules/orders.yaml --llm-evaluate

# Show upstream lineage
odep lineage "urn:li:dataset:(bigquery,prod.analytics.fact_orders,prod)"

# Switch execution engine
odep config set ODEP_EXECUTION__DEFAULT_ENGINE=metamind
```

---

## Execution Engines

All engines implement the `ExecutionEngine` Protocol — swap via config, no code changes.

### DuckDB (default — local)

Zero-dependency local execution. Ideal for development and small datasets.

```python
from odep.adapters.duckdb.adapter import DuckDbAdapter
from odep.config import ExecutionConfig
from odep.models import JobConfig, EngineType

adapter = DuckDbAdapter(ExecutionConfig())
handle = adapter.submit(JobConfig(
    engine=EngineType.SQL,
    code="SELECT COUNT(*) FROM read_parquet('data/*.parquet')",
    dependencies=[], cluster_config={}, io_config={}
))
result = adapter.wait_for_completion(handle)
print(result.records_processed)
```

### Apache Spark

```bash
odep config set ODEP_EXECUTION__DEFAULT_ENGINE=spark
odep config set ODEP_EXECUTION__SPARK_MASTER=spark://localhost:7077
```

### Apache Flink

```bash
odep config set ODEP_EXECUTION__DEFAULT_ENGINE=flink
odep config set ODEP_EXECUTION__FLINK_JOBMANAGER_URL=http://localhost:8083
```

### dbt

```bash
odep config set ODEP_EXECUTION__DEFAULT_ENGINE=dbt
odep config set ODEP_EXECUTION__DBT_PROJECT_DIR=./dbt_project
```

### Trino

```bash
odep config set ODEP_EXECUTION__DEFAULT_ENGINE=trino
odep config set ODEP_EXECUTION__TRINO_HOST=localhost
odep config set ODEP_EXECUTION__TRINO_PORT=8082
```

### ClickHouse

```bash
odep config set ODEP_EXECUTION__DEFAULT_ENGINE=clickhouse
odep config set ODEP_EXECUTION__CLICKHOUSE_HOST=localhost
```

---

## Orchestration

All orchestrators implement the `Orchestrator` Protocol.

### Apache Airflow (default)

```bash
odep config set ODEP_ORCHESTRATION__ENGINE=airflow
odep config set ODEP_ORCHESTRATION__AIRFLOW_URL=http://localhost:8090
```

### Dagster

```bash
odep config set ODEP_ORCHESTRATION__ENGINE=dagster
odep config set ODEP_ORCHESTRATION__DAGSTER_URL=http://localhost:3000
```

### Prefect

```bash
odep config set ODEP_ORCHESTRATION__ENGINE=prefect
odep config set ODEP_ORCHESTRATION__PREFECT_URL=http://localhost:4200
```

### Temporal

```bash
odep config set ODEP_ORCHESTRATION__ENGINE=temporal
odep config set ODEP_ORCHESTRATION__TEMPORAL_HOST=localhost:7233
```

---

## Metadata & Lineage

The `MetadataService` Protocol covers catalog, lineage, quality, and governance in one interface.

```python
from odep.factory import get_metadata_adapter
from odep.config import OdepConfig
from odep.models import DatasetMetadata, LineageEdge

config = OdepConfig()
meta = get_metadata_adapter(config.metadata.engine, config.metadata)

# Register a dataset
urn = meta.register_dataset(DatasetMetadata(
    urn="urn:li:dataset:(bigquery,prod.analytics.fact_orders,prod)",
    name="fact_orders",
    platform="bigquery",
    env="prod",
    schema=[{"name": "order_id", "type": "STRING"}, {"name": "amount", "type": "FLOAT"}],
    owner="data-team",
))

# Record lineage
meta.create_lineage([LineageEdge(
    source_urn="urn:li:dataset:(bigquery,prod.raw.orders,prod)",
    target_urn=urn,
    transformation="SELECT order_id, SUM(amount) FROM raw.orders GROUP BY 1",
)])

# Traverse upstream
graph = meta.get_full_upstream(urn, max_depth=5)

# Governance
meta.apply_tag(urn, "PII")
meta.check_access("alice@company.com", urn, "read")  # → True/False
```

---

## Data Quality

ODEP includes a native in-process data quality engine — no external service required.

### Supported rule types

| Rule | Description |
|---|---|
| `not_null` | Column has no null values |
| `unique` | Column has no duplicate values |
| `min` / `max` | Numeric range bounds |
| `min_length` / `max_length` | String length bounds |
| `regex` | Values match a regex pattern |
| `accepted_values` | Values are in an allowed set |
| `row_count_min` / `row_count_max` | Table row count bounds |
| `freshness` | Latest timestamp is within N hours |
| `completeness` | Non-null percentage meets threshold |
| `custom_sql` | Arbitrary DuckDB SQL expression |

### Define a suite in YAML

```yaml
name: orders_quality
dataset_urn: "urn:li:dataset:(bigquery,prod.analytics.fact_orders,prod)"
rules:
  - name: order_id_not_null
    rule_type: not_null
    column: order_id
    severity: critical
    blocking: true

  - name: amount_positive
    rule_type: min
    column: amount
    params:
      min: 0.0
    severity: error
    blocking: true

  - name: status_valid
    rule_type: accepted_values
    column: status
    params:
      values: [pending, processing, shipped, delivered, cancelled]
    severity: warning
    blocking: false

  - name: data_freshness
    rule_type: freshness
    column: created_at
    params:
      max_age_hours: 25
    severity: error
    blocking: true
```

### Run via CLI

```bash
odep test orders.csv --suite rules/orders.yaml
# exits non-zero if any blocking rule fails

# Auto-generate rules from data
odep dq generate orders.csv -o rules/orders.yaml
```

### Run via SDK

```python
from odep.dq.engine import NativeQualityEngine
from odep.dq.serializer import load_suite
import pandas as pd

engine = NativeQualityEngine()
suite = load_suite("rules/orders.yaml")
df = pd.read_csv("orders.csv")

result = engine.run_suite(suite, df)
print(f"Quality score: {result.quality_score:.1f}%")
print(f"Passed: {result.passed}/{result.total_rules}")

# Raise on blocking failure
engine.assert_suite(suite, df)
```

---

## MetaMind Integration

MetaMind v2.0 is available as an opt-in `ExecutionEngine` adapter. It routes SQL through an AI optimization pipeline — Cascades optimizer, Redis plan cache, RLS rewriter, workload classifier — and dispatches to the best backend automatically.

> MetaMind is an **execution engine**, not a metadata replacement. Lineage, quality, and governance continue to use the `MetadataService` layer (OpenMeta by default).

### Enable MetaMind

```bash
odep config set ODEP_EXECUTION__DEFAULT_ENGINE=metamind
odep config set ODEP_METAMIND__METAMIND_URL=http://localhost:8000
odep config set ODEP_METAMIND__TENANT_ID=my-tenant
odep config set ODEP_METAMIND__API_TOKEN=<your-token>
```

### What MetaMind provides

```python
from odep.adapters.metamind.adapter import MetaMindAdapter
from odep.config import MetaMindConfig
from odep.models import JobConfig, EngineType

adapter = MetaMindAdapter(MetaMindConfig())
handle = adapter.submit(JobConfig(
    engine=EngineType.SQL,
    code="SELECT customer_id, SUM(amount) FROM orders GROUP BY 1",
    dependencies=[], cluster_config={}, io_config={}
))

metrics = adapter.get_metrics(handle)
# {
#   "optimization_tier": 1,          # Cascades depth
#   "cache_hit": True,               # Redis plan cache
#   "workload_type": "OLAP",         # Classified workload
#   "backend_used": "duckdb",        # Actual backend routed to
#   "optimization_ms": 12.4,         # Optimizer time
#   "plan_cost": 0.0034,             # Estimated cost
#   "flags_used": ["F09", "F24"]     # Active feature flags
# }
```

---

## Python SDK

```python
from odep.sdk import Pipeline, deploy_pipeline, execute_with_quality_gate
from odep.config import OdepConfig

config = OdepConfig()

# Load and validate a pipeline
pipeline = Pipeline.from_file("pipelines/orders.yaml")
pipeline.validate()  # local — no network calls

# Deploy
job_id = deploy_pipeline("pipelines/orders.yaml", env="prod", config=config)

# Execute with quality gate
result = execute_with_quality_gate(job_id, run_conf={}, config=config)
print(f"Processed {result.records_processed:,} records in {result.execution_time_ms}ms")
```

### AdapterFactory

```python
from odep.factory import get_metadata_adapter, get_orchestrator_adapter, get_execution_adapter
from odep.config import OdepConfig

config = OdepConfig()

meta = get_metadata_adapter("openmeta", config.metadata)
orch = get_orchestrator_adapter("airflow", config.orchestration)
exec = get_execution_adapter("duckdb", config.execution)
# or swap to MetaMind:
exec = get_execution_adapter("metamind", config.metamind)
```

---

## Local Development Stack

Docker Compose profiles let you start only what you need.

### Profiles

| Profile | Services | RAM |
|---|---|---|
| `minimal` | Marquez (lineage) | ~1 GB |
| `full` | Airflow + DataHub + Marquez + Spark + MinIO + Trino + Flink + ClickHouse | ~8 GB |
| `spark` | Spark master + worker | ~2 GB |
| `trino` | Trino coordinator | ~1 GB |
| `flink` | Flink JobManager + TaskManager + SQL Gateway | ~2 GB |
| `clickhouse` | ClickHouse server | ~1 GB |
| `dagster` | Dagster webserver + daemon | ~1 GB |
| `prefect` | Prefect server | ~512 MB |
| `temporal` | Temporal + UI | ~512 MB |

```bash
# Start full stack
odep local up
# or directly:
docker compose --profile full up -d

# Start only Trino + Flink
docker compose --profile trino --profile flink up -d

# Stop everything and remove volumes
odep local down --volumes
```

### Service URLs (full profile)

| Service | URL |
|---|---|
| Airflow | http://localhost:8090 |
| DataHub | http://localhost:8080 |
| Marquez UI | http://localhost:5001 |
| Spark Master | http://localhost:8081 |
| Trino | http://localhost:8082 |
| Flink | http://localhost:8083 |
| ClickHouse | http://localhost:8123 |
| MinIO Console | http://localhost:9001 |

---

## Configuration

All config is loaded from `.odep.env` (or environment variables). Nested keys use `__` as delimiter.

```bash
# .odep.env
ODEP_METADATA__ENGINE=openmeta
ODEP_METADATA__DATAHUB_URL=http://localhost:8080
ODEP_METADATA__MARQUEZ_URL=http://localhost:5000

ODEP_ORCHESTRATION__ENGINE=airflow
ODEP_ORCHESTRATION__AIRFLOW_URL=http://localhost:8090
ODEP_ORCHESTRATION__AIRFLOW_USERNAME=admin
ODEP_ORCHESTRATION__AIRFLOW_PASSWORD=admin

ODEP_EXECUTION__DEFAULT_ENGINE=duckdb
ODEP_EXECUTION__SPARK_MASTER=local[*]
ODEP_EXECUTION__TRINO_HOST=localhost
ODEP_EXECUTION__TRINO_PORT=8082

ODEP_METAMIND__METAMIND_URL=http://localhost:8000
ODEP_METAMIND__TENANT_ID=default
ODEP_METAMIND__API_TOKEN=
```

### Valid engine values

| Config key | Valid values |
|---|---|
| `ODEP_METADATA__ENGINE` | `openmeta` |
| `ODEP_ORCHESTRATION__ENGINE` | `airflow`, `dagster`, `prefect`, `temporal` |
| `ODEP_EXECUTION__DEFAULT_ENGINE` | `duckdb`, `spark`, `flink`, `dbt`, `trino`, `clickhouse`, `metamind` |

---

## Cloud Deployment

Terraform modules are provided for AWS, GCP, and Azure under `infra/terraform/`.

```bash
cd infra/terraform/aws
terraform init
terraform apply \
  -var="metadata_engine=openmeta" \
  -var="orchestration_engine=airflow" \
  -var="environment=prod"
```

### Variables

| Variable | Default | Description |
|---|---|---|
| `metadata_engine` | `openmeta` | `openmeta` or `metamind` |
| `orchestration_engine` | `airflow` | `airflow` |
| `environment` | `dev` | `dev`, `staging`, `prod` |
| `region` | cloud-specific | Deployment region |

### Outputs

All modules expose a unified `metadata_endpoint` output regardless of which engine is selected.

```bash
terraform output metadata_endpoint
# → https://datahub.internal.company.com
```

---

## Pipeline Templates

Generate production-ready pipelines with Cookiecutter.

```bash
odep template list
# batch-pipeline        Standard batch ETL with DuckDB/Spark
# streaming-pipeline    Kafka + Flink streaming
# ml-feature-pipeline   Feature engineering with quality gates
# dbt-project           dbt project with ODEP wrapper

odep template use batch-pipeline --name=user_events_etl
```

Each generated template includes:
- `pipeline.py` or `odep.yaml` — pipeline definition
- `requirements.txt` — dependencies
- `tests/` — quality suite and unit tests

---

## Security

- **Authentication**: OIDC/OAuth2 JWT on all API Gateway endpoints. CLI uses device-flow OAuth2 (`odep login`).
- **RBAC**: Roles `viewer`, `editor`, `owner`, `admin` with actions `read`, `write`, `delete`, `tag` enforced at the metadata layer.
- **PII protection**: `apply_tag(urn, "PII")` triggers automatic column-level encryption via envelope encryption (KMS-managed keys).
- **Secrets**: Never stored in `odep.yaml`. Read from environment variables or a secrets manager (Vault, AWS Secrets Manager).
- **Audit logging**: All mutating operations emit structured OpenTelemetry log entries with user identity, timestamp, and resource URN.
- **CI scanning**: `pip-audit` and `trivy` run on all Docker images before publishing.

---

## Observability

ODEP is instrumented with OpenTelemetry and Prometheus out of the box.

- **Traces**: All pipeline operations (deploy, run, quality check, lineage record) emit OTel spans.
- **Metrics**: `/metrics` endpoint exposes `runtime_ms`, `rows_processed`, `quality_score` per pipeline run.
- **Structured logs**: All mutating operations emit JSON audit logs with user, timestamp, and resource URN.

```bash
# Prometheus metrics
curl http://localhost:8000/metrics

# Grafana dashboards are included in infra/monitoring/grafana/
```

---

## Testing

```bash
# Unit tests
pytest tests/unit/ -v

# Property-based tests (hypothesis)
pytest tests/property/ -v

# Integration tests (requires Docker)
pytest tests/integration/ -v

# Full suite with coverage
pytest --cov=odep --cov-report=term-missing
```

### Property-based tests

ODEP ships with `hypothesis` property tests covering 16 correctness properties including:

- URN idempotency (double-register never creates duplicates)
- Quality score always in `[0.0, 100.0]`
- Lineage traversal terminates on cyclic graphs
- Backfill returns chronologically ordered run IDs
- Engine swap produces identical observable outputs
- Config validation rejects invalid engine names before any adapter is instantiated

---

## Project Structure

```
odep/
├── interfaces/          # Python Protocol definitions
│   └── __init__.py      # MetadataService, Orchestrator, ExecutionEngine
├── adapters/
│   ├── openmeta/        # MetadataService — DataHub + OpenLineage + GE + OPA
│   ├── airflow/         # Orchestrator — Apache Airflow REST API
│   ├── dagster/         # Orchestrator — Dagster GraphQL API
│   ├── prefect/         # Orchestrator — Prefect REST API
│   ├── temporal/        # Orchestrator — Temporal SDK
│   ├── duckdb/          # ExecutionEngine — local DuckDB
│   ├── spark/           # ExecutionEngine — Apache Spark
│   ├── flink/           # ExecutionEngine — Apache Flink SQL Gateway
│   ├── dbt/             # ExecutionEngine — dbt Core
│   ├── trino/           # ExecutionEngine — Trino REST API
│   ├── clickhouse/      # ExecutionEngine — ClickHouse HTTP API
│   └── metamind/        # ExecutionEngine — MetaMind v2.0 REST API
├── dq/                  # Native data quality engine
│   ├── engine.py        # NativeQualityEngine (13 rule types)
│   ├── models.py        # QualityRule, QualitySuite, SuiteResult
│   ├── runner.py        # Suite runner with quality gate
│   └── serializer.py    # YAML suite loader/writer
├── sdk/
│   ├── pipeline.py      # Pipeline.from_file(), validate(), extract_lineage_edges()
│   ├── deploy.py        # deploy_pipeline()
│   └── execute.py       # execute_with_quality_gate()
├── cli/
│   ├── main.py          # odep CLI entry point
│   └── commands/        # init, local, template, config, dq
├── api/
│   ├── auth.py          # OIDC/OAuth2 JWT middleware
│   ├── middleware.py     # Rate limiting
│   ├── audit.py         # OpenTelemetry audit logging
│   └── routes/          # FastAPI routers
├── ui/                  # React web console
│   └── src/pages/       # Catalog, Lineage, Quality, Orchestration, Execution
├── config.py            # OdepConfig (Pydantic settings)
├── factory.py           # AdapterFactory
├── models.py            # DatasetMetadata, LineageEdge, JobDefinition, JobResult
└── exceptions.py        # Typed exceptions

infra/
├── terraform/
│   ├── aws/             # AWS deployment module
│   ├── gcp/             # GCP deployment module
│   └── azure/           # Azure deployment module
└── monitoring/
    └── grafana/         # Grafana dashboards

tests/
├── unit/                # Unit tests per adapter
├── property/            # hypothesis property-based tests (P1–P16)
└── integration/         # TestContainers integration tests

docker-compose.yml       # Local dev stack (profiles: full, minimal, spark, trino, flink, ...)
pyproject.toml           # Package metadata and dependencies
```

---

## Requirements

- Python ≥ 3.10
- Docker + Docker Compose ≥ 2.20 (for local stack)
- Terraform ≥ 1.7 (for cloud deployment)

---

<div align="center">

Built with ❤️ for data engineers who want control without complexity.

</div>

---

## Author

**Vikas Budde**

- LinkedIn: [linkedin.com/in/vikasbudde](https://in.linkedin.com/in/vikasbudde)
- Email: [vikas.budde@hotmail.com](mailto:vikas.budde@hotmail.com)
