# ODEP Data Quality Engine

The ODEP DQ engine provides automated data quality rule generation, evaluation,
and LLM-powered analysis. It runs on any data source — local files, cloud storage,
Spark clusters, or Trino — without requiring an external DQ service.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                             │
│  CSV · Parquet · ORC · Avro · JSON · JSONL · Delta · Excel      │
│  S3 · GCS · Azure Blob · SQL queries · Spark DF · Trino tables  │
└──────────────────────────┬──────────────────────────────────────┘
                           │  odep/dq/reader.py
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Rule Definitions                            │
│  QualityRule · QualitySuite · RuleType · Severity               │
│  odep/dq/models.py                                              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
  NativeQualityEngine  SparkQualityEngine  TrinoQualityEngine
  (pandas + DuckDB)    (PySpark SQL)       (server-side SQL)
  odep/dq/engine.py    spark_engine.py     trino_engine.py
          │                │                │
          └────────────────┴────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SuiteResult                                │
│  quality_score · passed · failed · blocking_failures · warnings │
│  List[CheckResult] with per-rule metrics and error messages     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
  Metadata Catalog    LLM Advisor      Rule Files
  record_quality_     evaluate_        YAML / JSON
  check()             results()        serializer.py
  OpenMetaAdapter     llm_advisor.py
```

---

## Supported Data Formats

The DQ engine reads data from any of these sources via `odep/dq/reader.py`:

### File Formats

| Format | Extension | Install |
|---|---|---|
| CSV | `.csv` | built-in |
| TSV | `.tsv`, `.tab` | built-in |
| Parquet | `.parquet`, `.pq` | `pip install pyarrow` |
| ORC | `.orc` | `pip install pyarrow` |
| Avro | `.avro` | `pip install fastavro` or `pyarrow` |
| JSON | `.json` | built-in |
| JSONL / NDJSON | `.jsonl`, `.ndjson` | built-in |
| Excel | `.xlsx`, `.xls` | `pip install openpyxl` |
| Feather / Arrow | `.feather`, `.arrow` | `pip install pyarrow` |
| Delta Lake | directory with `_delta_log/` | `pip install deltalake` |
| Plain text | `.txt` | treated as CSV |

### Remote Storage

DuckDB's `httpfs` extension handles remote reads automatically:

```bash
# S3
odep dq run rules.yaml "SELECT * FROM read_parquet('s3://my-bucket/orders/*.parquet')"

# GCS
odep dq run rules.yaml "SELECT * FROM read_csv_auto('gs://my-bucket/data.csv')"

# Azure Blob
odep dq run rules.yaml "SELECT * FROM read_parquet('az://container/path/data.parquet')"
```

For authenticated access, set credentials in `.odep.env`:
```bash
ODEP_DQ__S3_ACCESS_KEY=...
ODEP_DQ__S3_SECRET_KEY=...
```

### Query-Based Sources

Any SQL query is executed via DuckDB (for local/remote files) or passed directly
to Trino/Spark:

```bash
# DuckDB SQL (reads local files, S3, etc.)
odep dq run rules.yaml "SELECT * FROM read_parquet('orders/*.parquet') WHERE dt='2024-01-01'"

# Trino table
odep dq run rules.yaml "" --engine trino --trino-table tpch.tiny.orders

# Spark (reads from HDFS, S3, local)
odep dq run rules.yaml "s3://bucket/orders/" --engine spark
```

### Python SDK

```python
from odep.dq.reader import read_data, read_data_spark, read_data_trino

# Auto-detect format from extension
df = read_data("orders.parquet")
df = read_data("events.avro")
df = read_data("data.orc")
df = read_data("logs.jsonl")

# SQL query via DuckDB
df = read_data("SELECT * FROM read_parquet('s3://bucket/orders/*.parquet')")

# Sample for LLM prompts (avoids loading full dataset)
df = read_data("orders.parquet", sample_rows=100)

# Spark DataFrame
spark_df = read_data_spark("s3://bucket/orders/", format="parquet")
spark_df = read_data_spark("s3://bucket/events/", format="delta")

# Trino query result
df = read_data_trino("SELECT * FROM tpch.tiny.orders LIMIT 1000",
                     host="localhost", port=8082)
```

---

## Rule Types

All 13 rule types work across all three engines (Native, Spark, Trino):

| Rule Type | Description | Params |
|---|---|---|
| `not_null` | No null values in column | — |
| `unique` | No duplicate values | — |
| `min` | Column minimum >= threshold | `min: float` |
| `max` | Column maximum <= threshold | `max: float` |
| `min_length` | String length >= threshold | `min_length: int` |
| `max_length` | String length <= threshold | `max_length: int` |
| `regex` | Values match regex pattern | `pattern: str` |
| `accepted_values` | Values in allowed set | `values: list` |
| `row_count_min` | Table has >= N rows | `min_rows: int` |
| `row_count_max` | Table has <= N rows | `max_rows: int` |
| `freshness` | Latest timestamp within N hours | `max_age_hours: float` |
| `completeness` | % non-null >= threshold | `min_pct: float` |
| `custom_sql` | SQL expression returns true | `sql: str` |

Each rule has a `severity`:
- `blocking` — raises `QualityGateFailure` on failure, pipeline stops
- `warning` — records failure but execution continues

---

## Execution Engines

### NativeQualityEngine (default)

Evaluates rules using pandas + DuckDB. No cluster required.

```python
from odep.dq.engine import NativeQualityEngine
from odep.dq.models import QualitySuite, QualityRule
from odep.dq.reader import read_data

df = read_data("orders.parquet")

suite = QualitySuite(name="orders_suite", dataset_urn="urn:li:dataset:(duckdb,orders,prod)")
suite.add_rule(QualityRule.not_null("order_id"))
suite.add_rule(QualityRule.min_value("amount", 0.0))
suite.add_rule(QualityRule.custom_sql("no_negatives", "SELECT COUNT(*) = 0 FROM data WHERE amount < 0"))

engine = NativeQualityEngine()
result = engine.run_suite(suite, df)
print(f"Score: {result.quality_score:.1f}%")
```

**When to use:** Local development, small-to-medium datasets (< 100M rows), CI/CD pipelines.

### SparkQualityEngine

Evaluates rules as distributed Spark SQL aggregations. Data stays in the cluster.

```python
from odep.dq.spark_engine import SparkQualityEngine
from odep.dq.reader import read_data_spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://localhost:7077").getOrCreate()

# Read from any Spark-supported format
spark_df = read_data_spark("s3://bucket/orders/", format="parquet", spark=spark)
# or: spark_df = read_data_spark("hdfs://namenode/data/events/", format="orc")
# or: spark_df = read_data_spark("s3://bucket/delta-table/", format="delta")

engine = SparkQualityEngine(spark=spark)
result = engine.run_suite(suite, spark_df)
```

**CLI:**
```bash
# Start Spark cluster
docker compose --profile spark up -d

# Run DQ on a Parquet file via Spark
odep dq run rules/orders.yaml s3://bucket/orders/ --engine spark

# Run on a local Parquet file
odep dq run rules/orders.yaml orders.parquet --engine spark
```

**When to use:** Large datasets (> 100M rows), data already in Spark/HDFS/S3, Iceberg/Delta/Hudi tables.

**Supported formats via Spark:**
- Parquet, ORC, Avro, CSV, JSON
- Delta Lake (`format="delta"`)
- Apache Iceberg (`format="iceberg"` — requires Iceberg Spark runtime)
- Apache Hudi (`format="hudi"`)

### TrinoQualityEngine

Evaluates rules as server-side SQL on Trino. Zero data transfer — only scalar
aggregation results come back to Python.

```python
from odep.dq.trino_engine import TrinoQualityEngine

engine = TrinoQualityEngine(
    table="tpch.tiny.orders",   # fully-qualified: catalog.schema.table
    host="localhost",
    port=8082,
)
result = engine.run_suite(suite)  # no data argument needed
```

**With a subquery (filter/join before DQ):**
```python
engine = TrinoQualityEngine(
    table="(SELECT * FROM tpch.tiny.orders WHERE orderdate >= DATE '2024-01-01') t",
    host="localhost",
    port=8082,
)
result = engine.run_suite(suite)
```

**CLI:**
```bash
# Start Trino
docker compose --profile trino up -d

# Run DQ on a Trino table
odep dq run rules/orders.yaml "" --engine trino --trino-table tpch.tiny.orders

# With custom host/port
odep dq run rules/orders.yaml "" --engine trino \
  --trino-table "analytics.prod.fact_orders" \
  --trino-host trino.internal \
  --trino-port 8080
```

**When to use:** Very large tables (billions of rows), data already in Trino-accessible
catalogs (Iceberg, Hive, Delta, PostgreSQL, etc.), when you want zero data movement.

**Trino catalogs supported out of the box:**
- `tpch` — built-in benchmark data
- `memory` — in-memory tables
- Any catalog configured in `trino/catalog/` (Iceberg, PostgreSQL, MySQL, etc.)

---

## LLM-Powered Rule Generation

The LLM advisor analyses your data schema and sample rows to generate a complete
`QualitySuite` automatically.

### Setup

```bash
# OpenAI
export OPENAI_API_KEY=sk-...
export ODEP_LLM__PROVIDER=openai
export ODEP_LLM__MODEL=gpt-4o

# Anthropic Claude
export ODEP_LLM__PROVIDER=anthropic
export ODEP_LLM__MODEL=claude-3-5-sonnet-20241022
export ODEP_LLM__API_KEY=sk-ant-...

# Ollama (local, no API key needed)
export ODEP_LLM__PROVIDER=ollama
export ODEP_LLM__MODEL=llama3.1
export ODEP_LLM__BASE_URL=http://localhost:11434/v1
```

### Generate rules from any file format

```bash
# From CSV
odep dq generate orders.csv --urn urn:li:dataset:(duckdb,orders,prod) -o rules/orders.yaml

# From Parquet
odep dq generate events.parquet --name user_events -o rules/events.yaml

# From Avro
odep dq generate transactions.avro --description "Payment transactions" -o rules/transactions.yaml

# From ORC
odep dq generate warehouse.orc -o rules/warehouse.yaml

# From a SQL query (samples data for the prompt)
odep dq generate "SELECT * FROM tpch.tiny.orders LIMIT 500" --name orders -o rules/orders.yaml

# From S3
odep dq generate "SELECT * FROM read_parquet('s3://bucket/orders/*.parquet') LIMIT 200" -o rules/orders.yaml
```

### Python SDK

```python
from odep.dq.llm_advisor import LLMAdvisor
from odep.dq.reader import read_data

df = read_data("orders.parquet", sample_rows=100)

advisor = LLMAdvisor.from_config()
suite = advisor.generate_rules(
    data=df,
    urn="urn:li:dataset:(duckdb,orders,prod)",
    table_name="orders",
    description="Daily order transactions from the e-commerce platform",
    sample_rows=20,
)

print(f"Generated {len(suite.rules)} rules")
for rule in suite.rules:
    print(f"  {rule.name} ({rule.rule_type.value}) — {rule.severity.value}")
```

### What the LLM generates

The LLM receives:
- Column names and data types
- Null percentages per column
- Min/max/mean/std for numeric columns
- Top-5 value frequencies for categorical columns
- Sample rows (configurable, default 10)

It generates rules covering:
- Null checks for required fields (primary keys, foreign keys)
- Uniqueness for identifier columns
- Value ranges for numeric columns based on observed data
- Regex patterns for structured strings (emails, phone numbers, codes)
- Accepted values for low-cardinality categoricals
- Completeness thresholds for optional columns
- Freshness checks for timestamp columns
- Custom SQL for complex business invariants

---

## LLM Result Evaluation

After running a suite, the LLM can analyse the results and provide recommendations:

```bash
# Run suite and immediately evaluate with LLM
odep dq run rules/orders.yaml orders.parquet --llm-evaluate

# Save results first, evaluate later
odep dq run rules/orders.yaml orders.parquet -o results.json
odep dq evaluate results.json
```

**Example LLM output:**
```
## Executive Summary
The orders dataset has a quality score of 62.5% with 3 blocking failures.
The primary concern is negative amounts and invalid status values, suggesting
upstream data entry issues or a recent schema change.

## Critical Issues
1. min_amount [BLOCKING]: 2 orders have negative amounts (-5.00, -12.50).
   Likely cause: refund records being included without a separate refund flag.
   Fix: Add a `transaction_type` column and filter refunds before DQ checks.

2. accepted_status [BLOCKING]: 1 record has status='unknown'.
   Likely cause: new status value added to source system without updating DQ rules.
   Fix: Add 'unknown' to accepted values or investigate the source system change.

## Recommended Actions
1. (Immediate) Investigate negative amounts — check if refunds need separate handling
2. (This week) Update accepted_values rule to include new status values
3. (Ongoing) Add a completeness rule for customer_email (currently 80% filled)
```

---

## Anomaly Detection

```bash
# Detect anomalies and export suggested rules
odep dq anomalies orders.parquet --name orders --export-rules rules/anomaly_rules.yaml

# With a baseline for drift detection
odep dq anomalies orders.parquet --baseline baseline_stats.json --name orders
```

```python
from odep.dq.llm_advisor import LLMAdvisor
from odep.dq.reader import read_data

df = read_data("orders.parquet")
advisor = LLMAdvisor.from_config()

result = advisor.detect_anomalies(df, table_name="orders")
for anomaly in result["anomalies"]:
    print(f"[{anomaly['type']}] {anomaly['column']}: {anomaly['description']}")
    if "suggested_rule_obj" in anomaly:
        print(f"  → Suggested rule: {anomaly['suggested_rule_obj'].name}")
```

---

## Rule Files (YAML / JSON)

Rules are stored in portable YAML or JSON files that can be version-controlled,
shared across teams, and executed on any engine.

### YAML format

```yaml
suite:
  name: orders_suite
  dataset_urn: "urn:li:dataset:(duckdb,orders,prod)"
  description: "Quality rules for the orders table"
  rules:
    - name: not_null_order_id
      rule_type: not_null
      column: order_id
      severity: blocking
      params: {}

    - name: unique_order_id
      rule_type: unique
      column: order_id
      severity: blocking
      params: {}

    - name: min_amount
      rule_type: min
      column: amount
      severity: blocking
      params:
        min: 0.0

    - name: max_amount
      rule_type: max
      column: amount
      severity: warning
      params:
        max: 999999.0

    - name: valid_status
      rule_type: accepted_values
      column: status
      severity: blocking
      params:
        values: [active, inactive, pending]

    - name: valid_email
      rule_type: regex
      column: email
      severity: warning
      params:
        pattern: "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"

    - name: row_count_check
      rule_type: row_count_min
      column: null
      severity: blocking
      params:
        min_rows: 1

    - name: freshness_check
      rule_type: freshness
      column: created_at
      severity: blocking
      params:
        max_age_hours: 25.0

    - name: completeness_customer
      rule_type: completeness
      column: customer_id
      severity: blocking
      params:
        min_pct: 99.0

    - name: no_negative_amounts
      rule_type: custom_sql
      column: null
      severity: blocking
      params:
        sql: "SELECT COUNT(*) = 0 FROM data WHERE amount < 0"
```

### CLI commands for rule files

```bash
# Show rules in a file
odep dq show rules/orders.yaml

# Export to JSON
odep dq export rules/orders.yaml --format json -o rules/orders.json

# Export to YAML (convert from JSON)
odep dq export rules/orders.json --format yaml -o rules/orders.yaml

# Print to stdout
odep dq export rules/orders.yaml
```

### Python SDK

```python
from odep.dq.serializer import save_suite, load_suite, load_suites, suite_to_yaml_str
from odep.dq.models import QualitySuite, QualityRule

# Build a suite programmatically
suite = QualitySuite(name="orders_suite", dataset_urn="urn:li:dataset:(duckdb,orders,prod)")
suite.add_rule(QualityRule.not_null("order_id"))
suite.add_rule(QualityRule.min_value("amount", 0.0))
suite.add_rule(QualityRule.accepted_values("status", ["active", "inactive"]))

# Save to YAML
save_suite(suite, "rules/orders.yaml")

# Save to JSON
save_suite(suite, "rules/orders.json")

# Load back
suite = load_suite("rules/orders.yaml")

# Load all suites from a directory
suites = load_suites("rules/")

# Print as YAML string
print(suite_to_yaml_str(suite))
```

---

## Full Workflow Examples

### Example 1: Local CSV with native engine

```bash
# 1. Generate rules from data
odep dq generate orders.csv --urn urn:li:dataset:(duckdb,orders,prod) -o rules/orders.yaml

# 2. Review generated rules
odep dq show rules/orders.yaml

# 3. Run the suite
odep dq run rules/orders.yaml orders.csv

# 4. Run with LLM evaluation
odep dq run rules/orders.yaml orders.csv --llm-evaluate -o results.json

# 5. Evaluate saved results
odep dq evaluate results.json
```

### Example 2: Parquet on S3 with Spark

```bash
# 1. Generate rules from a sample
odep dq generate "SELECT * FROM read_parquet('s3://bucket/orders/*.parquet') LIMIT 500" \
  --name orders -o rules/orders.yaml

# 2. Run full DQ on Spark cluster
docker compose --profile spark up -d
odep dq run rules/orders.yaml s3://bucket/orders/ --engine spark --persist
```

### Example 3: Trino table (billions of rows, zero data transfer)

```bash
# 1. Generate rules from a sample via Trino
odep dq generate "SELECT * FROM analytics.prod.fact_orders LIMIT 1000" \
  --name fact_orders -o rules/fact_orders.yaml

# 2. Run full DQ on Trino (server-side, no data movement)
odep dq run rules/fact_orders.yaml "" \
  --engine trino \
  --trino-table analytics.prod.fact_orders \
  --trino-host trino.internal \
  --persist \
  --llm-evaluate
```

### Example 4: Python SDK end-to-end

```python
import pandas as pd
from odep.dq.models import QualitySuite, QualityRule, Severity
from odep.dq.runner import run_quality_suite
from odep.dq.serializer import save_suite, load_suite
from odep.dq.reader import read_data
from odep.adapters.openmeta.adapter import OpenMetaAdapter
from odep.config import MetadataConfig

# Read data (any format)
df = read_data("orders.parquet")

# Load suite from file
suite = load_suite("rules/orders.yaml")

# Run with metadata persistence
metadata = OpenMetaAdapter(MetadataConfig())
result = run_quality_suite(suite, df, metadata_adapter=metadata, raise_on_blocking=True)

print(f"Score: {result.quality_score:.1f}%")
print(f"Passed: {result.passed}/{result.total_rules}")

# Check score in catalog
score = metadata.get_quality_score(suite.dataset_urn)
print(f"Catalog score: {score:.1f}%")
```

### Example 5: Avro + ORC files

```python
from odep.dq.reader import read_data

# Avro (requires fastavro: pip install fastavro)
df = read_data("transactions.avro")

# ORC (requires pyarrow: pip install pyarrow)
df = read_data("warehouse.orc")

# Delta Lake (requires deltalake: pip install deltalake)
df = read_data("/path/to/delta-table/")

# JSONL
df = read_data("events.jsonl")

# Excel
df = read_data("report.xlsx")
```

---

## Integration with Pipeline Execution

The DQ engine integrates with `execute_with_quality_gate` for automated
post-run quality checks:

```python
from odep.sdk.execute import execute_with_quality_gate
from odep.config import OdepConfig

# Quality rules are embedded in JobResult.metrics["quality_rules"]
result = execute_with_quality_gate(
    job_id="my_pipeline",
    run_conf={
        "quality_rules": [
            {"name": "row_count", "passed": True, "is_blocking": True,
             "dataset_urn": "urn:li:dataset:(duckdb,output,prod)", "metrics": {"rows": 5000}},
            {"name": "freshness", "passed": False, "is_blocking": False,
             "dataset_urn": "urn:li:dataset:(duckdb,output,prod)", "metrics": {}},
        ]
    }
)
```

---

## Module Reference

```
odep/dq/
├── models.py          QualityRule, QualitySuite, CheckResult, SuiteResult
│                      RuleType (13 types), Severity (blocking/warning)
├── engine.py          DataQualityEngine Protocol
│                      NativeQualityEngine (pandas + DuckDB)
├── spark_engine.py    SparkQualityEngine (distributed Spark SQL)
├── trino_engine.py    TrinoQualityEngine (server-side Trino SQL)
├── reader.py          Universal data reader
│                      read_data()        → pandas DataFrame
│                      read_data_spark()  → Spark DataFrame
│                      read_data_trino()  → pandas DataFrame via Trino
├── llm_advisor.py     LLMAdvisor
│                      generate_rules()   → QualitySuite from schema/data
│                      evaluate_results() → natural language assessment
│                      detect_anomalies() → anomaly list + suggested rules
├── runner.py          run_quality_suite() — runs suite + persists to catalog
├── serializer.py      save_suite() / load_suite() — YAML and JSON I/O
└── ge_adapter.py      GreatExpectationsAdapter (optional GE integration)
```

---

## CLI Reference

```
odep dq generate SOURCE [OPTIONS]
  Generate DQ rules from data using an LLM
  --urn TEXT          Dataset URN
  --name TEXT         Table name for the prompt
  --description TEXT  Dataset description
  -o, --output TEXT   Output .yaml or .json file
  --sample-rows INT   Sample rows for LLM prompt (default: 10)
  --provider TEXT     LLM provider override (openai|anthropic|ollama)
  --model TEXT        LLM model override

odep dq run SUITE_FILE SOURCE [OPTIONS]
  Run a suite against data
  --engine CHOICE     native|spark|trino (default: native)
  --trino-table TEXT  Trino table for trino engine
  --trino-host TEXT   Trino host (default: localhost)
  --trino-port INT    Trino port (default: 8082)
  --persist           Persist results to metadata catalog
  --no-fail           Exit 0 even on blocking failures
  -o, --output TEXT   Save results to JSON
  --llm-evaluate      LLM evaluation after run

odep dq evaluate RESULTS_FILE
  LLM evaluation of a saved results JSON file

odep dq anomalies SOURCE [OPTIONS]
  LLM anomaly detection
  --name TEXT         Table name
  --baseline TEXT     Baseline stats JSON file
  --sample-rows INT   Sample rows (default: 20)
  -o, --output TEXT   Save anomaly report to JSON
  --export-rules TEXT Export suggested rules to YAML

odep dq export SUITE_FILE [OPTIONS]
  Export suite to YAML or JSON
  --format CHOICE     yaml|json (default: yaml)
  -o, --output TEXT   Output file (stdout if omitted)

odep dq show SUITE_FILE
  Print suite as a human-readable table
```

---

## Configuration

```bash
# .odep.env

# LLM provider
ODEP_LLM__PROVIDER=openai          # openai | anthropic | ollama
ODEP_LLM__MODEL=gpt-4o
ODEP_LLM__API_KEY=sk-...
ODEP_LLM__BASE_URL=                # optional: Ollama or Azure endpoint
ODEP_LLM__MAX_TOKENS=4096
ODEP_LLM__TEMPERATURE=0.2

# Trino connection (used by --engine trino)
ODEP_EXECUTION__TRINO_HOST=localhost
ODEP_EXECUTION__TRINO_PORT=8082
ODEP_EXECUTION__TRINO_USER=odep
ODEP_EXECUTION__TRINO_CATALOG=tpch
ODEP_EXECUTION__TRINO_SCHEMA=tiny

# Spark (used by --engine spark)
ODEP_EXECUTION__SPARK_MASTER=spark://localhost:7077
```

---

## Engine Comparison

| Feature | Native | Spark | Trino |
|---|---|---|---|
| Setup | None | `pip install pyspark` | `pip install trino` |
| Data location | Local / S3 / GCS | HDFS / S3 / GCS / local | Any Trino catalog |
| Max scale | ~100M rows | Unlimited | Unlimited |
| Data transfer | Loads to memory | Stays in cluster | Zero transfer |
| Formats | All 13 formats | Parquet/ORC/Avro/Delta/Iceberg | Any Trino connector |
| Custom SQL | DuckDB SQL | Spark SQL | Trino SQL |
| Freshness check | ✅ | ✅ | ✅ |
| Best for | Dev / CI / small data | Large Spark workloads | Existing Trino tables |
