# MetaMind Enterprise Query Intelligence Platform v4.0

[![CI](https://github.com/metamind/metamind/actions/workflows/ci.yml/badge.svg)](https://github.com/metamind/metamind/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)

An adaptive, metadata-driven query intelligence platform that operates as a control-plane optimization layer above heterogeneous execution engines.

## Table of Contents

- [Overview](#overview)
- [Unique Features](#unique-features)
- [Market Comparison](#market-comparison)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Query Routing](#query-routing)
- [Monitoring](#monitoring)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Overview

MetaMind separates optimization from execution, introducing AI-driven decision making for:

- **Query Routing**: Routes queries to the optimal engine (Oracle, Trino/S3, Spark, GPU)
- **Cost Estimation**: ML models predict query execution costs
- **Freshness Awareness**: CDC tracking ensures data freshness requirements are met
- **Cross-Engine Execution**: Federated queries across multiple data sources

## Unique Features

See the team-oriented feature summary here:
- [`README_UNIQUE_FEATURES.md`](README_UNIQUE_FEATURES.md)

## Market Comparison

See the competitive landscape and strategic holding rationale:
- [`MARKET_COMPARISON_AND_HOLDING_ADVANTAGES.md`](MARKET_COMPARISON_AND_HOLDING_ADVANTAGES.md)

### Supported Engines

| Engine | Use Case | Best For |
|--------|----------|----------|
| **Oracle** | OLTP | Real-time queries, transactions |
| **Trino/S3** | OLAP | Interactive analytics, aggregations |
| **Spark** | Batch | Large-scale ETL, complex joins (>1M rows) |
| **GPU** | Acceleration | Vectorized operations on large datasets |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ LAYER 1: AI CONTROL PLANE (MetaMind Intelligence)               │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ │
│ │ AI Synthesis│ │ Metadata    │ │ Cascades    │ │ Learned     │ │
│ │ Engine      │ │ Catalog     │ │ Optimizer   │ │ Cost Model  │ │
│ └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘ │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ │
│ │ Query       │ │ Router      │ │ Adaptive    │ │ Safety      │ │
│ │ Parser      │ │ (ML-Based)  │ │ Feedback    │ │ Layer       │ │
│ └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ LAYER 2: EXECUTION ENGINES (Data Plane)                         │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ │
│ │ ORACLE      │ │ TRINO       │ │ SPARK       │ │ GPU         │ │
│ │ (Source of  │ │ (Federated  │ │ (Batch/     │ │ (Vector     │ │
│ │ Truth)      │ │ Query)      │ │ Stream)     │ │ Engine)     │ │
│ └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ LAYER 3: DATA SOURCES & CDC PIPELINE                            │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ CDC PIPELINE (Debezium → Kafka → Spark → Iceberg)           │ │
│ │ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐            │ │
│ │ │ ORACLE  │──→│ DEBEZIUM│──→│ KAFKA │──→│ SPARK │──→ S3    │ │
│ │ │ CDC Log │   │ Capture │   │ Stream│   │ Merge │  Iceberg │ │
│ │ └─────────┘   └─────────┘   └─────────┘   └─────────┘      │ │
│ └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Key Features

### 1. AI-Driven Query Routing

Routes queries to the optimal engine based on:
- Query complexity and estimated cost
- Data freshness requirements
- Engine health and load
- Historical performance

### 2. Cost-Based Query Planner

- **Logical Plan Extraction**: Parses SQL into execution tree
- **Cardinality Estimation**: Estimates row counts using statistics
- **Engine Cost Simulation**: Predicts execution cost per engine

### 3. CDC Lag Adaptive Routing

- Real-time CDC lag monitoring
- Trend-based routing decisions
- Predictive lag estimation
- Automatic fallback to source when CDC is stale

### 4. Multi-Tier Caching

- L1: In-memory (hot, 5min TTL)
- L2: Redis (warm, 1hour TTL)
- L3: S3 (cold, 7day TTL)

### 5. Safety & Governance

- Query pattern validation (blocks DML/DDL)
- Complexity limits
- Per-user concurrency limits
- Circuit breaker protection
- Query timeouts

### 6. Observability

- Prometheus metrics
- Distributed tracing (OpenTelemetry)
- Query-level tracing across engines
- Model drift detection and alerts

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- 8GB+ RAM available for Docker

### Running with Docker Compose

```bash
# Clone the repository
git clone https://github.com/metamind/metamind.git
cd metamind-platform

# Start all services
docker-compose up -d

# Wait for services to be ready
sleep 30

# Check health
curl http://localhost:8000/api/v1/health
```

### Services

| Service | URL | Description |
|---------|-----|-------------|
| MetaMind API | http://localhost:8000 | Main API server |
| Trino | http://localhost:8080 | Query engine UI |
| MinIO | http://localhost:9001 | S3-compatible storage |
| Grafana | http://localhost:3000 | Metrics dashboards |
| Prometheus | http://localhost:9090 | Metrics collection |
| PostgreSQL | localhost:5432 | Metadata store |
| Redis | localhost:6379 | Result cache |

### Example Queries

```bash
# Execute a query (routes to Trino/S3 if fresh enough)
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT COUNT(*) FROM orders WHERE created_at > '"'"'2024-01-01'"'"'",
    "tenant_id": "default",
    "freshness_tolerance_seconds": 300
  }'

# Check CDC status
curl http://localhost:8000/api/v1/cdc/status?tenant_id=default

# Get cache statistics
curl http://localhost:8000/api/v1/cache/stats
```

## Installation

### Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Install development dependencies
pip install -e ".[dev]"

# Run migrations
psql -h localhost -U metamind -d metamind -f migrations/001_core.sql

# Start services
python -m metamind.api.server
```

### Deterministic Test Setup

```bash
cd metamind_complete
make test-setup
source .venv/bin/activate
pytest tests/unit -q
```

Notes:
- `make test-setup` installs runtime + dev dependencies into `.venv`.
- `.env.test` is auto-created from `.env.test.example` if missing.

### Kubernetes (Production)

```bash
# Add MetaMind Helm repository
helm repo add metamind https://charts.metamind.io
helm repo update

# Create namespace
kubectl create namespace metamind

# Install with custom values
helm install metamind metamind/metamind \
  --namespace metamind \
  --values values-production.yaml
```

## Configuration

Configuration is done via environment variables with the `METAMIND_` prefix:

```bash
# Database
METAMIND_DB__HOST=postgres
METAMIND_DB__PORT=5432
METAMIND_DB__DATABASE=metamind
METAMIND_DB__USER=metamind
METAMIND_DB__PASSWORD=metamind

# Redis
METAMIND_REDIS__HOST=redis
METAMIND_REDIS__PORT=6379

# Trino
METAMIND_TRINO__COORDINATOR_URL=http://trino:8080
METAMIND_TRINO__USER=metamind
METAMIND_TRINO__CATALOG=iceberg

# Oracle (optional)
METAMIND_ORACLE__HOST=oracle
METAMIND_ORACLE__PORT=1521
METAMIND_ORACLE__SERVICE_NAME=ORCLPDB1
METAMIND_ORACLE__USER=metamind_read
METAMIND_ORACLE__PASSWORD=secret

# S3/MinIO
METAMIND_S3__ENDPOINT_URL=http://minio:9000
METAMIND_S3__BUCKET=metamind-data-lake
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

See `.env.example` for a complete list.

## API Reference

### Query Execution

```http
POST /api/v1/query
Content-Type: application/json

{
  "sql": "SELECT * FROM orders LIMIT 100",
  "tenant_id": "default",
  "user_id": "user123",
  "freshness_tolerance_seconds": 300,
  "use_cache": true
}
```

Response:
```json
{
  "query_id": "uuid",
  "status": "success",
  "routed_to": "default_s3",
  "execution_strategy": "direct",
  "freshness_seconds": 60,
  "estimated_cost_ms": 150.5,
  "confidence": 0.85,
  "cache_hit": false,
  "execution_time_ms": 145,
  "row_count": 100,
  "columns": ["order_id", "customer_id", ...],
  "data": [...],
  "reason": "CDC lag acceptable (60s)"
}
```

### Health Check

```http
GET /api/v1/health
```

Response:
```json
{
  "status": "healthy",
  "version": "4.0.0",
  "checks": {
    "database": true,
    "redis": true,
    "trino": true,
    "oracle": false,
    "spark": true
  }
}
```

### CDC Status

```http
GET /api/v1/cdc/status?tenant_id=default
```

Response:
```json
{
  "total_tables": 10,
  "healthy": 8,
  "warning": 1,
  "critical": 1,
  "max_lag_seconds": 600,
  "overall_status": "degraded",
  "lagging_tables": [
    {"table": "events", "lag_seconds": 600}
  ]
}
```

## Query Routing

### Routing Decision Matrix

| Freshness | CDC Lag | Decision |
|-----------|---------|----------|
| 0s (realtime) | Any | Oracle |
| < 5min | < 5min | S3 |
| < 5min | > 5min | Hybrid |
| > 30min | Any | S3 |
| > 1M rows | Any | Spark |

### Batch Job Detection

Queries are routed to Spark when:
- Estimated rows >= 1,000,000
- Number of joins >= 5
- Full table scan without filters
- Complex aggregations (10+ aggregates)

### Hybrid Execution

For queries requiring both fresh and historical data:
```sql
-- Oracle (recent data)
SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '5 minutes'
UNION ALL
-- S3 (historical data)
SELECT * FROM orders WHERE created_at <= NOW() - INTERVAL '5 minutes'
```

## Monitoring

### Prometheus Metrics

| Metric | Description |
|--------|-------------|
| `metamind_queries_total` | Total queries processed |
| `metamind_query_duration_seconds` | Query execution duration |
| `metamind_routing_decision_seconds` | Routing decision latency |
| `metamind_cache_hits_total` | Cache hit count |
| `metamind_cdc_lag_seconds` | CDC replication lag |
| `metamind_oracle_circuit_breaker` | Circuit breaker state |

### Grafana Dashboards

Access Grafana at http://localhost:3000 (admin/admin)

Pre-configured dashboards:
- Query Performance
- CDC Health
- Cache Statistics
- System Resources

## Development

### Running Tests

```bash
# Unit tests
pytest tests/unit -v

# Integration tests (requires Docker)
pytest tests/integration -v

# With coverage
pytest --cov=metamind --cov-report=html
```

### Code Quality

```bash
# Format code
black metamind/

# Lint
ruff check metamind/

# Type check
mypy metamind/

# Validate imports
python scripts/validate_imports.py

# Quality check
python scripts/quality_check.py
```

## Troubleshooting

### High CDC Lag

```bash
# Check Kafka consumer lag
kubectl exec -it kafka-0 -- kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group debezium-oracle

# Restart Spark streaming job
kubectl rollout restart deployment/spark-streaming
```

### Oracle Circuit Breaker Open

```bash
# Check Oracle connectivity
kubectl exec -it deployment/metamind -- \
  curl http://localhost:8000/api/v1/health

# Manual circuit reset
kubectl rollout restart deployment/metamind
```

See [docs/troubleshooting.md](docs/troubleshooting.md) for more.

## Production Checklist

- [ ] Oracle credentials in secrets manager
- [ ] Network isolation (VPC peering)
- [ ] Query audit logging enabled
- [ ] RBAC configured
- [ ] Connection pools sized correctly
- [ ] ML models trained (>1000 samples)
- [ ] CDC lag < 5 minutes
- [ ] Circuit breakers tested
- [ ] Health checks passing
- [ ] Monitoring alerts configured
- [ ] Backups verified

See [docs/deployment.md](docs/deployment.md) for detailed deployment guide.

## License

Apache License 2.0

## Support

- GitHub Issues: https://github.com/metamind/metamind/issues
- Documentation: https://docs.metamind.io
- Email: support@metamind.io

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Acknowledgments

- [Trino](https://trino.io/) - Distributed SQL query engine
- [Apache Spark](https://spark.apache.org/) - Unified analytics engine
- [Debezium](https://debezium.io/) - Change data capture platform
- [Apache Iceberg](https://iceberg.apache.org/) - Open table format
