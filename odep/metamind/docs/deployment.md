# MetaMind Deployment Guide

## Quick Start with Docker Compose

The fastest way to run MetaMind is with Docker Compose:

```bash
git clone https://github.com/metamind/metamind-platform.git
cd metamind-platform
docker compose up -d
```

Expected output:

```
Creating metamind-postgres ... done
Creating metamind-redis    ... done
Creating metamind-api      ... done
MetaMind API running on http://localhost:8080
```

Verify the deployment:

```bash
curl http://localhost:8080/health
# {"status": "healthy", "version": "5.0.0"}
```

## Production Deployment: AWS ECS

### Prerequisites

Install Terraform 1.5+, AWS CLI v2, and configure credentials with sufficient permissions
for ECS, RDS, ElastiCache, and S3.

### Infrastructure

The Terraform configuration creates: an ECS Fargate cluster with auto-scaling, an RDS
PostgreSQL 15 instance (Multi-AZ), an ElastiCache Redis cluster, an S3 bucket for
storage, an ALB with TLS termination, and CloudWatch log groups.

```hcl
module "metamind" {
  source          = "./terraform/aws"
  environment     = "production"
  vpc_id          = var.vpc_id
  subnet_ids      = var.private_subnet_ids
  db_instance     = "db.r6g.xlarge"
  redis_node_type = "cache.r6g.large"
  ecs_cpu         = 2048
  ecs_memory      = 4096
  min_tasks       = 2
  max_tasks       = 10
}
```

### Environment Variables

```bash
METAMIND_DB_URL=postgresql://metamind:***@rds-endpoint:5432/metamind
METAMIND_REDIS_HOST=redis-endpoint.cache.amazonaws.com
METAMIND_STORAGE_PROVIDER=s3
METAMIND_S3_BUCKET=metamind-production-data
METAMIND_S3_REGION=us-east-1
METAMIND_LLM_PROVIDER=openai
METAMIND_LLM_API_KEY=sk-***
METAMIND_LLM_MODEL=gpt-4o
METAMIND_LOG_LEVEL=INFO
```

### Secrets Management

Store sensitive values in AWS Secrets Manager:

```bash
aws secretsmanager create-secret \
  --name metamind/production/db-url \
  --secret-string "postgresql://..."

aws secretsmanager create-secret \
  --name metamind/production/llm-api-key \
  --secret-string "sk-..."
```

Reference secrets in your ECS task definition using `valueFrom`.

## Production Deployment: GCP Cloud Run

### Infrastructure

```hcl
module "metamind" {
  source       = "./terraform/gcp"
  project_id   = var.project_id
  region       = "us-central1"
  db_tier      = "db-custom-4-8192"
  redis_tier   = "STANDARD_HA"
  min_instances = 1
  max_instances = 10
}
```

Cloud Run automatically handles TLS and load balancing. Cloud SQL provides managed
PostgreSQL. Memorystore provides managed Redis. GCS provides object storage.

### VPC Configuration

Create a VPC connector for Cloud Run to access Cloud SQL and Memorystore:

```bash
gcloud compute networks vpc-access connectors create metamind-vpc \
  --region=us-central1 \
  --network=default \
  --range=10.8.0.0/28
```

## Production Deployment: Azure

### Infrastructure

```hcl
module "metamind" {
  source              = "./terraform/azure"
  resource_group      = "metamind-production"
  location            = "eastus"
  db_sku              = "GP_Gen5_4"
  redis_sku           = "Premium"
  container_cpu       = 2.0
  container_memory_gb = 4.0
}
```

Azure Container Instances or AKS for compute. Azure Database for PostgreSQL for
the database. Azure Cache for Redis. Azure Blob Storage for object storage.

## Kubernetes / Helm

### Installation

```bash
helm repo add metamind https://charts.metamind.io
helm install metamind metamind/metamind \
  --namespace metamind \
  --create-namespace \
  --values values.yaml
```

### values.yaml

```yaml
replicaCount: 3

image:
  repository: metamind/metamind-api
  tag: "5.0.0"

resources:
  requests:
    cpu: "1000m"
    memory: "2Gi"
  limits:
    cpu: "2000m"
    memory: "4Gi"

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 20
  targetCPUUtilizationPercentage: 70

database:
  host: postgres-primary.metamind.svc
  port: 5432
  name: metamind

redis:
  host: redis-master.metamind.svc
  port: 6379

storage:
  provider: s3
  s3Bucket: metamind-data

podDisruptionBudget:
  minAvailable: 1
```

### Horizontal Pod Autoscaler

The HPA scales based on CPU utilization and custom metrics (query latency p95,
queue depth). Default configuration scales between 2 and 20 pods.

### Pod Disruption Budget

The PDB ensures at least one pod remains available during voluntary disruptions
(node upgrades, cluster scaling). For production, set `minAvailable: 2`.

## Environment Variables Reference

| Variable                     | Required | Default          | Description                           |
|------------------------------|----------|------------------|---------------------------------------|
| METAMIND_DB_URL              | Yes      | sqlite:///:memory| Database connection URL                |
| METAMIND_REDIS_HOST          | No       | localhost        | Redis host                            |
| METAMIND_REDIS_PORT          | No       | 6379             | Redis port                            |
| METAMIND_REDIS_PASSWORD      | No       | None             | Redis password                        |
| METAMIND_STORAGE_PROVIDER    | No       | local            | Storage backend (local/s3/gcs/azure)  |
| METAMIND_S3_BUCKET           | If S3    | None             | S3 bucket name                        |
| METAMIND_S3_REGION           | If S3    | us-east-1        | S3 region                             |
| METAMIND_GCS_BUCKET          | If GCS   | None             | GCS bucket name                       |
| METAMIND_GCS_PROJECT         | If GCS   | None             | GCP project ID                        |
| METAMIND_AZURE_CONN_STR      | If Azure | None             | Azure connection string               |
| METAMIND_AZURE_CONTAINER     | If Azure | None             | Azure container name                  |
| METAMIND_LLM_PROVIDER        | No       | openai           | LLM provider (openai/anthropic/ollama)|
| METAMIND_LLM_API_KEY         | If LLM   | None             | LLM API key                           |
| METAMIND_LLM_MODEL           | No       | gpt-4o           | LLM model name                        |
| METAMIND_LOG_LEVEL           | No       | INFO             | Logging level                         |

## Database Migrations

### Running Migrations

```bash
python -m metamind.migrations.run --direction up
```

### Rolling Back

```bash
python -m metamind.migrations.run --direction down --steps 1
```

### Zero-Downtime Strategy

MetaMind uses a two-phase migration approach. Phase 1 (expand) adds new columns/tables
without removing old ones. Phase 2 (contract) removes old columns after all application
instances have been updated. This ensures backward compatibility during rolling deploys.

## Scaling Guide

### Metrics Thresholds

Scale up when: CPU utilization exceeds 70% sustained for 5 minutes, p95 query latency
exceeds 500ms, or Redis memory exceeds 80%.

Scale down when: CPU utilization drops below 30% sustained for 15 minutes and p95
latency is below 100ms.

### Horizontal vs. Vertical

Horizontal scaling (more instances) is preferred for the API tier. Each instance is
stateless and can serve any tenant.

Vertical scaling (larger instances) is needed for the database tier when query complexity
increases or when tenant count exceeds connection pool limits.

### Redis Cluster

For production workloads exceeding 50K QPS or 10GB cache size, switch from single-node
Redis to Redis Cluster. MetaMind's cache keys include the tenant_id prefix, which
provides natural hash slot distribution.

## Health Monitoring

### Grafana Dashboards

Import the provided dashboards from `monitoring/grafana/`:

1. **MetaMind Overview** — API request rate, error rate, latency percentiles
2. **Optimizer Performance** — optimization latency, cache hit ratio, plan cost distribution
3. **Tenant Activity** — per-tenant query volume, feature usage, error rates
4. **Backend Health** — backend response times, connection pool usage, error rates

### Alerts

| Alert                      | Condition                    | Severity |
|----------------------------|------------------------------|----------|
| High Error Rate            | >5% 5xx in 5 min            | Critical |
| High Latency               | p95 > 2s for 10 min         | Warning  |
| Database Connection Pool   | >90% used for 5 min         | Warning  |
| Redis Memory               | >85% used                   | Warning  |
| Disk Space                 | >90% used                   | Critical |

### SLO Definitions

| SLO                        | Target  | Window  |
|----------------------------|---------|---------|
| API Availability           | 99.9%   | 30 days |
| Query Optimization Latency | p99 <5s | 30 days |
| NL-to-SQL Success Rate     | >90%    | 7 days  |
