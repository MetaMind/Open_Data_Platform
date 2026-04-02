# MetaMind Configuration Guide

## Overview

MetaMind uses a hierarchical configuration system with environment variables as the primary source. All configuration keys use the `METAMIND_` prefix.

## Configuration Hierarchy

1. **Environment Variables** (highest priority)
2. **`.env` file** (loaded automatically)
3. **Default values** (lowest priority)

## Core Configuration

### Application Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `METAMIND_APP_ENV` | `development` | Environment: `development`, `staging`, `production` |
| `METAMIND_DEBUG` | `false` | Enable debug logging |
| `METAMIND_HOST` | `0.0.0.0` | API server bind address |
| `METAMIND_PORT` | `8000` | API server port |
| `METAMIND_WORKERS` | `1` | Number of worker processes |

### Database Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METAMIND_DB__HOST` | `localhost` | PostgreSQL host |
| `METAMIND_DB__PORT` | `5432` | PostgreSQL port |
| `METAMIND_DB__DATABASE` | `metamind` | Database name |
| `METAMIND_DB__USER` | `metamind` | Database user |
| `METAMIND_DB__PASSWORD` | - | Database password |
| `METAMIND_DB__POOL_SIZE` | `20` | Connection pool size |
| `METAMIND_DB__MAX_OVERFLOW` | `10` | Max overflow connections |

### Redis Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METAMIND_REDIS__HOST` | `localhost` | Redis host |
| `METAMIND_REDIS__PORT` | `6379` | Redis port |
| `METAMIND_REDIS__DB` | `0` | Redis database number |
| `METAMIND_REDIS__PASSWORD` | - | Redis password (optional) |
| `METAMIND_REDIS__SSL` | `false` | Enable SSL for Redis |

### Trino Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METAMIND_TRINO__COORDINATOR_URL` | `http://localhost:8080` | Trino coordinator URL |
| `METAMIND_TRINO__USER` | `metamind` | Trino user |
| `METAMIND_TRINO__CATALOG` | `iceberg` | Default catalog |
| `METAMIND_TRINO__SCHEMA` | `default` | Default schema |
| `METAMIND_TRINO__TIMEOUT_SECONDS` | `300` | Query timeout |

### Oracle Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METAMIND_ORACLE__ENABLED` | `false` | Enable Oracle connector |
| `METAMIND_ORACLE__HOST` | `localhost` | Oracle host |
| `METAMIND_ORACLE__PORT` | `1521` | Oracle port |
| `METAMIND_ORACLE__SERVICE_NAME` | `ORCLPDB1` | Oracle service name |
| `METAMIND_ORACLE__USER` | - | Oracle username |
| `METAMIND_ORACLE__PASSWORD` | - | Oracle password |
| `METAMIND_ORACLE__POOL_SIZE` | `10` | Connection pool size |
| `METAMIND_ORACLE__DRCP` | `true` | Enable DRCP |

### S3/MinIO Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METAMIND_S3__ENDPOINT_URL` | `http://localhost:9000` | S3 endpoint |
| `METAMIND_S3__BUCKET` | `metamind-data-lake` | S3 bucket name |
| `METAMIND_S3__REGION` | `us-east-1` | AWS region |
| `AWS_ACCESS_KEY_ID` | - | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | - | S3 secret key |

### Spark Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METAMIND_SPARK__ENABLED` | `false` | Enable Spark engine |
| `METAMIND_SPARK__MASTER_URL` | `local[*]` | Spark master URL |
| `METAMIND_SPARK__APP_NAME` | `metamind-batch` | Spark app name |
| `METAMIND_SPARK__EXECUTOR_MEMORY` | `4g` | Executor memory |
| `METAMIND_SPARK__EXECUTOR_CORES` | `4` | Executor cores |

### Cache Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METAMIND_CACHE__L1_TTL_SECONDS` | `300` | L1 cache TTL (5 min) |
| `METAMIND_CACHE__L2_TTL_SECONDS` | `3600` | L2 cache TTL (1 hour) |
| `METAMIND_CACHE__L3_TTL_SECONDS` | `604800` | L3 cache TTL (7 days) |
| `METAMIND_CACHE__L1_MAX_SIZE` | `1000` | L1 max entries |
| `METAMIND_CACHE__COMPRESSION_ENABLED` | `true` | Enable compression |

### ML Model Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METAMIND_ML__MODEL_PATH` | `./models` | Model storage path |
| `METAMIND_ML__RETRAIN_INTERVAL_HOURS` | `24` | Retraining interval |
| `METAMIND_ML__MIN_TRAINING_SAMPLES` | `1000` | Min samples for training |
| `METAMIND_ML__CONFIDENCE_THRESHOLD` | `0.7` | Min prediction confidence |

### Circuit Breaker Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `METAMIND_CIRCUIT_BREAKER__FAILURE_THRESHOLD` | `5` | Failures before opening |
| `METAMIND_CIRCUIT_BREAKER__RECOVERY_TIMEOUT` | `60` | Recovery timeout (seconds) |
| `METAMIND_CIRCUIT_BREAKER__HALF_OPEN_MAX_CALLS` | `3` | Max calls in half-open |

## Environment-Specific Configuration

### Development

```bash
# .env.development
METAMIND_APP_ENV=development
METAMIND_DEBUG=true
METAMIND_DB__HOST=localhost
METAMIND_REDIS__HOST=localhost
METAMIND_TRINO__COORDINATOR_URL=http://localhost:8080
METAMIND_ORACLE__ENABLED=false
```

### Production

```bash
# .env.production
METAMIND_APP_ENV=production
METAMIND_DEBUG=false
METAMIND_DB__HOST=postgres.metamind.svc.cluster.local
METAMIND_REDIS__HOST=redis.metamind.svc.cluster.local
METAMIND_TRINO__COORDINATOR_URL=http://trino.metamind.svc.cluster.local:8080
METAMIND_ORACLE__ENABLED=true
METAMIND_ORACLE__HOST=oracle.production.internal
METAMIND_WORKERS=4
```

## Configuration Validation

MetaMind validates configuration on startup. Invalid configuration will prevent the application from starting.

```python
# Example validation error
ERROR: Invalid configuration: METAMIND_DB__PASSWORD is required
```

## Secrets Management

### Kubernetes Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: metamind-config
  namespace: metamind
type: Opaque
stringData:
  METAMIND_DB__PASSWORD: "secure-password"
  METAMIND_ORACLE__PASSWORD: "oracle-password"
  AWS_SECRET_ACCESS_KEY: "s3-secret-key"
```

### AWS Secrets Manager

```python
import boto3
import json

# Retrieve secrets
client = boto3.client('secretsmanager')
response = client.get_secret_value(SecretId='metamind/production')
secrets = json.loads(response['SecretString'])

# Set environment variables
os.environ['METAMIND_DB__PASSWORD'] = secrets['db_password']
```

### HashiCorp Vault

```python
import hvac

client = hvac.Client(url='https://vault.example.com')
client.auth.kubernetes.login(role='metamind')

secret = client.secrets.kv.v2.read_secret_version(
    path='metamind/production'
)
```

## Dynamic Configuration

Some configuration can be updated at runtime via the API (future feature):

```bash
# Update cache TTL
curl -X POST http://localhost:8000/api/v1/admin/config \
  -H "Content-Type: application/json" \
  -d '{
    "cache.l1_ttl_seconds": 600
  }'
```

## Configuration Best Practices

1. **Never commit secrets** to version control
2. **Use different databases** per environment
3. **Set appropriate timeouts** for production
4. **Enable SSL** for external connections
5. **Monitor configuration changes** with audit logs
6. **Use secret management** in production
7. **Validate configuration** before deployment

## Troubleshooting

### Configuration Not Loading

```bash
# Check environment variables
env | grep METAMIND

# Verify .env file location
ls -la .env

# Test configuration loading
python -c "from metamind.config.settings import get_settings; print(get_settings())"
```

### Database Connection Failed

```bash
# Test PostgreSQL connection
psql -h $METAMIND_DB__HOST -U $METAMIND_DB__USER -d $METAMIND_DB__DATABASE

# Check Redis connection
redis-cli -h $METAMIND_REDIS__HOST ping
```
