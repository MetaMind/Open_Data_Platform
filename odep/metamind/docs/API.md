# MetaMind API Reference

## Overview

The MetaMind API provides a RESTful interface for executing SQL queries with intelligent routing across multiple execution engines (Oracle, Trino/S3, Spark).

**Base URL:** `http://localhost:8000/api/v1`

**Content-Type:** `application/json`

---

## Authentication

Currently, MetaMind uses tenant-based isolation. Authentication can be added via API Gateway or reverse proxy.

```bash
# Example with API key header
curl -H "X-API-Key: your-api-key" http://localhost:8000/api/v1/health
```

---

## Endpoints

### Health Check

Check the health status of the MetaMind platform and its dependencies.

```http
GET /api/v1/health
```

**Response (200 OK):**
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

**Status Values:**
| Status | Description |
|--------|-------------|
| `healthy` | All critical services operational |
| `degraded` | Some non-critical services down |
| `unhealthy` | Critical service failure |

---

### Execute Query

Execute a SQL query with intelligent routing to the optimal execution engine.

```http
POST /api/v1/query
Content-Type: application/json
```

**Request Body:**
```json
{
  "sql": "SELECT COUNT(*) FROM orders WHERE created_at > '2024-01-01'",
  "tenant_id": "default",
  "user_id": "user123",
  "freshness_tolerance_seconds": 300,
  "use_cache": true
}
```

**Parameters:**
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `sql` | string | Yes | - | SQL query to execute |
| `tenant_id` | string | No | "default" | Tenant identifier for isolation |
| `user_id` | string | No | null | User identifier for audit |
| `freshness_tolerance_seconds` | integer | No | null | Max acceptable staleness (seconds) |
| `use_cache` | boolean | No | true | Whether to use result cache |

**Freshness Tolerance Guide:**
| Value | Behavior |
|-------|----------|
| `0` | Real-time only (Oracle) |
| `<= 300` | Recent data acceptable (S3 if CDC lag < 5min) |
| `<= 1800` | Standard freshness (S3 default) |
| `> 1800` | Historical data (S3 definitely) |

**Response (200 OK):**
```json
{
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "routed_to": "s3_analytics",
  "execution_strategy": "direct",
  "freshness_seconds": 60,
  "estimated_cost_ms": 150.5,
  "confidence": 0.85,
  "cache_hit": false,
  "execution_time_ms": 145,
  "row_count": 100,
  "columns": ["order_id", "customer_id", "total_amount"],
  "data": [
    {"order_id": 1, "customer_id": 101, "total_amount": 99.99},
    {"order_id": 2, "customer_id": 102, "total_amount": 149.99}
  ],
  "rewritten_sql": "SELECT order_id, customer_id, total_amount FROM iceberg.orders WHERE created_at > DATE '2024-01-01' LIMIT 100",
  "reason": "CDC lag acceptable (60s), estimated cost 150.5ms"
}
```

**Response Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `query_id` | string | Unique query identifier |
| `status` | string | `success`, `failed`, or `cancelled` |
| `routed_to` | string | Target engine (oracle_prod, s3_analytics, spark_batch) |
| `execution_strategy` | string | `direct`, `cached`, `hybrid`, `batch`, or `federated` |
| `freshness_seconds` | integer | Actual data freshness |
| `estimated_cost_ms` | float | ML-predicted execution time |
| `confidence` | float | Model confidence (0-1) |
| `cache_hit` | boolean | Whether result was from cache |
| `execution_time_ms` | integer | Actual execution time |
| `row_count` | integer | Number of rows returned |
| `columns` | array | Column names |
| `data` | array | Query results (max 100 rows) |
| `rewritten_sql` | string | Engine-specific SQL |
| `reason` | string | Human-readable routing explanation |

**Error Response (400 Bad Request):**
```json
{
  "error": "Query execution failed",
  "detail": "Table 'orders' not found in tenant 'default'",
  "query_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Error Response (503 Service Unavailable):**
```json
{
  "error": "Application not initialized",
  "detail": null,
  "query_id": null
}
```

---

### Cancel Query

Cancel a running query.

```http
POST /api/v1/query/{query_id}/cancel
```

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `query_id` | string | Query identifier to cancel |

**Response (200 OK):**
```json
{
  "status": "cancelled",
  "query_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

### Query History

Get recent query execution history.

```http
GET /api/v1/query/history?tenant_id=default&limit=100&offset=0
```

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `tenant_id` | string | No | "default" | Tenant filter |
| `limit` | integer | No | 100 | Max results to return |
| `offset` | integer | No | 0 | Pagination offset |

**Response (200 OK):**
```json
{
  "queries": [
    {
      "query_id": "550e8400-e29b-41d4-a716-446655440000",
      "user_id": "user123",
      "sql": "SELECT COUNT(*) FROM orders WHERE created_at > '2024-01-01'...",
      "target_source": "s3_analytics",
      "status": "success",
      "execution_time_ms": 145,
      "row_count": 100,
      "submitted_at": "2024-03-04T12:34:56Z"
    }
  ],
  "count": 1
}
```

---

### CDC Status

Get CDC replication status for all tables.

```http
GET /api/v1/cdc/status?tenant_id=default
```

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `tenant_id` | string | No | "default" | Tenant filter |

**Response (200 OK):**
```json
{
  "total_tables": 10,
  "healthy": 8,
  "warning": 1,
  "critical": 1,
  "max_lag_seconds": 600,
  "overall_status": "degraded",
  "lagging_tables": [
    {
      "table": "events",
      "lag_seconds": 600
    }
  ]
}
```

**Status Levels:**
| Status | Lag Range | Description |
|--------|-----------|-------------|
| `healthy` | < 5 min | CDC within target |
| `warning` | 5-10 min | Acceptable lag |
| `critical` | > 10 min | Routing to Oracle |

---

### Cache Statistics

Get cache performance statistics.

```http
GET /api/v1/cache/stats
```

**Response (200 OK):**
```json
{
  "l1_memory": {
    "hits": 1500,
    "misses": 300,
    "hit_rate": 0.833,
    "size": 100,
    "max_size": 1000
  },
  "l2_redis": {
    "hits": 800,
    "misses": 200,
    "hit_rate": 0.8,
    "keys": 500
  },
  "l3_s3": {
    "hits": 50,
    "misses": 150,
    "hit_rate": 0.25
  },
  "overall_hit_rate": 0.78
}
```

---

### Invalidate Cache

Invalidate cache entries matching a pattern.

```http
POST /api/v1/cache/invalidate?pattern=orders*
```

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `pattern` | string | No | "" | Pattern to match (empty = clear all) |

**Response (200 OK):**
```json
{
  "status": "success",
  "invalidated_count": 15
}
```

**Note:** Use with caution. Clearing all cache (`pattern=""`) may impact performance.

---

### Search Tables

Search tables by name pattern.

```http
GET /api/v1/tables/search?q=order&tenant_id=default&limit=100
```

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `q` | string | Yes | - | Search query (table name pattern) |
| `tenant_id` | string | No | "default" | Tenant filter |
| `limit` | integer | No | 100 | Max results |

**Response (200 OK):**
```json
{
  "tables": [
    {
      "table_id": "uuid",
      "source_id": "oracle_prod",
      "schema_name": "public",
      "table_name": "orders",
      "row_count": 1000000,
      "size_bytes": 52428800,
      "is_partitioned": true
    }
  ],
  "count": 1
}
```

---

### Get Table Details

Get detailed metadata for a specific table.

```http
GET /api/v1/tables/{table_name}?tenant_id=default
```

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `table_name` | string | Table name |

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `tenant_id` | string | No | "default" | Tenant filter |

**Response (200 OK):**
```json
{
  "table": {
    "table_id": "550e8400-e29b-41d4-a716-446655440000",
    "source_id": "oracle_prod",
    "schema_name": "public",
    "table_name": "orders",
    "row_count": 1000000,
    "size_bytes": 52428800,
    "is_partitioned": true
  },
  "columns": [
    {
      "column_name": "order_id",
      "data_type": "BIGINT",
      "is_nullable": false,
      "is_primary_key": true
    },
    {
      "column_name": "customer_id",
      "data_type": "BIGINT",
      "is_nullable": false,
      "is_primary_key": false
    },
    {
      "column_name": "total_amount",
      "data_type": "DECIMAL(18,2)",
      "is_nullable": true,
      "is_primary_key": false
    }
  ]
}
```

**Error Response (404 Not Found):**
```json
{
  "error": "Table orders not found",
  "detail": null,
  "query_id": null
}
```

---

## Query Routing Logic

The routing decision is based on multiple factors:

### 1. Freshness Requirements

| Freshness | CDC Lag | Decision |
|-----------|---------|----------|
| `0s` (realtime) | Any | Oracle |
| `<= 300s` | < 5min | S3 |
| `<= 300s` | > 5min | Hybrid (Oracle + S3) |
| `> 1800s` | Any | S3 |

### 2. Batch Job Detection

Queries are routed to Spark when:
- Estimated rows >= 1,000,000
- Number of joins >= 5
- Full table scan without filters
- Complex aggregations (10+ aggregates)

### 3. ML Cost Model

```
score = predicted_cost + freshness_penalty + confidence_bonus

# Freshness penalty: 10ms per second of staleness
freshness_penalty = actual_freshness * 10

# Confidence bonus: -5% for high confidence
confidence_bonus = predicted_cost * (confidence - 0.5) * -0.1
```

### 4. Engine Health

If an engine is unhealthy (circuit breaker open), queries are routed to the fallback source.

---

## Execution Strategies

### Direct
Execute query on a single source (Oracle or S3).

### Cached
Return result from cache without executing.

### Hybrid
Split query between Oracle (fresh data) and S3 (historical data):
```sql
-- Oracle (recent 5 minutes)
SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '5 minutes'
UNION ALL
-- S3 (historical)
SELECT * FROM orders WHERE created_at <= NOW() - INTERVAL '5 minutes'
```

### Batch
Submit as Spark batch job for large-scale processing.

### Federated
Execute across multiple data sources (cross-cloud).

---

## Error Handling

### HTTP Status Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad Request (invalid SQL, missing parameters) |
| 404 | Not Found (table, query) |
| 503 | Service Unavailable (app not initialized) |
| 500 | Internal Server Error |

### Error Response Format

```json
{
  "error": "Human-readable error message",
  "detail": "Additional details (dev mode only)",
  "query_id": "uuid or null"
}
```

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Table not found` | Table doesn't exist in tenant | Check table name and tenant_id |
| `Query too complex` | Exceeds complexity limits | Simplify query, add filters |
| `Oracle circuit breaker open` | Too many Oracle errors | Wait for recovery, use S3 |
| `CDC lag too high` | Replication behind | Check CDC pipeline health |

---

## Rate Limiting

MetaMind supports per-tenant rate limiting:

| Resource | Default Limit |
|----------|---------------|
| Queries per minute | 1000 |
| Concurrent queries | 100 |
| Max rows per query | 1,000,000 |
| Max execution time | 300 seconds |

**Rate Limit Response (429 Too Many Requests):**
```json
{
  "error": "Rate limit exceeded",
  "detail": "Too many requests for tenant 'default'",
  "retry_after": 60
}
```

---

## Examples

### Basic Query

```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT COUNT(*) FROM orders",
    "tenant_id": "default"
  }'
```

### Real-time Query (Oracle)

```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM transactions WHERE status = '"'"'pending'"'"'",
    "tenant_id": "default",
    "freshness_tolerance_seconds": 0
  }'
```

### Analytics Query (S3)

```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT DATE(created_at), COUNT(*) FROM orders GROUP BY 1",
    "tenant_id": "default",
    "freshness_tolerance_seconds": 1800
  }'
```

### Batch Query (Spark)

```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT customer_id, SUM(total_amount) FROM orders GROUP BY customer_id",
    "tenant_id": "default"
  }'
```

### With Cache Disabled

```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM orders WHERE updated_at > NOW() - INTERVAL '"'"'1 hour'"'"'",
    "tenant_id": "default",
    "use_cache": false
  }'
```

---

## SDK Examples

### Python

```python
import requests

class MetaMindClient:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
    
    def query(self, sql, tenant_id="default", freshness=None):
        response = requests.post(
            f"{self.base_url}/api/v1/query",
            json={
                "sql": sql,
                "tenant_id": tenant_id,
                "freshness_tolerance_seconds": freshness
            }
        )
        response.raise_for_status()
        return response.json()

# Usage
client = MetaMindClient()
result = client.query(
    "SELECT COUNT(*) FROM orders",
    freshness=300
)
print(f"Routed to: {result['routed_to']}")
print(f"Rows: {result['row_count']}")
```

### JavaScript

```javascript
class MetaMindClient {
  constructor(baseUrl = 'http://localhost:8000') {
    this.baseUrl = baseUrl;
  }
  
  async query(sql, tenantId = 'default', freshness = null) {
    const response = await fetch(`${this.baseUrl}/api/v1/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql,
        tenant_id: tenantId,
        freshness_tolerance_seconds: freshness
      })
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }
    
    return response.json();
  }
}

// Usage
const client = new MetaMindClient();
const result = await client.query(
  'SELECT COUNT(*) FROM orders',
  'default',
  300
);
console.log('Routed to:', result.routed_to);
```

---

## WebSocket API (Future)

Real-time query streaming is planned for v4.1:

```javascript
const ws = new WebSocket('ws://localhost:8000/ws/query');

ws.onopen = () => {
  ws.send(JSON.stringify({
    sql: 'SELECT * FROM large_table',
    stream: true
  }));
};

ws.onmessage = (event) => {
  const row = JSON.parse(event.data);
  console.log('Received row:', row);
};
```

---

## Changelog

### v4.0.0
- Initial API release
- Query routing with ML cost model
- CDC-aware freshness
- Multi-tier caching
- Batch job routing to Spark

### Planned (v4.1)
- WebSocket streaming
- GraphQL support
- Query cancellation improvements
- Bulk query API
