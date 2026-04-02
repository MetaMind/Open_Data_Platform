# MetaMind SaaS Blueprint

## Business Model

MetaMind offers three pricing tiers designed around query volume and feature access.

**Per-Query Pricing:** The base unit of billing is the optimized query. Each query that
passes through the Cascades optimizer counts as one billable query. Queries served
entirely from cache (F09) are billed at a reduced rate (10% of full price) since they
consume minimal compute.

**Per-Tenant Pricing:** Enterprise customers pay a fixed monthly fee per tenant with
unlimited queries. This model is preferred for high-volume workloads where per-query
pricing becomes prohibitive.

**Enterprise Licensing:** On-premise deployment with annual licensing. Includes dedicated
support, custom feature development, and SLA guarantees.

### Pricing Tiers

| Tier       | Monthly Cost | Query Limit    | Features             |
|------------|-------------|----------------|----------------------|
| Free       | $0          | 10,000/month   | F09 (cache) + F12 (basic opt) |
| Pro        | $499/month  | 1M/month       | F01-F22 (all core)   |
| Enterprise | Custom      | Unlimited      | F01-F30 + support    |

## Tenant Onboarding

### Self-Service Flow

The target is signup to first optimized query in under 5 minutes.

**Step 1: Signup (30 seconds)**
User provides email, password, and company name. System creates tenant_id, provisions
metadata namespace, and issues JWT.

**Step 2: Connect Database (2 minutes)**
User provides database connection string. MetaMind runs a discovery query to enumerate
tables, columns, and basic statistics. Results are stored in the metadata catalog.

**Step 3: First Query (1 minute)**
User submits a SQL query or natural language question through the dashboard. MetaMind
optimizes the query, shows the execution plan, and returns results.

**Step 4: Explore Features (ongoing)**
Dashboard highlights available features based on the tenant's tier. Guided tours show
how to use NL queries (F28), view rewrite suggestions (F29), and run what-if simulations
(F30).

### Automated Schema Discovery

On database connection, MetaMind automatically runs `information_schema` queries to discover
all tables, columns, data types, and basic statistics. For PostgreSQL, it reads `pg_stats`
to import histogram data. For DuckDB, it reads `duckdb_tables()` metadata functions.

This provides immediate optimization capability without manual schema configuration.

## Multi-Region Architecture

### Deployment Topology

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   US-EAST-1     │     │   EU-WEST-1     │     │  AP-SOUTHEAST-1 │
│                 │     │                 │     │                 │
│  API Cluster    │     │  API Cluster    │     │  API Cluster    │
│  PostgreSQL     │     │  PostgreSQL     │     │  PostgreSQL     │
│  Redis          │     │  Redis          │     │  Redis          │
│  S3             │     │  S3             │     │  S3             │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────┬───────────┘                       │
                     │                                   │
              ┌──────┴──────┐                            │
              │  Global DB  │◀───────────────────────────┘
              │  (Config)   │
              └─────────────┘
```

### Tenant Affinity

Tenants are assigned to a primary region based on their database location. All
optimization and query execution happens in the tenant's primary region. The global
configuration database stores tenant-to-region mappings and feature flag overrides.

### Cross-Region Sync

Tenant metadata is replicated asynchronously across regions for disaster recovery.
The replication lag target is under 5 seconds. If a region fails, tenants are
automatically redirected to the nearest healthy region.

## Billing Integration

### Usage Metering

MetaMind tracks usage through the `mm_cloud_budgets` table, recording per-tenant query
counts, cache hit counts, NL query counts, and simulation run counts. Metering is
aggregated hourly and reported to the billing system.

### Stripe Blueprint

```
Customer (1:1 with tenant)
  └── Subscription
       ├── Price: Pro Plan ($499/month)
       └── Usage Record
            ├── query_count: 250,000
            ├── nl_query_count: 5,000
            └── simulation_count: 50
```

Overage billing: queries beyond the plan limit are billed at $0.001 per query for Pro
tier. Enterprise tier has no overage charges.

### Budget Alerts

Tenants can configure budget alerts in `mm_cloud_budgets`. When projected monthly spend
exceeds the configured threshold, MetaMind sends an alert (webhook, email, or in-app
notification). If the hard limit is reached, queries are rate-limited rather than
rejected, ensuring the tenant's applications continue to function.

## Feature Gating

### Tier Mapping

| Feature                    | Free | Pro | Enterprise |
|----------------------------|------|-----|------------|
| F09 Query Cache            | Yes  | Yes | Yes        |
| F12 Basic Optimization     | Yes  | Yes | Yes        |
| F01 Learned Cardinality    | No   | Yes | Yes        |
| F04 DPccp Join Enum        | No   | Yes | Yes        |
| F11 Compiled Execution     | No   | Yes | Yes        |
| F19 Vector Search          | No   | Yes | Yes        |
| F20 Regret Minimization    | No   | Yes | Yes        |
| F28 NL Interface           | No   | No  | Yes        |
| F29 Query Rewrite          | No   | No  | Yes        |
| F30 What-If Simulation     | No   | No  | Yes        |

Feature gates are enforced at the API layer. When a user calls an API endpoint for a
feature not included in their tier, they receive a 403 response with a message indicating
the required tier.

## Customer Isolation Levels

### Shared Infrastructure (Free + Pro)

Multiple tenants share the same API cluster, database, and Redis instance. Isolation is
enforced at the application layer through tenant_id namespacing. This is the most
cost-efficient model.

### Dedicated Backend (Enterprise Option)

The tenant's database queries are routed to a dedicated backend connection pool. The
MetaMind API cluster is still shared, but the tenant's data never passes through shared
database connections.

### VPC Peering (Enterprise Premium)

MetaMind deploys a dedicated API cluster in the tenant's VPC. The tenant's data never
leaves their network. This requires custom deployment and is priced accordingly.

## Operational Playbook

### Adding a New Tenant

1. Create tenant record in the global config database
2. Assign tenant to a region based on their database location
3. Provision metadata namespace (create tenant_id prefix in catalog)
4. Issue initial JWT with default role (analyst)
5. Run schema discovery against the tenant's database
6. Configure billing (Stripe customer + subscription)

### Upgrading a Tenant

1. Update the tenant's subscription in Stripe
2. Update feature flags in `FeatureFlagsSettings` for the tenant
3. Issue new JWT with updated `features` claim
4. Notify tenant of newly available features

### Incident Response

**Severity 1 (Service Down):** All tenants affected. Response time: 15 minutes.
Escalation: on-call engineer plus engineering manager.

**Severity 2 (Degraded):** Performance degradation or single-tenant outage. Response
time: 1 hour. Escalation: on-call engineer.

**Severity 3 (Minor):** Feature-specific issue, non-blocking. Response time: 4 hours.
Escalation: feature team.

## Growth and Scale

### Metadata Sharding

At scale (>10,000 tenants), the metadata catalog can be sharded by tenant_id range. Each
shard handles a subset of tenants. Shard assignment uses consistent hashing to minimize
rebalancing when shards are added.

### Redis Sizing

| Tenant Count | Avg Queries/Day | Redis Memory | Redis Config        |
|-------------|-----------------|--------------|---------------------|
| < 100       | < 100K          | 1 GB         | Single node         |
| 100-1000    | 100K-10M        | 8 GB         | Single node, large  |
| 1000-10000  | 10M-1B          | 32 GB        | Redis Cluster (3+3) |
| > 10000     | > 1B            | 128 GB+      | Redis Cluster (6+6) |

### Connector Pool Tuning

Default pool size per backend: 5 connections, max overflow 10. For enterprise tenants
with dedicated backends, pool size can be increased to 20 with max overflow 30. Monitor
`pool_checkedout` and `pool_overflow` metrics to detect pool exhaustion.
