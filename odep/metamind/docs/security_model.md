# MetaMind Security Model

## Multi-Tenant Isolation

### tenant_id Enforcement

Every data access path in MetaMind requires an explicit `tenant_id` parameter. This is
enforced at multiple levels.

At the API layer, every request must include a `tenant_id` in the request body or JWT
claims. The API server validates that the authenticated user has access to the requested
tenant before routing to any handler.

At the metadata layer, the `MetadataCatalog` maintains separate dictionaries per tenant.
The methods `get_table()`, `get_indexes()`, `get_statistics()`, and all vector index
methods require `tenant_id` as the first parameter. There is no global accessor.

At the storage layer, all storage keys are prefixed with the tenant_id to prevent
cross-tenant data access: `{tenant_id}/{feature}/{key}`.

At the cache layer, Redis keys follow the namespace pattern `mm:{tenant_id}:{feature}:{key}`.
Cache invalidation is always scoped to a single tenant.

### Cache Namespacing

```
mm:tenant_abc:cache:query_hash_1234    # Query plan cache
mm:tenant_abc:stats:orders             # Statistics cache
mm:tenant_abc:vector:embeddings_idx    # Vector index cache
```

Cache TTLs are per-tenant configurable. Cache flush operations only affect the
requesting tenant's namespace.

### ML Model Isolation

Learned cardinality models (F01) and workload classifiers (F24) are trained per-tenant.
Model artifacts are stored under `{tenant_id}/models/` in the storage backend. A model
trained on Tenant A's data is never used for Tenant B's queries.

## Authentication and Authorization

### JWT Structure

MetaMind uses JWT tokens for API authentication. The expected token structure:

```json
{
  "sub": "user_id_12345",
  "tenant_id": "tenant_abc",
  "roles": ["admin", "analyst"],
  "features": ["F01", "F19", "F28", "F29", "F30"],
  "exp": 1735689600,
  "iss": "metamind-auth"
}
```

The `features` claim controls which feature APIs the user can access. This integrates
with the feature flag system — a user can only use features that are both enabled for
the tenant AND present in their JWT.

### RBAC Roles

MetaMind defines four roles with increasing privileges:

**viewer** — Read-only access: can view query plans, suggestions, and simulation results.
Cannot execute queries or create scenarios.

**analyst** — Can execute NL queries, view suggestions, and create what-if scenarios.
Cannot modify indexes or change configuration.

**admin** — Full access to all features including index management, feature flag
configuration, and tenant settings.

**superadmin** — Cross-tenant access for platform operators. Can view system health,
manage tenants, and access audit logs.

### Feature Flag Access Control

Feature access is a three-way gate: the feature flag must be enabled globally for the
tenant (in `FeatureFlagsSettings`), the user's JWT must include the feature in the
`features` claim, and the user's role must have sufficient privileges for the operation.

## Column-Level Security

### Masking Rules

MetaMind supports column-level data masking through `mm_masking_rules`. Masking is
applied after query execution but before results are returned to the user.

**Masking Types:**

`hash` — Replaces the column value with a deterministic SHA-256 hash. The same input
always produces the same hash, allowing JOIN operations on masked data while preventing
value recovery.

`nullify` — Replaces the column value with NULL. Used for columns that should be
completely hidden from certain users.

`partial` — Masks part of the value. For example, email `user@example.com` becomes
`u***@example.com`. Phone numbers `555-1234` become `***-1234`.

`redact` — Replaces with a fixed string `[REDACTED]`. Used for free-text fields that
might contain PII.

### Pipeline Integration

Masking rules are evaluated after the query executor returns results and before the
API response is serialized. The masking pipeline:

1. Load masking rules for the tenant and user role
2. Identify columns in the result set that have masking rules
3. Apply the appropriate masking function to each affected column
4. Return the masked result set

This approach ensures masking is applied regardless of the query structure — whether
the column appears in a SELECT, a subquery, or a JOIN result.

## Network Security

### TLS Configuration

All external API endpoints require TLS 1.2+. Internal service-to-service communication
within the same VPC can use plaintext for performance, but TLS is recommended.

Certificate management: use AWS ACM, GCP-managed certificates, or cert-manager for
Kubernetes. Certificates should auto-renew with at least 30 days before expiration.

### Internal vs External Endpoints

External endpoints (accessible from the internet): the REST API on port 8080 behind
an ALB/ingress with TLS termination.

Internal endpoints (VPC-only): the database on port 5432, Redis on port 6379, and
any inter-service communication. These should not be exposed outside the VPC.

### VPC Architecture

```
┌─────────────────────────────────────────┐
│                   VPC                    │
│  ┌──────────┐  ┌──────────┐             │
│  │ Public   │  │ Private  │             │
│  │ Subnet   │  │ Subnet   │             │
│  │          │  │          │             │
│  │  ALB     │  │  ECS     │             │
│  │  NAT GW  │  │  RDS     │             │
│  │          │  │  Redis   │             │
│  └──────────┘  └──────────┘             │
└─────────────────────────────────────────┘
```

## Secrets Management

### Environment Variable Patterns

For development: use `.env` files (never committed to git).

For staging/production: use the cloud provider's secret management service.

**AWS:** Secrets Manager or SSM Parameter Store. Reference in ECS task definitions:

```json
{
  "secrets": [
    {
      "name": "METAMIND_DB_URL",
      "valueFrom": "arn:aws:secretsmanager:region:account:secret:metamind/db-url"
    }
  ]
}
```

**GCP:** Secret Manager. Reference in Cloud Run:

```yaml
env:
  - name: METAMIND_DB_URL
    valueFrom:
      secretKeyRef:
        secret: metamind-db-url
        version: latest
```

**Azure:** Key Vault. Reference in Container Instances via managed identity.

## Audit Logging

### Optimization Decisions as Audit Trail

The `mm_optimization_decisions` table (used by F30 replay) doubles as an audit log.
Every query processed by MetaMind records: the tenant_id, query text, optimization
decisions made, cost estimates, and timestamp. This provides a complete audit trail
of all data access patterns.

### Retention

Default retention is 90 days. For compliance-sensitive deployments, retention can be
extended to 7 years by configuring the storage backend to archive older records to
cold storage (S3 Glacier, GCS Coldline, Azure Cool).

## Compliance

### GDPR

**Data Minimization:** MetaMind stores only metadata about queries and schemas. It does
not store query result data. The NL conversation history stores only the query text and
generated SQL, not the results.

**Right to Erasure:** Tenant deletion cascades to all metadata, statistics, vector
indexes, NL sessions, optimization decisions, and workload patterns. The
`delete_tenant()` operation removes all data associated with a tenant_id from all
tables and the storage backend.

**Data Processing Agreement:** MetaMind processes data as a data processor under the
tenant's instructions. No cross-tenant data sharing occurs.

### SOC 2

MetaMind's architecture supports SOC 2 Type II compliance through: access controls
(RBAC with JWT), audit logging (mm_optimization_decisions), encryption (TLS in transit,
KMS at rest), availability (multi-AZ deployment, health monitoring), and change
management (CI/CD pipeline with approval gates).

### HIPAA

For healthcare deployments: enable column-level masking on PHI columns, configure audit
log retention to 7 years, deploy in a HIPAA-eligible region with BAA, enable encryption
at rest for all storage backends, and restrict VPC access to authorized networks.
