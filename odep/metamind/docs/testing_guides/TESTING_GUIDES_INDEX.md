# Testing Guides Index

Use this index as the starting point for feature-wise validation.

## Core guides
- `ML_TESTING_GUIDE.md`
- `QUERY_ROUTING_PIPELINE_TESTING_GUIDE.md`
- `CACHE_TESTING_GUIDE.md`
- `CDC_FRESHNESS_TESTING_GUIDE.md`
- `SECURITY_TESTING_GUIDE.md`
- `OBSERVABILITY_TESTING_GUIDE.md`

## Suggested execution order
1. `QUERY_ROUTING_PIPELINE_TESTING_GUIDE.md`
2. `SECURITY_TESTING_GUIDE.md`
3. `OBSERVABILITY_TESTING_GUIDE.md`
4. `CACHE_TESTING_GUIDE.md`
5. `CDC_FRESHNESS_TESTING_GUIDE.md`
6. `ML_TESTING_GUIDE.md`

## Notes
- Some checks depend on optional components (Oracle/Spark/CDC pipeline).
- In minimal/local setups, zero values can be valid for advanced metrics.
- Prefer verifying endpoint stability and schema consistency first.

## One-command smoke runner
```bash
./scripts/RUN_ALL_TEST_GUIDES.sh
```

Optional:
```bash
BASE_URL=http://localhost:8000 TENANT_ID=default ./scripts/RUN_ALL_TEST_GUIDES.sh
```
