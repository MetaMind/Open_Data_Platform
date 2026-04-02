# MetaMind Development Guide

## Overview

This guide covers setting up a development environment, running tests, and contributing to MetaMind.

## Prerequisites

- Python 3.11+
- Docker and Docker Compose
- PostgreSQL 15+ (or use Docker)
- Redis 7+ (or use Docker)
- Git

## Development Setup

### 1. Clone Repository

```bash
git clone https://github.com/metamind/metamind.git
cd metamind-platform
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
# Install core dependencies
pip install -r requirements.txt

# Install development dependencies
pip install -e ".[dev]"
```

### 4. Start Infrastructure Services

```bash
# Start PostgreSQL, Redis, MinIO
docker-compose up -d postgres redis minio

# Wait for services
sleep 10
```

### 5. Run Database Migrations

```bash
# Apply all migrations
psql -h localhost -U metamind -d metamind -f migrations/001_core.sql
psql -h localhost -U metamind -d metamind -f migrations/002_cdc_tracking.sql
# ... continue for all migrations
```

### 6. Start Development Server

```bash
# Start with auto-reload
python -m metamind.api.server

# Or using uvicorn directly
uvicorn metamind.api.server:app --reload --host 0.0.0.0 --port 8000
```

## Project Structure

```
metamind-platform/
├── metamind/                 # Main application
│   ├── api/                  # API layer
│   │   └── server.py         # FastAPI server
│   ├── cache/                # Caching layer
│   │   └── result_cache.py   # Multi-tier cache
│   ├── config/               # Configuration
│   │   └── settings.py       # Settings management
│   ├── core/                 # Core components
│   │   ├── router.py         # Query router
│   │   ├── cdc_monitor.py    # CDC monitoring
│   │   ├── logical/          # Logical planning
│   │   │   └── planner.py    # Cost-based planner
│   │   ├── physical/         # Physical execution
│   │   │   └── execution_graph.py  # DAG orchestration
│   │   └── control_plane.py  # Control plane
│   ├── execution/            # Execution engines
│   │   ├── trino_engine.py   # Trino connector
│   │   ├── oracle_connector.py  # Oracle connector
│   │   └── spark_engine.py   # Spark engine
│   ├── ml/                   # Machine learning
│   │   ├── cost_model.py     # Cost prediction model
│   │   └── feature_store.py  # Feature store
│   ├── observability/        # Observability
│   │   ├── query_tracer.py   # Query tracing
│   │   └── drift_detector.py # Drift detection
│   └── bootstrap.py          # Application bootstrap
├── tests/                    # Test suite
│   ├── unit/                 # Unit tests
│   ├── integration/          # Integration tests
│   └── conftest.py           # Test fixtures
├── migrations/               # Database migrations
├── docs/                     # Documentation
├── scripts/                  # Utility scripts
└── monitoring/               # Monitoring configs
```

## Development Workflow

### Branch Naming

| Type | Pattern | Example |
|------|---------|---------|
| Feature | `feature/*` | `feature/spark-engine` |
| Bugfix | `bugfix/*` | `bugfix/cache-race` |
| Hotfix | `hotfix/*` | `hotfix/circuit-breaker` |
| Release | `release/*` | `release/v4.1.0` |

### Commit Messages

Follow conventional commits:

```
feat: add Spark batch job routing
fix: resolve cache race condition
docs: update API documentation
test: add integration tests for CDC
refactor: simplify router logic
perf: optimize query parsing
```

### Code Style

```bash
# Format code
black metamind/ tests/

# Lint code
ruff check metamind/ tests/

# Type check
mypy metamind/

# Run all quality checks
make quality
```

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run unit tests only
pytest tests/unit -v

# Run integration tests (requires Docker)
pytest tests/integration -v

# Run with coverage
pytest --cov=metamind --cov-report=html --cov-report=term

# Run specific test
pytest tests/unit/test_router.py::test_routing_decision -v
```

### Writing Tests

```python
# tests/unit/test_example.py
import pytest
from metamind.core.router import QueryRouter

@pytest.fixture
def router():
    return QueryRouter(...)

@pytest.mark.asyncio
async def test_routing_decision(router):
    decision = await router.route(
        sql="SELECT * FROM orders",
        tenant_id="default",
        user_context={}
    )
    assert decision.target_source == "s3_analytics"
    assert decision.confidence > 0.7
```

### Test Categories

| Category | Location | Description |
|----------|----------|-------------|
| Unit | `tests/unit/` | Test individual components |
| Integration | `tests/integration/` | Test component interactions |
| E2E | `tests/e2e/` | Test full workflows |

## Debugging

### Local Debugging

```python
# Add breakpoint
import pdb; pdb.set_trace()

# Or use IPython
import IPython; IPython.embed()
```

### Logging

```python
import logging

logger = logging.getLogger(__name__)

# Log levels
logger.debug("Detailed debug info")
logger.info("Normal operation")
logger.warning("Recoverable issue")
logger.error("Failed operation")
```

### VS Code Configuration

```json
// .vscode/launch.json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Python: MetaMind Server",
      "type": "python",
      "request": "launch",
      "module": "metamind.api.server",
      "console": "integratedTerminal"
    }
  ]
}
```

## Adding New Features

### Adding a New Engine

1. Create engine class in `metamind/execution/`
2. Implement `execute()` method
3. Add engine to bootstrap
4. Update router to route to new engine
5. Add tests

```python
# metamind/execution/new_engine.py
class NewEngine:
    async def execute(self, sql: str) -> ExecutionResult:
        # Implementation
        pass
```

### Adding a New Router Strategy

1. Add strategy to `ExecutionStrategy` enum
2. Implement routing logic in `QueryRouter.route()`
3. Update decision dataclass
4. Add tests

### Adding ML Features

1. Add feature extraction in `FeatureStore`
2. Update cost model training
3. Add feature importance logging

## Database Migrations

### Creating Migrations

```bash
# Create new migration
scripts/create_migration.sh 011_new_feature

# Edit the generated file
vim migrations/011_new_feature.sql
```

### Migration Template

```sql
-- migrations/011_new_feature.sql
-- Description: Add new feature table
-- Author: Your Name
-- Date: 2024-03-04

BEGIN;

-- Add new table
CREATE TABLE mm_new_table (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Add index
CREATE INDEX idx_new_table_name ON mm_new_table(name);

COMMIT;
```

### Rollback Migrations

```bash
# Create rollback script
vim migrations/rollback/011_new_feature.sql
```

## Documentation

### API Documentation

API docs are auto-generated from FastAPI:

```bash
# Access Swagger UI
http://localhost:8000/docs

# Access ReDoc
http://localhost:8000/redoc
```

### Code Documentation

Use Google-style docstrings:

```python
def route_query(sql: str, tenant_id: str) -> RoutingDecision:
    """Route a query to the optimal execution engine.
    
    Args:
        sql: SQL query to route
        tenant_id: Tenant identifier for isolation
        
    Returns:
        RoutingDecision with target engine and execution plan
        
    Raises:
        ValueError: If query is invalid
        RoutingError: If no suitable engine found
    """
```

## Performance Optimization

### Profiling

```python
# cProfile
python -m cProfile -o profile.stats -m metamind.api.server

# line_profiler
@profile
def slow_function():
    pass
```

### Benchmarking

```bash
# Run benchmarks
pytest tests/benchmark/ --benchmark-only

# Load testing
locust -f tests/load/locustfile.py
```

## Release Process

### Version Bumping

```bash
# Update version
vim metamind/__init__.py

# Update changelog
vim CHANGELOG.md
```

### Building

```bash
# Build Docker image
docker build -t metamind:v4.0.0 .

# Push to registry
docker push metamind:v4.0.0
```

### Tagging

```bash
# Create git tag
git tag -a v4.0.0 -m "Release v4.0.0"
git push origin v4.0.0
```

## Contributing

### Pull Request Process

1. Create feature branch
2. Make changes with tests
3. Run quality checks
4. Update documentation
5. Submit PR with description
6. Address review comments
7. Merge after approval

### PR Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added
- [ ] Integration tests added
- [ ] Manual testing performed

## Checklist
- [ ] Code follows style guide
- [ ] Tests pass
- [ ] Documentation updated
- [ ] Changelog updated
```

## Troubleshooting

### Import Errors

```bash
# Reinstall package
pip install -e .

# Check Python path
python -c "import sys; print(sys.path)"
```

### Database Connection Issues

```bash
# Check PostgreSQL
pg_isready -h localhost

# Check migrations
psql -h localhost -U metamind -d metamind -c "\dt"
```

### Test Failures

```bash
# Run with verbose output
pytest -vvs tests/unit/test_router.py

# Run with pdb on failure
pytest --pdb tests/unit/test_router.py
```

## Resources

- [FastAPI Docs](https://fastapi.tiangolo.com/)
- [SQLGlot Docs](https://sqlglot.com/)
- [Trino Docs](https://trino.io/docs/)
- [Apache Spark Docs](https://spark.apache.org/docs/)
