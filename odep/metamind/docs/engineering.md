# MetaMind Engineering Guide

## Development Setup

### Prerequisites

MetaMind requires Python 3.11+, Node.js 18+ (for frontend), and Redis 7+ (optional,
for caching). PostgreSQL 15+ is recommended for production but SQLite works for
development.

### Quick Start

Clone the repository and install dependencies:

```bash
git clone https://github.com/metamind/metamind-platform.git
cd metamind-platform
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set environment variables:

```bash
export METAMIND_DB_URL="sqlite:///metamind_dev.db"
export METAMIND_LOG_LEVEL="DEBUG"
export METAMIND_STORAGE_PROVIDER="local"
```

Run the API server:

```bash
python -m metamind.api.server
```

Run tests:

```bash
pytest tests/unit/ -v
```

### Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

## Tutorials

### Adding a New Backend Engine

MetaMind supports adding custom backend engines through the `BackendConnector` ABC.

**Step 1: Implement the connector.**

Create a new file in `metamind/core/execution/`:

```python
from metamind.core.execution.backends import BackendConnector

class MyNewBackend(BackendConnector):
    def engine_name(self) -> str:
        return "my_engine"

    def execute_sql(self, sql, params=None):
        # Connect to your engine and execute
        connection = self._connect()
        cursor = connection.execute(sql, params or {})
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def capabilities(self) -> dict[str, bool]:
        return {
            "sql_execution": True,
            "vector_search": False,
            "transactions": True,
            "columnar_storage": False,
        }
```

**Step 2: Register the connector.**

```python
from metamind.core.execution.backends import BackendRegistry

registry = BackendRegistry()
registry.register("my_engine", MyNewBackend())
```

**Step 3: Test the connector.**

Write unit tests that verify SQL execution returns correct results. Mock the actual
database connection for unit tests, and use a real instance for integration tests.

**Step 4: Add dialect support.**

If your engine has non-standard SQL syntax, extend the relevant planner classes to
generate compatible queries. For example, if adding vector search support, add a
`_build_myengine_sql()` method to `VectorSearchPlanner`.

### Adding a New Feature Flag

Feature flags control access to MetaMind's capabilities. To add a new flag:

1. Add the flag to `FeatureFlagsSettings.flags` in `metamind/config/settings.py`:

```python
"F31_new_feature": False,  # Default off until validated
```

2. Gate the feature in the relevant code path:

```python
if settings.features.is_enabled("F31_new_feature"):
    # New behavior
else:
    # Existing behavior
```

3. Document the flag in the deployment guide's environment variables table.

### Extending the Cost Model

The `CostModel` class can be extended with new operator types:

```python
class ExtendedCostModel(CostModel):
    def estimate(self, node, stats):
        if node.node_type == "VectorScan":
            rows = stats.get("row_count", 1000)
            dims = stats.get("dimensions", 768)
            return CostVector(cpu=rows * dims * 0.001)
        return super().estimate(node, stats)
```

Register the extended model when creating optimizers:

```python
optimizer = create_optimizer(catalog, cost_model=ExtendedCostModel())
```

### Writing Tests

MetaMind uses pytest with the following conventions.

**Fixtures:** Common test fixtures are defined in `tests/conftest.py`. Key fixtures
include `catalog` (pre-populated MetadataCatalog), `cost_model` (default CostModel),
and `backend_registry` (BackendRegistry with mock connectors).

**Unit Tests:** Located in `tests/unit/`. These test individual classes and functions
in isolation. External dependencies (databases, APIs, Redis) must be mocked.

```python
class TestMyFeature(unittest.TestCase):
    def setUp(self):
        self.catalog = MetadataCatalog()
        # Register test tables...

    def test_specific_behavior(self):
        result = my_function(self.catalog)
        self.assertEqual(result.status, "success")
```

**Integration Tests:** Located in `tests/integration/`. These test component interactions
with real (or containerized) dependencies. Use pytest markers to skip when dependencies
are unavailable:

```python
@pytest.mark.skipif(not HAS_POSTGRES, reason="Postgres not available")
def test_postgres_backend():
    ...
```

**Mock Patterns:** Use `unittest.mock.patch` for external dependencies:

```python
from unittest.mock import patch, MagicMock

def test_openai_call():
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "choices": [{"message": {"content": "SELECT 1"}}]
        }).encode()
        mock_urlopen.return_value.__enter__ = lambda s: mock_response
        # Test the generator...
```

### Performance Profiling

MetaMind includes benchmark scripts in `benchmarks/` for measuring optimizer performance:

```bash
python benchmarks/optimizer_benchmark.py --tables 10 --queries 1000
```

Key metrics to monitor: optimization latency (p50, p95, p99), cost estimation accuracy
(predicted vs actual), cache hit ratio, and memory usage per tenant.

**Regression Detection:** Run benchmarks before and after changes. A regression is
flagged if p95 latency increases by more than 10% or if cost estimation error increases
by more than 5%.

## CI/CD Pipeline

The CI pipeline runs on every pull request:

1. `python scripts/compile_all.py` — verify all files compile
2. `python scripts/validate_imports.py` — check for circular imports
3. `pytest tests/unit/ -v` — run unit tests
4. `flake8 metamind/` — lint check
5. `mypy metamind/` — type check (when configured)
6. File size check — ensure no file exceeds 700 lines

The CD pipeline deploys on merge to main:

1. Build Docker image
2. Run integration tests against staging
3. Deploy to staging environment
4. Run smoke tests
5. Promote to production (manual approval)

## Code Standards: The 10 Golden Rules

1. **Every file <= 700 lines.** Split large files by responsibility.
2. **Every Python file must compile.** No syntax errors, no broken imports.
3. **Strict layered architecture.** api -> core -> execution -> storage. No upward imports.
4. **Tenant isolation everywhere.** Every DB query includes `tenant_id`.
5. **No dynamic global state.** Config via DI or explicit context objects.
6. **SQLAlchemy Core only.** No ORM. Use `text()` for raw SQL.
7. **Real implementations.** No pass-only bodies, no stubs, no placeholders.
8. **Type hints everywhere.** `from __future__ import annotations` at the top of every file.
9. **Feature flags for all F01-F30.** Every feature can be toggled independently.
10. **No print() — use logging.** Structured logging with appropriate levels.

### Naming Conventions

Modules use `snake_case`. Classes use `PascalCase`. Constants use `UPPER_SNAKE_CASE`.
Private methods prefix with `_`. Test methods prefix with `test_`. Dataclasses are
preferred over plain dicts for structured data.

### File Organization

Each feature lives in its own directory under `metamind/core/`. The directory contains
the main implementation file(s) and any supporting modules. Tests mirror the source
structure under `tests/unit/`.

```
metamind/
  core/
    vector/        # F19: Vector search
      search.py    # Main search engine
      batch.py     # Batch processor
      operators.py # Filter/aggregate ops
    nl_interface/  # F28: NL interface
      generator.py # NL-to-SQL generator
    rewrite/       # F29: Query rewrite
      analyzer.py  # Anti-pattern detection
    replay/        # F30: What-if replay
      recorder.py  # Recorder and simulator
```
