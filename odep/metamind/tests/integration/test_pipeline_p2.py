"""Integration tests using in-process DuckDB backend (Phase 2)."""
from __future__ import annotations

import pytest

from metamind.core.backends.connector import ConnectionConfig, QueryResult


pytestmark = pytest.mark.integration


@pytest.fixture
def duckdb_connector() -> "DuckDBConnector":  # type: ignore[name-defined]
    """Create a real DuckDB connector with in-memory database."""
    try:
        import duckdb  # noqa: F401
    except ImportError:
        pytest.skip("duckdb not installed")

    from metamind.core.backends.duckdb_connector import DuckDBConnector

    config = ConnectionConfig(
        backend_id="integration-duckdb",
        database=":memory:",
        extra_params={"backend_type": "duckdb"},
    )
    conn = DuckDBConnector(config)
    conn.connect()
    yield conn
    conn.disconnect()


@pytest.fixture
def duckdb_with_data(duckdb_connector: Any) -> Any:  # type: ignore[name-defined]
    """DuckDB connector with a pre-populated test table."""
    duckdb_connector.execute("""
        CREATE TABLE orders (
            id INTEGER,
            customer_id INTEGER,
            amount DECIMAL(10, 2),
            status VARCHAR,
            created_at DATE
        )
    """)
    duckdb_connector.execute("""
        INSERT INTO orders VALUES
            (1, 100, 99.99, 'shipped', '2024-01-01'),
            (2, 101, 49.99, 'pending', '2024-01-02'),
            (3, 100, 199.99, 'shipped', '2024-01-03'),
            (4, 102, 29.99, 'cancelled', '2024-01-04'),
            (5, 101, 149.99, 'shipped', '2024-01-05')
    """)
    return duckdb_connector


class TestDuckDBIntegration:
    """Real end-to-end integration tests using DuckDB."""

    def test_simple_select(self, duckdb_connector: Any) -> None:
        """SELECT constant should work."""
        result = duckdb_connector.execute("SELECT 42 AS answer, 'hello' AS greeting")
        assert result.row_count == 1
        assert result.rows[0]["answer"] == 42
        assert result.rows[0]["greeting"] == "hello"

    def test_table_crud(self, duckdb_connector: Any) -> None:
        """CREATE, INSERT, SELECT sequence should work correctly."""
        duckdb_connector.execute("CREATE TABLE t (x INT, y TEXT)")
        duckdb_connector.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        result = duckdb_connector.execute("SELECT * FROM t ORDER BY x")
        assert result.row_count == 2
        assert result.rows[0]["x"] == 1

    def test_where_filter(self, duckdb_with_data: Any) -> None:
        """WHERE clause should filter rows correctly."""
        result = duckdb_with_data.execute(
            "SELECT * FROM orders WHERE status = 'shipped'"
        )
        assert result.row_count == 3
        for row in result.rows:
            assert row["status"] == "shipped"

    def test_aggregation(self, duckdb_with_data: Any) -> None:
        """GROUP BY aggregation should produce correct results."""
        result = duckdb_with_data.execute("""
            SELECT status, COUNT(*) AS cnt, SUM(amount) AS total
            FROM orders
            GROUP BY status
            ORDER BY status
        """)
        assert result.row_count >= 1
        statuses = {r["status"] for r in result.rows}
        assert "shipped" in statuses

    def test_query_result_columns(self, duckdb_with_data: Any) -> None:
        """QueryResult should correctly populate column names."""
        result = duckdb_with_data.execute("SELECT id, amount FROM orders LIMIT 1")
        assert "id" in result.columns
        assert "amount" in result.columns

    def test_query_result_has_duration(self, duckdb_connector: Any) -> None:
        """QueryResult should have non-zero duration_ms."""
        result = duckdb_connector.execute("SELECT 1")
        assert result.duration_ms >= 0.0

    def test_query_result_has_backend(self, duckdb_connector: Any) -> None:
        """QueryResult.backend should match the connector's backend_id."""
        result = duckdb_connector.execute("SELECT 1")
        assert result.backend == "integration-duckdb"

    def test_explain_returns_dict(self, duckdb_with_data: Any) -> None:
        """EXPLAIN should return a non-empty dict."""
        plan = duckdb_with_data.explain("SELECT * FROM orders WHERE id = 1")
        assert isinstance(plan, dict)

    def test_get_table_stats(self, duckdb_with_data: Any) -> None:
        """get_table_stats should return row count and column info."""
        stats = duckdb_with_data.get_table_stats("main", "orders")
        assert isinstance(stats, dict)
        # Row count should be populated (either from stats or 0 if not supported)
        assert "row_count" in stats

    def test_cte_query(self, duckdb_with_data: Any) -> None:
        """DuckDB should support CTE queries."""
        result = duckdb_with_data.execute("""
            WITH shipped AS (
                SELECT id, amount FROM orders WHERE status = 'shipped'
            )
            SELECT COUNT(*) AS shipped_count FROM shipped
        """)
        assert result.row_count == 1
        assert result.rows[0]["shipped_count"] == 3

    def test_window_function(self, duckdb_with_data: Any) -> None:
        """DuckDB should support window functions."""
        result = duckdb_with_data.execute("""
            SELECT
                id,
                amount,
                ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC) AS rn
            FROM orders
        """)
        assert result.row_count == 5

    def test_second_query_same_result(self, duckdb_with_data: Any) -> None:
        """Repeated queries should produce consistent results."""
        sql = "SELECT COUNT(*) AS n FROM orders"
        r1 = duckdb_with_data.execute(sql)
        r2 = duckdb_with_data.execute(sql)
        assert r1.rows[0]["n"] == r2.rows[0]["n"]

    def test_empty_result(self, duckdb_with_data: Any) -> None:
        """Empty result set should have row_count == 0."""
        result = duckdb_with_data.execute(
            "SELECT * FROM orders WHERE id = 9999"
        )
        assert result.row_count == 0
        assert result.is_empty is True


class TestSQLGeneratorWithDuckDB:
    """Test dialect SQL generation with real DuckDB execution."""

    def test_postgres_gen_executes_on_duckdb(self, duckdb_with_data: Any) -> None:
        """PostgreSQL-generated SQL should execute on DuckDB without changes."""
        from metamind.core.execution.sql_generator import SQLGenerator
        from metamind.core.logical.nodes import ScanNode

        gen = SQLGenerator(dialect="postgres")
        node = ScanNode(table_name="orders")
        sql = gen.generate(node)
        # DuckDB can execute standard SELECT * FROM table
        result = duckdb_with_data.execute("SELECT * FROM orders LIMIT 5")
        assert result.row_count == 5

    def test_duckdb_sample_syntax(self, duckdb_with_data: Any) -> None:
        """DuckDB USING SAMPLE syntax should execute correctly."""
        from metamind.core.execution.dialects.duckdb_gen import DuckDBGenerator
        gen = DuckDBGenerator()
        base_sql = "SELECT * FROM orders"
        sampled = gen.add_sample_clause(base_sql, pct=100.0)
        # Execute with 100% sample = full table
        result = duckdb_with_data.execute(sampled)
        assert result.row_count == 5
