"""Unit tests for dialect SQL generators (Phase 2 — F17)."""
from __future__ import annotations

import pytest

from metamind.core.logical.nodes import (
    AggregateNode,
    AggregateExpr,
    AggFunc,
    FilterNode,
    JoinNode,
    JoinType,
    LimitNode,
    Predicate,
    ProjectNode,
    ScanNode,
    SortKey,
    SortNode,
    SortDirection,
)


# ── Fixtures ───────────────────────────────────────────────────

@pytest.fixture
def simple_scan() -> ScanNode:
    """Return a simple ScanNode for testing."""
    return ScanNode(table_name="orders", alias="o")


@pytest.fixture
def simple_filter(simple_scan: ScanNode) -> FilterNode:
    """Return a FilterNode with a single predicate."""
    return FilterNode(
        child=simple_scan,
        predicates=[Predicate(column="status", operator="=", value="shipped")],
    )


# ── Base generator (node_to_sql) ──────────────────────────────

class TestBaseGenerator:
    """Tests for the base node_to_sql function."""

    def test_scan_node_emits_from(self, simple_scan: ScanNode) -> None:
        from metamind.core.execution.dialects.base_gen import node_to_sql
        sql = node_to_sql(simple_scan)
        assert "FROM" in sql.upper()
        assert "orders" in sql

    def test_filter_node_emits_where(self, simple_filter: FilterNode) -> None:
        from metamind.core.execution.dialects.base_gen import node_to_sql
        sql = node_to_sql(simple_filter)
        assert "WHERE" in sql.upper()
        assert "status" in sql

    def test_project_node_selects_columns(self, simple_scan: ScanNode) -> None:
        from metamind.core.execution.dialects.base_gen import node_to_sql
        proj = ProjectNode(child=simple_scan, columns=["id", "amount"])
        sql = node_to_sql(proj)
        assert "id" in sql
        assert "amount" in sql

    def test_limit_node_emits_limit(self, simple_scan: ScanNode) -> None:
        from metamind.core.execution.dialects.base_gen import node_to_sql
        limit_node = LimitNode(child=simple_scan, limit=100)
        sql = node_to_sql(limit_node)
        assert "LIMIT" in sql.upper()
        assert "100" in sql

    def test_sort_node_emits_order_by(self, simple_scan: ScanNode) -> None:
        from metamind.core.execution.dialects.base_gen import node_to_sql
        sort = SortNode(
            child=simple_scan,
            sort_keys=[SortKey(column="created_at", direction=SortDirection.DESC)],
        )
        sql = node_to_sql(sort)
        assert "ORDER BY" in sql.upper()
        assert "created_at" in sql

    def test_in_predicate(self, simple_scan: ScanNode) -> None:
        from metamind.core.execution.dialects.base_gen import node_to_sql
        node = FilterNode(
            child=simple_scan,
            predicates=[Predicate(column="status", operator="IN", value=["a", "b"])],
        )
        sql = node_to_sql(node)
        assert "IN" in sql.upper()


# ── PostgreSQL generator ──────────────────────────────────────

class TestPostgreSQLGenerator:
    """Tests for PostgreSQLGenerator."""

    def test_generate_select_from_scan(self, simple_scan: ScanNode) -> None:
        from metamind.core.execution.dialects.postgres_gen import PostgreSQLGenerator
        gen = PostgreSQLGenerator()
        sql = gen.generate(simple_scan)
        assert "SELECT" in sql.upper()

    def test_add_index_hint_prepends_comment(self, simple_scan: ScanNode) -> None:
        from metamind.core.execution.dialects.postgres_gen import PostgreSQLGenerator
        gen = PostgreSQLGenerator()
        sql = "SELECT * FROM orders"
        hinted = gen.add_index_hint(sql, "orders", "idx_status")
        assert "/*+" in hinted
        assert "IndexScan" in hinted
        assert "idx_status" in hinted

    def test_add_parallel_hint(self, simple_scan: ScanNode) -> None:
        from metamind.core.execution.dialects.postgres_gen import PostgreSQLGenerator
        gen = PostgreSQLGenerator()
        sql = "SELECT * FROM big_table"
        result = gen.add_parallel_hint(sql, workers=4)
        assert "Parallel" in result
        assert "4" in result

    def test_set_work_mem_prepends_set(self) -> None:
        from metamind.core.execution.dialects.postgres_gen import PostgreSQLGenerator
        gen = PostgreSQLGenerator()
        sql = "SELECT 1"
        result = gen.set_work_mem(sql, 64)
        assert "work_mem" in result.lower()
        assert "64MB" in result

    def test_sql_hint_passes_through(self, simple_scan: ScanNode) -> None:
        from metamind.core.execution.dialects.postgres_gen import PostgreSQLGenerator
        gen = PostgreSQLGenerator()
        hint = "SELECT id FROM orders WHERE status = 'done'"
        result = gen.generate(simple_scan, sql_hint=hint)
        # Should contain the query content (possibly transpiled)
        assert "orders" in result or "SELECT" in result.upper()


# ── Snowflake generator ───────────────────────────────────────

class TestSnowflakeGenerator:
    """Tests for SnowflakeGenerator."""

    def test_add_result_cache_bypass(self) -> None:
        from metamind.core.execution.dialects.snowflake_gen import SnowflakeGenerator
        gen = SnowflakeGenerator()
        sql = "SELECT 1"
        result = gen.add_result_cache_bypass(sql)
        assert "USE_CACHED_RESULT" in result

    def test_add_warehouse_hint(self) -> None:
        from metamind.core.execution.dialects.snowflake_gen import SnowflakeGenerator
        gen = SnowflakeGenerator()
        result = gen.add_warehouse_hint("SELECT 1", "LARGE_WH")
        assert "LARGE_WH" in result
        assert "USE WAREHOUSE" in result

    def test_qualify_table_names(self) -> None:
        from metamind.core.execution.dialects.snowflake_gen import SnowflakeGenerator
        gen = SnowflakeGenerator()
        sql = "SELECT * FROM orders JOIN customers ON orders.cid = customers.id"
        result = gen.qualify_table_names(sql, "MYDB", "PUBLIC")
        assert "MYDB.PUBLIC." in result

    def test_translate_oracle_trunc(self) -> None:
        from metamind.core.execution.dialects.snowflake_gen import SnowflakeGenerator
        gen = SnowflakeGenerator()
        sql = "SELECT TRUNC(created_at, 'MM') FROM events"
        result = gen.translate_date_functions(sql)
        assert "DATE_TRUNC" in result.upper()

    def test_translate_array_collect(self) -> None:
        from metamind.core.execution.dialects.snowflake_gen import SnowflakeGenerator
        gen = SnowflakeGenerator()
        sql = "SELECT ARRAY_COLLECT(id) FROM t"
        result = gen.translate_array_functions(sql)
        assert "ARRAY_AGG" in result.upper()


# ── BigQuery generator ────────────────────────────────────────

class TestBigQueryGenerator:
    """Tests for BigQueryGenerator."""

    def test_translate_date_trunc_pg_to_bq(self) -> None:
        from metamind.core.execution.dialects.bigquery_gen import BigQueryGenerator
        gen = BigQueryGenerator()
        pg_sql = "SELECT DATE_TRUNC('month', created_at) FROM events"
        result = gen.translate_date_functions(pg_sql)
        # BQ format: DATE_TRUNC(col, MONTH)
        assert "MONTH" in result.upper()
        assert "created_at" in result

    def test_qualify_table_names_three_part(self) -> None:
        from metamind.core.execution.dialects.bigquery_gen import BigQueryGenerator
        gen = BigQueryGenerator()
        sql = "SELECT * FROM orders"
        result = gen.qualify_table_names(sql, "my-project", "analytics")
        assert "my-project.analytics." in result

    def test_add_partition_filter_no_where(self) -> None:
        from metamind.core.execution.dialects.bigquery_gen import BigQueryGenerator
        gen = BigQueryGenerator()
        sql = "SELECT * FROM events"
        result = gen.add_partition_filter(
            sql, "event_date", ("2024-01-01", "2024-01-31")
        )
        assert "event_date" in result
        assert "2024-01-01" in result

    def test_now_replaced_with_current_timestamp(self) -> None:
        from metamind.core.execution.dialects.bigquery_gen import BigQueryGenerator
        gen = BigQueryGenerator()
        sql = "SELECT NOW() AS ts"
        result = gen.translate_date_functions(sql)
        assert "CURRENT_TIMESTAMP" in result.upper()


# ── Redshift generator ────────────────────────────────────────

class TestRedshiftGenerator:
    """Tests for RedshiftGenerator."""

    def test_add_distkey_hint_comment(self) -> None:
        from metamind.core.execution.dialects.redshift_gen import RedshiftGenerator
        gen = RedshiftGenerator()
        result = gen.add_distkey_hint("SELECT 1", "orders")
        assert "DISTKEY" in result
        assert "orders" in result

    def test_add_sortkey_hint_comment(self) -> None:
        from metamind.core.execution.dialects.redshift_gen import RedshiftGenerator
        gen = RedshiftGenerator()
        result = gen.add_sortkey_hint("SELECT 1", "events")
        assert "SORTKEY" in result

    def test_translate_window_removes_nulls_first(self) -> None:
        from metamind.core.execution.dialects.redshift_gen import RedshiftGenerator
        gen = RedshiftGenerator()
        sql = "SELECT ROW_NUMBER() OVER (ORDER BY id NULLS FIRST) FROM t"
        result = gen.translate_window_functions(sql)
        assert "NULLS FIRST" not in result

    def test_add_query_group(self) -> None:
        from metamind.core.execution.dialects.redshift_gen import RedshiftGenerator
        gen = RedshiftGenerator()
        result = gen.add_query_group("SELECT 1", "analytics_queue")
        assert "query_group" in result.lower()
        assert "analytics_queue" in result


# ── DuckDB generator ──────────────────────────────────────────

class TestDuckDBGenerator:
    """Tests for DuckDBGenerator."""

    def test_add_parallel_pragma(self) -> None:
        from metamind.core.execution.dialects.duckdb_gen import DuckDBGenerator
        gen = DuckDBGenerator()
        result = gen.add_parallel_hint("SELECT 1", threads=8)
        assert "PRAGMA threads=8" in result

    def test_add_sample_clause(self) -> None:
        from metamind.core.execution.dialects.duckdb_gen import DuckDBGenerator
        gen = DuckDBGenerator()
        result = gen.add_sample_clause("SELECT * FROM big_table", pct=5.0)
        assert "USING SAMPLE" in result.upper()
        assert "5.0" in result

    def test_add_memory_limit(self) -> None:
        from metamind.core.execution.dialects.duckdb_gen import DuckDBGenerator
        gen = DuckDBGenerator()
        result = gen.add_memory_limit("SELECT 1", memory_gb=2.0)
        assert "memory_limit" in result
        assert "2.0GB" in result

    def test_translate_array_collect(self) -> None:
        from metamind.core.execution.dialects.duckdb_gen import DuckDBGenerator
        gen = DuckDBGenerator()
        sql = "SELECT ARRAY_COLLECT(id) FROM t"
        result = gen.translate_array_functions(sql)
        assert "ARRAY_AGG" in result.upper()


# ── Spark SQL generator ───────────────────────────────────────

class TestSparkSQLGenerator:
    """Tests for SparkSQLGenerator."""

    def test_add_broadcast_hint(self) -> None:
        from metamind.core.execution.dialects.spark_gen import SparkSQLGenerator
        gen = SparkSQLGenerator()
        sql = "SELECT * FROM orders o JOIN customers c ON o.cid = c.id"
        result = gen.add_broadcast_hint(sql, "c")
        assert "/*+ BROADCAST" in result
        assert "c" in result

    def test_add_repartition_hint(self) -> None:
        from metamind.core.execution.dialects.spark_gen import SparkSQLGenerator
        gen = SparkSQLGenerator()
        sql = "SELECT * FROM events"
        result = gen.add_repartition_hint(sql, col="event_date", n=100)
        assert "REPARTITION" in result
        assert "100" in result

    def test_add_skew_hint(self) -> None:
        from metamind.core.execution.dialects.spark_gen import SparkSQLGenerator
        gen = SparkSQLGenerator()
        result = gen.add_skew_hint("SELECT * FROM t", table="t", col="user_id")
        assert "SKEWJOIN" in result

    def test_translate_date_trunc_unit_lowercased(self) -> None:
        from metamind.core.execution.dialects.spark_gen import SparkSQLGenerator
        gen = SparkSQLGenerator()
        sql = "SELECT DATE_TRUNC('MONTH', ts) FROM events"
        result = gen.translate_date_functions(sql)
        assert "month" in result.lower()

    def test_set_shuffle_partitions(self) -> None:
        from metamind.core.execution.dialects.spark_gen import SparkSQLGenerator
        gen = SparkSQLGenerator()
        result = gen.set_shuffle_partitions("SELECT 1", n=400)
        assert "shuffle.partitions" in result
        assert "400" in result

    def test_now_replaced(self) -> None:
        from metamind.core.execution.dialects.spark_gen import SparkSQLGenerator
        gen = SparkSQLGenerator()
        result = gen.translate_date_functions("SELECT NOW()")
        assert "CURRENT_TIMESTAMP" in result.upper()


# ── SQLGenerator router ───────────────────────────────────────

class TestSQLGeneratorRouter:
    """Tests for the top-level SQLGenerator router."""

    def test_router_postgres_dialect(self, simple_scan: ScanNode) -> None:
        from metamind.core.execution.sql_generator import SQLGenerator
        gen = SQLGenerator(dialect="postgres")
        sql = gen.generate(simple_scan)
        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_router_unknown_dialect_fallback(self, simple_scan: ScanNode) -> None:
        from metamind.core.execution.sql_generator import SQLGenerator
        gen = SQLGenerator(dialect="unknown_engine_xyz")
        sql = gen.generate(simple_scan, sql_hint="SELECT 1")
        # Should return some SQL string without raising
        assert isinstance(sql, str)

    def test_router_generate_from_sql(self) -> None:
        from metamind.core.execution.sql_generator import SQLGenerator
        gen = SQLGenerator(dialect="duckdb")
        result = gen.generate_from_sql("SELECT id FROM orders WHERE status = 'done'")
        assert isinstance(result, str)
        assert "orders" in result
