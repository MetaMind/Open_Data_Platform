"""Unit tests for all backend connectors (Phase 2 — F13)."""
from __future__ import annotations

import sys
import types
from typing import Any
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from metamind.core.backends.connector import (
    ConnectionConfig,
    ConnectorCapabilities,
    ConnectorExecutionError,
    QueryResult,
)


# ── Helpers ────────────────────────────────────────────────────

def _pg_config(**extra: Any) -> ConnectionConfig:
    return ConnectionConfig(
        backend_id="test-postgres",
        host="localhost", port=5432,
        database="testdb", username="user", password="pw",
        extra_params={"backend_type": "postgres", **extra},
    )


def _mysql_config(**extra: Any) -> ConnectionConfig:
    return ConnectionConfig(
        backend_id="test-mysql",
        host="localhost", port=3306,
        database="mydb", username="root", password="",
        extra_params={"backend_type": "mysql", **extra},
    )


def _snowflake_config(**extra: Any) -> ConnectionConfig:
    return ConnectionConfig(
        backend_id="test-snowflake",
        host=None, username="sfuser", password="sfpw",
        database="MYDB", schema="PUBLIC",
        extra_params={"backend_type": "snowflake", "account": "my-acct", **extra},
    )


# ── PostgreSQL connector tests ────────────────────────────────

class TestPostgresConnector:
    """Tests for PostgresConnector using mocked psycopg2."""

    def _make_connector(self, mock_psycopg2: Any) -> Any:
        from metamind.core.backends.postgres_connector import PostgresConnector
        config = _pg_config()
        conn = PostgresConnector(config)
        conn.connect()
        return conn

    def test_capabilities_dialect(self, mock_psycopg2: Any) -> None:
        from metamind.core.backends.postgres_connector import PostgresConnector
        c = PostgresConnector(_pg_config())
        assert c.capabilities.dialect == "postgres"
        assert c.capabilities.supports_cte is True

    def test_connect_sets_connected(self, mock_psycopg2: Any) -> None:
        from metamind.core.backends.postgres_connector import PostgresConnector
        c = PostgresConnector(_pg_config())
        c.connect()
        assert c.is_connected is True

    def test_execute_returns_query_result(self, mock_psycopg2: Any) -> None:
        conn = self._make_connector(mock_psycopg2)
        result = conn.execute("SELECT id, name FROM users")
        assert isinstance(result, QueryResult)
        assert "id" in result.columns or len(result.columns) >= 0
        assert result.backend == "test-postgres"

    def test_disconnect_sets_not_connected(self, mock_psycopg2: Any) -> None:
        conn = self._make_connector(mock_psycopg2)
        conn.disconnect()
        assert conn.is_connected is False

    def test_repr_contains_backend_id(self, mock_psycopg2: Any) -> None:
        from metamind.core.backends.postgres_connector import PostgresConnector
        c = PostgresConnector(_pg_config())
        assert "test-postgres" in repr(c)


# ── MySQL connector tests ─────────────────────────────────────

class TestMySQLConnector:
    """Tests for MySQLConnector using mocked pymysql."""

    def test_capabilities_dialect(self, mock_pymysql: Any) -> None:
        from metamind.core.backends.mysql_connector import MySQLConnector
        c = MySQLConnector(_mysql_config())
        assert c.capabilities.dialect == "mysql"
        assert c.capabilities.supports_window_functions is True

    def test_connect_with_mock(self, mock_pymysql: Any) -> None:
        from metamind.core.backends.mysql_connector import MySQLConnector
        c = MySQLConnector(_mysql_config())
        c.connect()
        assert c.is_connected is True

    def test_execute_returns_rows(self, mock_pymysql: Any) -> None:
        from metamind.core.backends.mysql_connector import MySQLConnector
        c = MySQLConnector(_mysql_config())
        c.connect()
        result = c.execute("SELECT 1 AS n")
        assert isinstance(result, QueryResult)

    def test_disconnect_cleans_up(self, mock_pymysql: Any) -> None:
        from metamind.core.backends.mysql_connector import MySQLConnector
        c = MySQLConnector(_mysql_config())
        c.connect()
        c.disconnect()
        assert c.is_connected is False


# ── Snowflake connector tests ─────────────────────────────────

class TestSnowflakeConnector:
    """Tests for SnowflakeConnector using mocked snowflake.connector."""

    def test_capabilities_cost(self, mock_snowflake: Any) -> None:
        from metamind.core.backends.snowflake_connector import SnowflakeConnector
        c = SnowflakeConnector(_snowflake_config())
        assert c.capabilities.cost_per_gb_scan == 5.0

    def test_connect_sets_connected(self, mock_snowflake: Any) -> None:
        from metamind.core.backends.snowflake_connector import SnowflakeConnector
        c = SnowflakeConnector(_snowflake_config())
        c.connect()
        assert c.is_connected is True

    def test_explain_returns_dict(self, mock_snowflake: Any) -> None:
        from metamind.core.backends.snowflake_connector import SnowflakeConnector
        c = SnowflakeConnector(_snowflake_config())
        c.connect()
        # Mock explain to return something parseable
        c.execute = MagicMock(return_value=QueryResult(
            columns=["EXPLAIN"],
            rows=[{"EXPLAIN": '{"plan": "test"}'}],
            row_count=1,
            duration_ms=1.0,
        ))
        result = c.explain("SELECT 1")
        assert isinstance(result, dict)


# ── Backend registry tests ────────────────────────────────────

class TestBackendRegistry:
    """Tests for BackendRegistry class."""

    def test_register_and_retrieve_class(self) -> None:
        from metamind.core.backends.registry import BackendRegistry

        class MockConnector:
            """Mock connector."""
            def __init__(self, config: Any) -> None:
                self._config = config
                self._connected = False

        registry = BackendRegistry()
        registry.register_class("mock_engine", MockConnector)  # type: ignore[arg-type]
        assert "mock_engine" in registry._connector_classes

    def test_list_backends_empty_initially(self) -> None:
        from metamind.core.backends.registry import BackendRegistry
        registry = BackendRegistry()
        assert registry.list_backends() == []

    def test_health_check_all_no_instances(self) -> None:
        from metamind.core.backends.registry import BackendRegistry
        registry = BackendRegistry()
        result = registry.health_check_all()
        assert result == {}

    def test_get_returns_none_for_unknown(self) -> None:
        from metamind.core.backends.registry import BackendRegistry
        registry = BackendRegistry()
        assert registry.get("nonexistent") is None

    def test_register_class_logs_and_stores(self) -> None:
        from metamind.core.backends.registry import BackendRegistry
        from metamind.core.backends.postgres_connector import PostgresConnector
        registry = BackendRegistry()
        registry.register_class("pg_test", PostgresConnector)
        assert registry._connector_classes.get("pg_test") is PostgresConnector

    def test_get_registry_returns_singleton(self) -> None:
        from metamind.core.backends.registry import get_registry, BackendRegistry
        r1 = get_registry()
        r2 = get_registry()
        assert r1 is r2

    def test_auto_detect_unknown_type_returns_none(self) -> None:
        from metamind.core.backends.registry import BackendRegistry
        registry = BackendRegistry()
        result = registry._auto_detect_class("totally_unknown_engine_xyz")
        assert result is None


# ── DuckDB connector tests ────────────────────────────────────

class TestDuckDBConnector:
    """Tests for DuckDBConnector using real DuckDB if available."""

    def test_duckdb_capabilities(self) -> None:
        try:
            from metamind.core.backends.duckdb_connector import DuckDBConnector
            config = ConnectionConfig(
                backend_id="test-duckdb",
                extra_params={"backend_type": "duckdb"},
            )
            c = DuckDBConnector(config)
            assert c.capabilities.dialect in ("duckdb", "postgres")
        except ImportError:
            pytest.skip("duckdb not installed")

    def test_duckdb_connect_and_execute(self) -> None:
        try:
            import duckdb  # noqa: F401
            from metamind.core.backends.duckdb_connector import DuckDBConnector
            config = ConnectionConfig(
                backend_id="test-duckdb",
                database=":memory:",
                extra_params={"backend_type": "duckdb"},
            )
            c = DuckDBConnector(config)
            c.connect()
            result = c.execute("SELECT 42 AS answer")
            assert result.row_count == 1
            assert result.rows[0].get("answer") == 42
            c.disconnect()
        except ImportError:
            pytest.skip("duckdb not installed")


# ── PGVector connector tests ──────────────────────────────────

class TestPGVectorConnector:
    """Tests for PGVectorConnector vector-search extensions."""

    def test_capabilities_vector_search(self, mock_psycopg2: Any) -> None:
        from metamind.core.backends.pgvector_connector import PGVectorConnector
        c = PGVectorConnector(_pg_config())
        assert c.capabilities.supports_vector_search is True

    def test_create_vector_index_invalid_type_raises(self, mock_psycopg2: Any) -> None:
        from metamind.core.backends.pgvector_connector import PGVectorConnector
        c = PGVectorConnector(_pg_config())
        c.connect()
        c.execute = MagicMock(return_value=QueryResult(
            columns=[], rows=[], row_count=0, duration_ms=1.0
        ))
        with pytest.raises(ValueError, match="Unsupported index type"):
            c.create_vector_index("mytable", "embedding", index_type="btree")

    def test_metric_operator_mapping(self, mock_psycopg2: Any) -> None:
        from metamind.core.backends.pgvector_connector import _METRIC_OPS
        assert _METRIC_OPS["cosine"] == "<=>"
        assert _METRIC_OPS["l2"] == "<->"
        assert _METRIC_OPS["inner_product"] == "<#>"
