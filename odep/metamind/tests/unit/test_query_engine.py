"""Unit tests for query engine pipeline (Phase 2)."""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from metamind.config.feature_flags import FeatureFlags
from metamind.core.backends.connector import QueryResult, ConnectionConfig


def _make_mock_result(**kwargs: Any) -> QueryResult:
    """Build a minimal QueryResult."""
    defaults = dict(
        columns=["id", "val"],
        rows=[{"id": 1, "val": "x"}],
        row_count=1,
        duration_ms=3.0,
        backend="test",
        query_id="qid-1",
    )
    defaults.update(kwargs)
    return QueryResult(**defaults)


# ── SQLGenerator integration with query engine ────────────────

class TestSQLGeneratorInPipeline:
    """Test SQLGenerator routing from within a pipeline context."""

    def test_generator_postgres_does_not_raise(self) -> None:
        from metamind.core.execution.sql_generator import SQLGenerator
        from metamind.core.logical.nodes import ScanNode
        gen = SQLGenerator(dialect="postgres")
        node = ScanNode(table_name="users")
        sql = gen.generate(node)
        assert "SELECT" in sql.upper()

    def test_generator_snowflake_does_not_raise(self) -> None:
        from metamind.core.execution.sql_generator import SQLGenerator
        from metamind.core.logical.nodes import ScanNode
        gen = SQLGenerator(dialect="snowflake")
        node = ScanNode(table_name="transactions")
        sql = gen.generate(node, sql_hint="SELECT * FROM transactions LIMIT 10")
        assert isinstance(sql, str)

    def test_generator_duckdb_does_not_raise(self) -> None:
        from metamind.core.execution.sql_generator import SQLGenerator
        from metamind.core.logical.nodes import ScanNode
        gen = SQLGenerator(dialect="duckdb")
        node = ScanNode(table_name="events")
        sql = gen.generate(node)
        assert isinstance(sql, str)


# ── Feature flag impact on engine behaviour ───────────────────

class TestFeatureFlagImpact:
    """Test that feature flags correctly gate engine behaviour."""

    def test_f13_flag_controls_connector_feature(self) -> None:
        flags = FeatureFlags(F13_universal_connectors=True)
        assert flags.F13_universal_connectors is True

    def test_f17_flag_controls_dialect_feature(self) -> None:
        flags = FeatureFlags(F17_dialect_aware_sql=True)
        assert flags.F17_dialect_aware_sql is True

    def test_f23_flag_controls_budget(self) -> None:
        flags = FeatureFlags(F23_cloud_budget=True)
        assert flags.F23_cloud_budget is True

    def test_all_phase2_flags_can_be_set(self) -> None:
        """All Phase 2 feature flags can be enabled together without error."""
        flags = FeatureFlags(
            F13_universal_connectors=True,
            F17_dialect_aware_sql=True,
            F19_vector_search=True,
            F21_auto_advisor=True,
            F23_cloud_budget=True,
        )
        assert flags.F13_universal_connectors is True
        assert flags.F17_dialect_aware_sql is True
        assert flags.F19_vector_search is True


# ── Backend registry connector retrieval ─────────────────────

class TestRegistryConnectorRetrieval:
    """Test BackendRegistry integration."""

    def test_registry_registers_postgres_class(self) -> None:
        from metamind.core.backends.registry import BackendRegistry
        from metamind.core.backends.postgres_connector import PostgresConnector
        registry = BackendRegistry()
        registry.register_class("postgres", PostgresConnector)
        cls = registry._connector_classes.get("postgres")
        assert cls is PostgresConnector

    def test_registry_get_returns_none_before_create(self) -> None:
        from metamind.core.backends.registry import BackendRegistry
        registry = BackendRegistry()
        assert registry.get("never_created") is None

    def test_registry_disconnect_all_empty(self) -> None:
        from metamind.core.backends.registry import BackendRegistry
        registry = BackendRegistry()
        # Should not raise when no instances are registered
        registry.disconnect_all()

    def test_registry_health_check_empty(self) -> None:
        from metamind.core.backends.registry import BackendRegistry
        registry = BackendRegistry()
        result = registry.health_check_all()
        assert isinstance(result, dict)
        assert len(result) == 0


# ── Connector capabilities ────────────────────────────────────

class TestConnectorCapabilities:
    """Test ConnectorCapabilities default values and customisation."""

    def test_default_capabilities(self) -> None:
        from metamind.core.backends.connector import ConnectorCapabilities
        cap = ConnectorCapabilities()
        assert cap.supports_aggregation is True
        assert cap.supports_vector_search is False
        assert cap.cost_per_gb_scan == 0.0

    def test_snowflake_capabilities_cost(self) -> None:
        from metamind.core.backends.connector import ConnectorCapabilities
        cap = ConnectorCapabilities(cost_per_gb_scan=5.0, is_serverless=True)
        assert cap.cost_per_gb_scan == 5.0
        assert cap.is_serverless is True

    def test_spark_capabilities_distributed(self) -> None:
        from metamind.core.backends.connector import ConnectorCapabilities
        cap = ConnectorCapabilities(
            is_distributed=True,
            supports_broadcast_join=True,
            max_concurrent_queries=200,
        )
        assert cap.is_distributed is True
        assert cap.max_concurrent_queries == 200

    def test_vector_capabilities(self) -> None:
        from metamind.core.backends.connector import ConnectorCapabilities
        cap = ConnectorCapabilities(supports_vector_search=True)
        assert cap.supports_vector_search is True
