"""Integration tests for F15: Federated Materialized Views."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from metamind.core.mv.federated import FederatedMVDefinition, FederatedMVManager
from metamind.core.mv.matcher import MVMatcher
from metamind.core.logical.nodes import ScanNode, JoinNode, JoinType, FilterNode, Predicate
from metamind.core.metadata.models import BackendType, MaterializedViewMeta


def _make_mv_manager() -> FederatedMVManager:
    engine = MagicMock()
    # Make context manager work
    engine.connect.return_value.__enter__ = lambda s: MagicMock(
        execute=MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=[])))
    )
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = lambda s: MagicMock(execute=MagicMock())
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    registry = MagicMock()
    return FederatedMVManager(engine, registry, None)


def test_create_federated_mv_stores_definition():
    """create() returns a fully populated FederatedMVDefinition."""
    manager = _make_mv_manager()
    mv = manager.create(
        tenant_id="t1",
        name="daily_orders",
        source_sql="SELECT * FROM orders o JOIN products p ON o.product_id = p.id",
        source_backends={"orders": "postgres-1", "products": "duckdb-1"},
        target_backend="duckdb-1",
        refresh_policy="scheduled",
        refresh_interval_minutes=120,
    )

    assert isinstance(mv, FederatedMVDefinition)
    assert mv.mv_name == "daily_orders"
    assert mv.tenant_id == "t1"
    assert mv.source_backends == {"orders": "postgres-1", "products": "duckdb-1"}
    assert mv.target_backend == "duckdb-1"
    assert mv.refresh_policy == "scheduled"
    assert mv.refresh_interval_minutes == 120
    assert mv.last_refreshed is None
    assert mv.row_count == 0


def test_refresh_federated_mv_returns_stats():
    """refresh() returns dict with rows_written, duration_ms, bytes_transferred."""
    manager = _make_mv_manager()

    # Mock load_mv to return a definition
    mv_def = FederatedMVDefinition(
        mv_name="sales_mv", tenant_id="t1",
        source_sql="SELECT * FROM sales",
        source_backends={"sales": "postgres-1"},
        target_backend="duckdb-1",
        refresh_policy="manual",
        refresh_interval_minutes=60,
        last_refreshed=None,
        estimated_staleness_seconds=float("inf"),
        row_count=0,
    )
    manager._load_mv = MagicMock(return_value=mv_def)

    # Mock connector
    connector = MagicMock()
    result = MagicMock()
    result.rows = [{"id": 1}, {"id": 2}]
    result.row_count = 2
    result.bytes_scanned = 512
    connector.execute.return_value = result
    manager._registry.get.return_value = connector

    refresh_result = manager.refresh("t1", "sales_mv", manager._registry)

    assert "rows_written" in refresh_result
    assert "duration_ms" in refresh_result
    assert "bytes_transferred" in refresh_result
    assert isinstance(refresh_result["duration_ms"], float)


def test_mv_staleness_detection():
    """MV older than refresh_interval * 1.5 is considered stale."""
    manager = _make_mv_manager()

    # MV refreshed 100 minutes ago with 60-minute interval → stale (100 > 60*1.5=90)
    stale_ts = datetime.now(tz=timezone.utc) - timedelta(minutes=100)
    mv_stale = FederatedMVDefinition(
        mv_name="test_mv", tenant_id="t1",
        source_sql="SELECT 1", source_backends={},
        target_backend="duckdb-1", refresh_policy="scheduled",
        refresh_interval_minutes=60, last_refreshed=stale_ts,
        estimated_staleness_seconds=0.0, row_count=0,
    )
    assert not manager.should_use_for_query(mv_stale, set())

    # MV refreshed 30 minutes ago with 60-minute interval → fresh
    fresh_ts = datetime.now(tz=timezone.utc) - timedelta(minutes=30)
    mv_fresh = FederatedMVDefinition(
        mv_name="test_mv2", tenant_id="t1",
        source_sql="SELECT 1", source_backends={"orders": "pg"},
        target_backend="duckdb-1", refresh_policy="scheduled",
        refresh_interval_minutes=60, last_refreshed=fresh_ts,
        estimated_staleness_seconds=0.0, row_count=0,
    )
    assert manager.should_use_for_query(mv_fresh, {"orders"})


def test_mv_staleness_infinity_for_never_refreshed():
    """An MV that was never refreshed has infinite staleness."""
    manager = _make_mv_manager()
    mv = FederatedMVDefinition(
        mv_name="new_mv", tenant_id="t1",
        source_sql="SELECT 1", source_backends={},
        target_backend="duckdb-1", refresh_policy="manual",
        refresh_interval_minutes=60, last_refreshed=None,
        estimated_staleness_seconds=0.0, row_count=0,
    )
    assert manager.get_staleness_seconds(mv) == float("inf")


def test_mv_matcher_finds_covering_mv():
    """Query against subset of MV's base tables → MVMatcher returns a match."""
    matcher = MVMatcher()

    mv = MaterializedViewMeta(
        mv_id=1, tenant_id="t1", mv_name="orders_products_mv",
        schema_name="public", backend=BackendType.DUCKDB,
        query_text="SELECT * FROM orders o JOIN products p ON o.pid = p.id",
        query_fingerprint="abc123",
        base_tables=["orders", "products"],
        row_count=5000, size_bytes=1024000,
        refresh_type="manual", is_active=True, benefit_score=0.8,
    )

    left = ScanNode(table_name="orders")
    right = ScanNode(table_name="products")
    join = JoinNode(join_type=JoinType.INNER)
    join.children = [left, right]

    matches = matcher.find_matching_mvs(join, [mv], "t1")
    assert len(matches) > 0, "Expected at least one MV match"
    matched_mv, confidence = matches[0]
    assert matched_mv.mv_name == "orders_products_mv"
    assert 0.0 < confidence <= 1.0


def test_mv_matcher_no_match_different_tables():
    """Query on unrelated tables → no MV match."""
    matcher = MVMatcher()

    mv = MaterializedViewMeta(
        mv_id=2, tenant_id="t1", mv_name="customers_mv",
        schema_name="public", backend=BackendType.POSTGRES,
        query_text="SELECT * FROM customers",
        query_fingerprint="def456",
        base_tables=["customers"],
        row_count=1000, size_bytes=50000,
        refresh_type="manual", is_active=True, benefit_score=0.5,
    )

    # Query uses orders, not customers
    scan = ScanNode(table_name="orders")
    matches = matcher.find_matching_mvs(scan, [mv], "t1")
    assert len(matches) == 0
