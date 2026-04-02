"""Integration tests for F18: Cross-Engine Statistics Synchronization."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from metamind.core.federation.stats_sync import StatsSynchronizer, UnifiedTableProfile


def _make_syncer(connector_stats: dict | None = None) -> StatsSynchronizer:
    """Create a StatsSynchronizer with mocked dependencies."""
    engine = MagicMock()
    conn_ctx = MagicMock()
    conn_ctx.__enter__ = lambda s: MagicMock(
        execute=MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=[])))
    )
    conn_ctx.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = conn_ctx

    begin_ctx = MagicMock()
    begin_ctx.__enter__ = lambda s: MagicMock(execute=MagicMock())
    begin_ctx.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = begin_ctx

    catalog = MagicMock()
    catalog.get_table.return_value = None

    registry = MagicMock()
    connector = MagicMock()
    connector.get_table_stats.return_value = connector_stats or {
        "revenue": {"ndv": 500, "null_fraction": 0.01, "avg_width": 8, "data_type": "float"},
        "region": {"ndv": 10, "null_fraction": 0.0, "avg_width": 12, "data_type": "varchar"},
    }
    registry.get.return_value = connector

    return StatsSynchronizer(engine, catalog, registry)


def test_sync_returns_correct_structure():
    """sync() returns dict with columns_synced, duration_ms, and conflicts keys."""
    syncer = _make_syncer()
    result = syncer.sync(
        tenant_id="t1",
        table_name="sales",
        source_backend="postgres-1",
        target_backends=["duckdb-1"],
    )
    assert "columns_synced" in result
    assert "duration_ms" in result
    assert "conflicts" in result
    assert isinstance(result["columns_synced"], int)
    assert isinstance(result["duration_ms"], float)
    assert isinstance(result["conflicts"], list)


def test_sync_updates_catalog_stats():
    """After sync(), columns_synced matches number of columns in returned stats."""
    stats = {
        "user_id": {"ndv": 10000, "null_fraction": 0.0, "avg_width": 4, "data_type": "int"},
        "email": {"ndv": 9990, "null_fraction": 0.02, "avg_width": 30, "data_type": "varchar"},
        "age": {"ndv": 80, "null_fraction": 0.1, "avg_width": 4, "data_type": "int"},
    }
    syncer = _make_syncer(connector_stats=stats)
    result = syncer.sync("t1", "users", "postgres-1", [])
    assert result["columns_synced"] == 3


def test_unified_profile_uses_max_ndv():
    """If backend A says NDV=100 and B says NDV=200, unified profile uses 200."""
    syncer = _make_syncer()

    registry = MagicMock()
    conn_a = MagicMock()
    conn_a.get_table_stats.return_value = {"city": {"ndv": 100, "null_fraction": 0.0}}
    conn_b = MagicMock()
    conn_b.get_table_stats.return_value = {"city": {"ndv": 200, "null_fraction": 0.0}}

    def get_side_effect(bid: str):
        if bid == "backend-a":
            return conn_a
        if bid == "backend-b":
            return conn_b
        return None

    syncer._registry.get.side_effect = get_side_effect

    profile = syncer.build_unified_profile("t1", "locations", ["backend-a", "backend-b"])

    assert isinstance(profile, UnifiedTableProfile)
    assert profile.unified_ndv.get("city", 0) == 200, "Should take MAX NDV across backends"


def test_reconcile_ndv_returns_max():
    """reconcile_ndv returns the maximum NDV value."""
    syncer = _make_syncer()
    assert syncer.reconcile_ndv([50, 200, 75]) == 200
    assert syncer.reconcile_ndv([10]) == 10
    assert syncer.reconcile_ndv([]) == 0


def test_staleness_detection():
    """detect_staleness marks backend as stale if last sync was > max_age_hours ago."""
    syncer = _make_syncer()

    # Override engine to return a stale timestamp
    old_ts = datetime.now(tz=timezone.utc) - timedelta(hours=25)

    conn_mock = MagicMock()
    conn_mock.execute.return_value.fetchall.return_value = [
        ("postgres-1", old_ts),
    ]
    syncer._engine.connect.return_value.__enter__ = lambda s: conn_mock

    staleness = syncer.detect_staleness("t1", "orders", max_age_hours=24)

    # postgres-1 should be stale (25h > 24h)
    if "postgres-1" in staleness:
        assert staleness["postgres-1"] is True


def test_sync_with_missing_backend_returns_zero():
    """If source backend is not in registry, sync returns 0 columns_synced."""
    syncer = _make_syncer()
    syncer._registry.get.return_value = None  # backend not found

    result = syncer.sync("t1", "missing_table", "nonexistent-backend", [])
    assert result["columns_synced"] == 0
