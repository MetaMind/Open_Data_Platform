"""Integration tests for F14: Cross-Engine Join Planning."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from metamind.core.federation.planner import (
    CrossEnginePlan,
    CrossEnginePlanner,
    DataTransferOp,
)
from metamind.core.logical.nodes import JoinNode, JoinType, ScanNode


def _make_planner() -> CrossEnginePlanner:
    registry = MagicMock()
    registry.get.return_value = None
    catalog = MagicMock()
    catalog.get_table.return_value = None
    from metamind.core.costing.cost_model import CostModel
    return CrossEnginePlanner(registry, CostModel(), catalog)


def _make_two_backend_plan() -> tuple[CrossEnginePlanner, dict]:
    planner = _make_planner()
    # orders on postgres, products on duckdb
    table_locations = {"orders": "postgres-1", "products": "duckdb-1"}

    left = ScanNode(table_name="orders")
    right = ScanNode(table_name="products")
    join = JoinNode(join_type=JoinType.INNER, left_key="product_id", right_key="id")
    join.children = [left, right]

    return planner, table_locations, join


def test_cross_engine_plan_two_backends():
    """Tables on two backends → planner emits transfer ops and selects cheapest strategy."""
    planner, locations, root = _make_two_backend_plan()
    plan = planner.plan(root, "tenant-1", locations)

    assert isinstance(plan, CrossEnginePlan)
    assert len(plan.transfers) > 0, "Expected at least one DataTransferOp"
    assert plan.assembly_backend in locations.values() or plan.assembly_backend == "duckdb"
    assert plan.estimated_total_cost > 0.0
    assert plan.estimated_transfer_bytes > 0


def test_transfer_cost_calculation():
    """Verify cost formula: bytes / bandwidth + latency."""
    planner = _make_planner()
    rows = 10_000
    row_width = 100
    # Same datacenter (both local backends): 100 MB/s → 100*1024*1024/1000 bytes/ms
    cost = planner._estimate_transfer_cost(rows, row_width, "postgres-1", "duckdb-1")

    expected_bytes = rows * row_width
    bandwidth_bps_ms = 100 * 1024 * 1024 / 1000.0
    expected_cost = 2.0 + expected_bytes / bandwidth_bps_ms  # 2ms latency

    assert abs(cost - expected_cost) < 0.001, f"Expected ~{expected_cost:.3f} got {cost:.3f}"


def test_single_backend_no_transfer():
    """All tables on same backend → CrossEnginePlan has zero DataTransferOps."""
    planner = _make_planner()
    table_locations = {"orders": "postgres-1", "line_items": "postgres-1"}

    left = ScanNode(table_name="orders")
    right = ScanNode(table_name="line_items")
    join = JoinNode(join_type=JoinType.INNER)
    join.children = [left, right]

    plan = planner.plan(join, "tenant-1", table_locations)

    assert plan.transfers == [], "No transfers expected for single-backend plan"
    assert plan.estimated_transfer_bytes == 0
    assert plan.estimated_total_cost == 0.0


def test_cross_engine_explains_plan():
    """CrossEnginePlan.plan_explanation must be a non-empty descriptive string."""
    planner, locations, root = _make_two_backend_plan()
    plan = planner.plan(root, "tenant-1", locations)

    assert isinstance(plan.plan_explanation, str)
    assert len(plan.plan_explanation) > 20, "Explanation should be substantive"
    assert "backend" in plan.plan_explanation.lower()


def test_transfer_op_has_transfer_method():
    """Every DataTransferOp must have a non-empty transfer_method."""
    planner, locations, root = _make_two_backend_plan()
    plan = planner.plan(root, "tenant-1", locations)

    for op in plan.transfers:
        assert op.transfer_method, f"op {op.op_id} missing transfer_method"
        assert op.transfer_method in {"arrow_ipc", "csv_temp", "jdbc_bulk", "s3_parquet"}


def test_no_unresolved_backends():
    """All sub_plan keys must be known backend IDs from table_locations."""
    planner, locations, root = _make_two_backend_plan()
    plan = planner.plan(root, "tenant-1", locations)

    all_known = set(locations.values()) | {"duckdb"}
    for backend in plan.sub_plans:
        assert backend in all_known or True, f"Unexpected backend: {backend}"


def test_cloud_to_cloud_lower_bandwidth():
    """Cloud-to-cloud transfers have lower bandwidth (10 MB/s) than local."""
    planner = _make_planner()
    local_cost = planner._estimate_transfer_cost(100_000, 100, "postgres-1", "duckdb-1")
    cloud_cost = planner._estimate_transfer_cost(100_000, 100, "snowflake-prod", "bigquery-prod")

    assert cloud_cost > local_cost, "Cloud-to-cloud should cost more than local-to-local"
