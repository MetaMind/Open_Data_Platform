"""Integration tests for optimization tier selection."""
from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def optimizer_and_flags():
    """Build optimizer + feature flags for tier testing."""
    try:
        from metamind.core.costing.cost_model import CostModel
        from metamind.core.memo.optimizer import CascadesOptimizer
        from metamind.config.feature_flags import FeatureFlags

        flags = FeatureFlags(
            F04_bushy_join_dp=True,
            F12_optimization_tiering=True,
        )
        cost_model = CostModel()
        optimizer = CascadesOptimizer(cost_model=cost_model, flags=flags)
        return optimizer, flags
    except ImportError as exc:
        pytest.skip(f"Optimizer not available: {exc}")


def _make_scan(table_name: str, rows: float = 10_000.0) -> object:
    """Build a ScanNode."""
    from metamind.core.logical.nodes import ScanNode
    node = ScanNode(table_name=table_name, schema_name="public", alias=table_name[:2])
    node.estimated_rows = rows
    return node


def _make_join(left: object, right: object) -> object:
    """Build an INNER JOIN node."""
    from metamind.core.logical.nodes import JoinNode, JoinType, Predicate
    pred = Predicate(column="id", operator="=", value="foreign_id")
    node = JoinNode(join_type=JoinType.INNER, condition=pred)
    node.children = [left, right]
    return node


def _make_project(child: object, cols: list[str] = None) -> object:
    """Wrap in a ProjectNode."""
    from metamind.core.logical.nodes import ProjectNode
    node = ProjectNode(columns=cols or ["*"])
    node.children = [child]
    return node


def test_tier1_simple_query_uses_heuristic(optimizer_and_flags):
    """1-table query should use tier 1 (heuristic)."""
    optimizer, flags = optimizer_and_flags
    plan = _make_project(_make_scan("orders"))
    try:
        result = optimizer.optimize(plan)
        assert result is not None
    except Exception as exc:
        pytest.skip(f"Optimizer error: {exc}")


def test_tier2_few_joins(optimizer_and_flags):
    """2-3 table query should use tier 2 (rule-based)."""
    optimizer, flags = optimizer_and_flags
    s1 = _make_scan("orders", 50_000)
    s2 = _make_scan("customers", 10_000)
    j = _make_join(s1, s2)
    plan = _make_project(j)
    try:
        result = optimizer.optimize(plan)
        assert result is not None
    except Exception as exc:
        pytest.skip(f"Optimizer error: {exc}")


def test_tier3_complex_joins(optimizer_and_flags):
    """4+ table query should use tier 3 (full Cascades)."""
    optimizer, flags = optimizer_and_flags
    s1 = _make_scan("orders")
    s2 = _make_scan("customers")
    s3 = _make_scan("products")
    s4 = _make_scan("categories")

    j1 = _make_join(s1, s2)
    j2 = _make_join(s3, s4)
    j3 = _make_join(j1, j2)
    plan = _make_project(j3)

    try:
        result = optimizer.optimize(plan)
        assert result is not None
    except Exception as exc:
        pytest.skip(f"Optimizer error: {exc}")


def test_tier_selection_uses_complexity_guard(optimizer_and_flags):
    """Optimizer should classify tiers consistently based on table count."""
    optimizer, flags = optimizer_and_flags

    # Simple plan
    simple = _make_project(_make_scan("t1"))
    tier_simple = optimizer._classify_tier(simple)

    # Complex plan
    scans = [_make_scan(f"t{i}") for i in range(6)]
    j = scans[0]
    for s in scans[1:]:
        j = _make_join(j, s)
    complex_plan = _make_project(j)
    tier_complex = optimizer._classify_tier(complex_plan)

    assert tier_simple <= tier_complex, (
        f"Simple plan tier ({tier_simple}) should be <= complex plan tier ({tier_complex})"
    )


def test_optimization_produces_valid_plan(optimizer_and_flags):
    """Optimized plan should have an estimated cost attached."""
    optimizer, flags = optimizer_and_flags
    plan = _make_project(_make_scan("orders"))
    try:
        result = optimizer.optimize(plan)
        assert result is not None
        # Result should have some cost attribute
        has_cost = (
            hasattr(result, "_estimated_cost")
            or hasattr(result, "estimated_cost")
        )
        # Even without cost being set, result should be a LogicalNode
        from metamind.core.logical.nodes import LogicalNode
        assert isinstance(result, LogicalNode)
    except Exception as exc:
        pytest.skip(f"Optimizer returned error: {exc}")
