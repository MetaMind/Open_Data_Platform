"""Integration tests for F16: Data Placement Advisor."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from metamind.core.federation.placement_advisor import (
    DataPlacementAdvisor,
    PlacementRecommendation,
)
from metamind.core.costing.cost_model import CostModel


def _make_advisor() -> DataPlacementAdvisor:
    engine = MagicMock()
    engine.begin.return_value.__enter__ = lambda s: MagicMock(execute=MagicMock())
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    catalog = MagicMock()
    catalog.get_table.return_value = None
    return DataPlacementAdvisor(engine, catalog, CostModel())


def test_colocate_frequently_joined_tables():
    """Tables joined in >50% of queries on different backends → recommendation generated."""
    advisor = _make_advisor()

    # 10 queries, all join orders+products — currently on different backends
    query_history = [
        {"sql": "SELECT ...", "backend_used": "postgres-1", "tables": ["orders", "products"]}
        for _ in range(10)
    ]
    table_locations = {"orders": "postgres-1", "products": "duckdb-1"}
    available_backends = ["postgres-1", "duckdb-1"]

    recs = advisor.analyze("t1", query_history, table_locations, available_backends)

    assert len(recs) > 0, "Expected at least one placement recommendation"
    rec = recs[0]
    assert rec.table in {"orders", "products"}
    assert rec.recommended_backend in {"postgres-1", "duckdb-1"}
    assert rec.affected_query_count == 10
    assert rec.confidence > 0.0


def test_no_recommendation_for_same_backend():
    """Tables already on same backend → no placement recommendation."""
    advisor = _make_advisor()

    query_history = [
        {"sql": "SELECT ...", "backend_used": "postgres-1", "tables": ["orders", "line_items"]}
        for _ in range(10)
    ]
    # Both on postgres-1 already
    table_locations = {"orders": "postgres-1", "line_items": "postgres-1"}
    available_backends = ["postgres-1"]

    recs = advisor.analyze("t1", query_history, table_locations, available_backends)
    assert recs == [], "No recommendations when tables already co-located"


def test_recommendation_includes_cost_estimate():
    """PlacementRecommendation must have non-zero estimated_cost_savings."""
    advisor = _make_advisor()

    query_history = [
        {"sql": "JOIN", "backend_used": "postgres-1", "tables": ["fact_sales", "dim_product"]}
        for _ in range(20)
    ]
    table_locations = {"fact_sales": "postgres-1", "dim_product": "duckdb-1"}
    available_backends = ["postgres-1", "duckdb-1"]

    recs = advisor.analyze("t1", query_history, table_locations, available_backends)
    assert len(recs) > 0
    rec = recs[0]
    assert rec.estimated_cost_savings_monthly_usd > 0.0
    assert rec.estimated_query_speedup_x >= 2.0
    assert rec.migration_difficulty in {"trivial", "moderate", "complex"}


def test_colocation_matrix_counts_pairs():
    """_build_colocation_matrix should count (A, B) joint occurrences correctly."""
    advisor = _make_advisor()

    history = [
        {"tables": ["orders", "products", "customers"]},
        {"tables": ["orders", "products"]},
        {"tables": ["customers", "orders"]},
    ]
    matrix = advisor._build_colocation_matrix(history)

    orders_products = matrix.get(("orders", "products"), 0)
    orders_customers = matrix.get(("customers", "orders"), 0)
    assert orders_products == 2
    assert orders_customers == 2


def test_score_recommendation_uses_all_factors():
    """Score should increase with savings, confidence, and speedup."""
    advisor = _make_advisor()

    rec_low = PlacementRecommendation(
        table="t", current_backend="a", recommended_backend="b",
        reason="", estimated_query_speedup_x=1.5,
        estimated_cost_savings_monthly_usd=5.0,
        migration_difficulty="trivial", affected_query_count=2, confidence=0.2,
    )
    rec_high = PlacementRecommendation(
        table="t", current_backend="a", recommended_backend="b",
        reason="", estimated_query_speedup_x=4.0,
        estimated_cost_savings_monthly_usd=100.0,
        migration_difficulty="trivial", affected_query_count=50, confidence=0.9,
    )
    assert advisor._score_recommendation(rec_high) > advisor._score_recommendation(rec_low)
