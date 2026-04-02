"""Core unit tests for MetaMind."""
from __future__ import annotations
import pytest
from metamind.config.feature_flags import FeatureFlags
from metamind.config.settings import AppSettings
from metamind.core.logical.builder import LogicalPlanBuilder
from metamind.core.logical.nodes import ScanNode, FilterNode, JoinNode, AggregateNode
from metamind.core.costing.histograms import HistogramEstimator
from metamind.core.costing.cost_model import CostModel, CostVector, CostWeights
from metamind.core.logical.nodes import Predicate
from metamind.core.metadata.models import ColumnMeta, DataType, TableMeta, BackendType
from metamind.core.cache.plan_cache import PlanCache, QueryFingerprint
from metamind.core.workload.classifier import WorkloadClassifier, WorkloadType
from metamind.core.memo.dpccp import DPccp, JoinRelation, JoinEdge

# ── Feature Flags ──────────────────────────────────────────────
def test_feature_flags_defaults():
    flags = FeatureFlags()
    assert flags.F01_learned_cardinality is False
    assert flags.F09_plan_caching is False
    assert flags.F30_optimization_replay is False

def test_feature_flags_to_dict():
    flags = FeatureFlags(F01_learned_cardinality=True)
    d = flags.to_dict()
    assert d["F01_learned_cardinality"] is True
    assert d["F09_plan_caching"] is False

def test_feature_flags_from_dict():
    flags = FeatureFlags.from_dict({"F01_learned_cardinality": True})
    assert flags.F01_learned_cardinality is True
    assert flags.F09_plan_caching is False

def test_feature_flags_all_enabled():
    flags = FeatureFlags.all_enabled()
    assert flags.F01_learned_cardinality is True
    assert flags.F30_optimization_replay is True

def test_feature_flags_phase1():
    flags = FeatureFlags.phase1()
    assert flags.F12_optimization_tiering is True
    assert flags.F09_plan_caching is True
    assert flags.F28_nl_interface is False

# ── Logical Builder ────────────────────────────────────────────
def test_build_simple_select():
    builder = LogicalPlanBuilder()
    root = builder.build("SELECT * FROM orders")
    assert root is not None

def test_build_select_with_where():
    builder = LogicalPlanBuilder()
    root = builder.build("SELECT id FROM users WHERE status = 'active' LIMIT 10")
    assert root is not None

def test_build_join():
    builder = LogicalPlanBuilder()
    root = builder.build("""
        SELECT o.id, c.name 
        FROM orders o 
        JOIN customers c ON o.customer_id = c.id
    """)
    assert root is not None

def test_build_aggregate():
    builder = LogicalPlanBuilder()
    root = builder.build("""
        SELECT customer_id, COUNT(*) as cnt, SUM(total) as rev
        FROM orders
        GROUP BY customer_id
    """)
    assert root is not None

# ── Histograms ────────────────────────────────────────────────
def test_histogram_eq_selectivity_with_mcv():
    estimator = HistogramEstimator()
    col = ColumnMeta(column_name="status", data_type=DataType.VARCHAR, ordinal_pos=0,
                     ndv=3, most_common_vals=["active","pending","done"],
                     most_common_freqs=[0.5, 0.3, 0.2])
    pred = Predicate(column="status", operator="=", value="active")
    sel = estimator.estimate_selectivity(pred, col)
    assert abs(sel - 0.5) < 0.01

def test_histogram_no_stats():
    estimator = HistogramEstimator()
    pred = Predicate(column="x", operator="=", value="test")
    sel = estimator.estimate_selectivity(pred, None)
    assert 0 < sel < 1

def test_histogram_range_with_bounds():
    estimator = HistogramEstimator()
    col = ColumnMeta(column_name="amount", data_type=DataType.FLOAT, ordinal_pos=0,
                     ndv=1000, histogram_bounds=["0","100","200","300","400","500"])
    pred = Predicate(column="amount", operator="<", value="250")
    sel = estimator.estimate_selectivity(pred, col)
    assert 0 < sel < 1

def test_histogram_cardinality():
    estimator = HistogramEstimator()
    col = ColumnMeta(column_name="id", data_type=DataType.INT, ordinal_pos=0, ndv=1000)
    pred = Predicate(column="id", operator="=", value="42")
    card = estimator.estimate_cardinality(10000, [pred], {"id": col})
    assert card >= 1.0

# ── Cost Model ────────────────────────────────────────────────
def test_scan_cost():
    model = CostModel()
    cv = model.scan_cost(1_000_000, 100_000_000, 0.01, "postgres")
    assert cv.latency_ms > 0
    assert cv.io_pages > 0

def test_join_hash_cost():
    model = CostModel()
    cv = model.join_cost(10000, 5000, "hash")
    assert cv.latency_ms > 0
    assert cv.memory_mb > 0

def test_cost_vector_add():
    cv1 = CostVector(latency_ms=10, cloud_cost_usd=0.01)
    cv2 = CostVector(latency_ms=20, cloud_cost_usd=0.02)
    total = cv1 + cv2
    assert total.latency_ms == 30
    assert abs(total.cloud_cost_usd - 0.03) < 1e-10

def test_multi_objective_weights():
    model = CostModel()
    weights = CostWeights(latency=1.0, cloud_cost=0.5)
    model.set_weights(weights)
    cv = CostVector(latency_ms=100, cloud_cost_usd=5.0)
    scalar = model.scalar_cost(cv)
    assert scalar > 0

# ── Plan Cache ────────────────────────────────────────────────
def test_plan_cache_miss():
    cache = PlanCache(redis_client=None)
    result = cache.get("SELECT 1", "tenant1")
    assert result is None

def test_plan_cache_put_get():
    cache = PlanCache(redis_client=None)
    fp = cache.put("SELECT * FROM orders", "t1", '{"type":"scan"}', "postgres", 10.0)
    assert fp is not None
    entry = cache.get("SELECT * FROM orders", "t1")
    assert entry is not None
    assert entry.backend == "postgres"
    assert entry.hit_count == 1

def test_plan_cache_different_tenants():
    cache = PlanCache(redis_client=None)
    cache.put("SELECT 1", "tenant1", "{}", "pg", 1.0)
    assert cache.get("SELECT 1", "tenant2") is None
    assert cache.get("SELECT 1", "tenant1") is not None

def test_query_fingerprint_normalization():
    fp = QueryFingerprint()
    sql1 = "SELECT * FROM orders WHERE id = 42"
    sql2 = "SELECT * FROM orders WHERE id = 99"
    assert fp.compute(sql1, "t1") == fp.compute(sql2, "t1")

def test_plan_cache_invalidation():
    cache = PlanCache(redis_client=None)
    cache.put("SELECT * FROM orders", "t1", "{}", "pg", 1.0)
    count = cache.invalidate("t1")
    assert count >= 1

# ── Workload Classifier ───────────────────────────────────────
def test_workload_classify_point_lookup():
    classifier = WorkloadClassifier()
    builder = LogicalPlanBuilder()
    root = builder.build("SELECT * FROM users WHERE id = 1 LIMIT 1")
    wtype = classifier.classify(root, "SELECT * FROM users WHERE id = 1 LIMIT 1")
    assert wtype == WorkloadType.POINT_LOOKUP

def test_workload_classify_dashboard():
    classifier = WorkloadClassifier()
    builder = LogicalPlanBuilder()
    root = builder.build("SELECT region, COUNT(*) FROM orders GROUP BY region")
    wtype = classifier.classify(root, "SELECT region, COUNT(*) FROM orders GROUP BY region")
    assert wtype == WorkloadType.DASHBOARD_AGGREGATE

# ── DPccp Join Enumeration ────────────────────────────────────
def test_dpccp_two_tables():
    relations = [
        JoinRelation(0, "orders", 100000),
        JoinRelation(1, "customers", 5000),
    ]
    edges = [JoinEdge(0, 1, selectivity=0.01)]
    dp = DPccp(relations, edges)
    result = dp.enumerate()
    assert result is not None
    assert result.cost > 0

def test_dpccp_three_tables():
    relations = [
        JoinRelation(0, "orders", 500000),
        JoinRelation(1, "customers", 10000),
        JoinRelation(2, "products", 1000),
    ]
    edges = [JoinEdge(0, 1, 0.01), JoinEdge(0, 2, 0.01)]
    dp = DPccp(relations, edges)
    result = dp.enumerate()
    assert result is not None

def test_dpccp_too_many_tables():
    with pytest.raises(ValueError):
        relations = [JoinRelation(i, f"t{i}", 1000) for i in range(16)]
        DPccp(relations, [])
