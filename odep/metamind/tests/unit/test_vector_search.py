"""Unit tests for F19 Vector Similarity Search."""
from __future__ import annotations

import math
import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from metamind.core.catalog.metadata import MetadataCatalog
from metamind.core.execution.backends import BackendRegistry
from metamind.core.types import ColumnMeta, Predicate, TableMeta
from metamind.core.vector.batch import VectorBatchProcessor
from metamind.core.vector.operators import VectorAggregateOperator, VectorFilterOperator
from metamind.core.vector.search import (
    VectorCostModel,
    VectorIndex,
    VectorIndexManager,
    VectorSearchPlanner,
    VectorSearchRequest,
)


class TestCosineDistanceCalculation(unittest.TestCase):
    """Test vectorized cosine distance computation."""

    def setUp(self) -> None:
        self.processor = VectorBatchProcessor(batch_size=100)

    def test_cosine_identical_vectors_distance_zero(self) -> None:
        data = [{"id": 1, "emb": [1.0, 0.0]}]
        result = self.processor.compute_distances_batch(
            [1.0, 0.0], data, "emb", "cosine"
        )
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result[0]["__distance"], 0.0, places=5)

    def test_cosine_orthogonal_vectors_distance_one(self) -> None:
        data = [{"id": 1, "emb": [0.0, 1.0]}]
        result = self.processor.compute_distances_batch(
            [1.0, 0.0], data, "emb", "cosine"
        )
        self.assertAlmostEqual(result[0]["__distance"], 1.0, places=5)

    def test_cosine_opposite_vectors_distance_two(self) -> None:
        data = [{"id": 1, "emb": [-1.0, 0.0]}]
        result = self.processor.compute_distances_batch(
            [1.0, 0.0], data, "emb", "cosine"
        )
        self.assertAlmostEqual(result[0]["__distance"], 2.0, places=5)

    def test_l2_distance_calculation(self) -> None:
        data = [{"id": 1, "emb": [3.0, 4.0]}]
        result = self.processor.compute_distances_batch(
            [0.0, 0.0], data, "emb", "l2"
        )
        self.assertAlmostEqual(result[0]["__distance"], 5.0, places=5)

    def test_inner_product_distance(self) -> None:
        data = [{"id": 1, "emb": [2.0, 3.0]}]
        result = self.processor.compute_distances_batch(
            [1.0, 1.0], data, "emb", "inner_product"
        )
        # inner_product distance = -(A·B) = -(2+3) = -5
        self.assertAlmostEqual(result[0]["__distance"], -5.0, places=5)


class TestVectorSearchPlannerSelectsIndex(unittest.TestCase):
    """Test that the planner selects index-aware strategy when available."""

    def setUp(self) -> None:
        self.catalog = MetadataCatalog()
        self.catalog.register_table(
            "tenant1",
            TableMeta(
                table_name="documents",
                schema_name="public",
                tenant_id="tenant1",
                columns=[ColumnMeta(name="embedding", dtype="vector(768)")],
                row_count=1_000_000,
            ),
        )
        self.catalog.register_vector_index(
            "tenant1",
            "documents.embedding",
            {
                "index_name": "vi_docs_emb_hnsw",
                "index_type": "HNSW",
                "column": "embedding",
                "dimensions": 768,
                "distance_metric": "cosine",
                "build_params": {"m": 16, "ef_construction": 200},
            },
        )
        self.cost_model = VectorCostModel()
        self.registry = BackendRegistry()

    def test_planner_chooses_hnsw_strategy(self) -> None:
        planner = VectorSearchPlanner(self.catalog, self.cost_model, self.registry)
        request = VectorSearchRequest(
            table="documents",
            embedding_column="embedding",
            query_vector=[0.1] * 768,
            top_k=10,
            distance_metric="cosine",
            tenant_id="tenant1",
        )
        plan = planner.plan(request)
        self.assertEqual(plan["strategy"], "HNSW")
        self.assertEqual(plan["index_type"], "HNSW")

    def test_planner_falls_back_to_exact_scan(self) -> None:
        catalog = MetadataCatalog()
        catalog.register_table(
            "tenant1",
            TableMeta(
                table_name="small_table",
                schema_name="public",
                tenant_id="tenant1",
                row_count=100,
            ),
        )
        planner = VectorSearchPlanner(catalog, self.cost_model, self.registry)
        request = VectorSearchRequest(
            table="small_table",
            embedding_column="vec",
            query_vector=[0.1, 0.2, 0.3],
            top_k=5,
            distance_metric="cosine",
            tenant_id="tenant1",
        )
        plan = planner.plan(request)
        self.assertEqual(plan["strategy"], "exact_scan")


class TestVectorCostModelRecommendations(unittest.TestCase):
    """Test cost model index recommendations."""

    def setUp(self) -> None:
        self.cost_model = VectorCostModel()

    def test_recommends_hnsw_for_large_table(self) -> None:
        rec = self.cost_model.recommend_index(
            rows=50_000_000, dimensions=768, expected_qps=100.0
        )
        self.assertEqual(rec["index_type"], "HNSW")

    def test_recommends_exact_for_small_table(self) -> None:
        rec = self.cost_model.recommend_index(
            rows=50_000, dimensions=256, expected_qps=10.0
        )
        self.assertEqual(rec["index_type"], "exact")

    def test_recommends_ivfflat_for_medium_table(self) -> None:
        rec = self.cost_model.recommend_index(
            rows=1_000_000, dimensions=768, expected_qps=10.0
        )
        self.assertEqual(rec["index_type"], "IVFFlat")

    def test_hnsw_cost_less_than_exact_for_large(self) -> None:
        exact_cost = self.cost_model.estimate_ann_cost(
            10_000_000, 768, 10, index_type=None
        )
        hnsw_cost = self.cost_model.estimate_ann_cost(
            10_000_000, 768, 10, index_type="HNSW"
        )
        self.assertLess(hnsw_cost, exact_cost)


class TestPgvectorSQLGeneration(unittest.TestCase):
    """Test pgvector SQL generation."""

    def setUp(self) -> None:
        self.catalog = MetadataCatalog()
        self.registry = BackendRegistry()
        self.cost_model = VectorCostModel()
        self.planner = VectorSearchPlanner(self.catalog, self.cost_model, self.registry)

    def test_pgvector_cosine_operator(self) -> None:
        request = VectorSearchRequest(
            table="items",
            embedding_column="emb",
            query_vector=[0.1, 0.2],
            top_k=5,
            distance_metric="cosine",
            tenant_id="t1",
        )
        sql = self.planner._build_pgvector_sql(request, "cosine")
        self.assertIn("<=>", sql)
        self.assertIn("LIMIT 5", sql)

    def test_pgvector_l2_operator(self) -> None:
        request = VectorSearchRequest(
            table="items",
            embedding_column="emb",
            query_vector=[0.1, 0.2],
            top_k=5,
            distance_metric="l2",
            tenant_id="t1",
        )
        sql = self.planner._build_pgvector_sql(request, "l2")
        self.assertIn("<->", sql)

    def test_pgvector_inner_product_operator(self) -> None:
        request = VectorSearchRequest(
            table="items",
            embedding_column="emb",
            query_vector=[0.1, 0.2],
            top_k=5,
            distance_metric="inner_product",
            tenant_id="t1",
        )
        sql = self.planner._build_pgvector_sql(request, "inner_product")
        self.assertIn("<#>", sql)


class TestBatchProcessorTopK(unittest.TestCase):
    """Test batch processor top-k filtering."""

    def setUp(self) -> None:
        self.processor = VectorBatchProcessor()

    def test_top_k_returns_exact_count(self) -> None:
        data = [{"id": i, "emb": [float(i), 0.0]} for i in range(100)]
        with_distances = self.processor.compute_distances_batch(
            [0.0, 0.0], data, "emb", "l2"
        )
        top_5 = self.processor.top_k_filter(with_distances, 5)
        self.assertEqual(len(top_5), 5)

    def test_top_k_sorted_ascending(self) -> None:
        data = [
            {"id": 1, "emb": [10.0, 0.0]},
            {"id": 2, "emb": [1.0, 0.0]},
            {"id": 3, "emb": [5.0, 0.0]},
        ]
        with_distances = self.processor.compute_distances_batch(
            [0.0, 0.0], data, "emb", "l2"
        )
        top_2 = self.processor.top_k_filter(with_distances, 2)
        self.assertLessEqual(top_2[0]["__distance"], top_2[1]["__distance"])

    def test_empty_data_returns_empty(self) -> None:
        result = self.processor.compute_distances_batch(
            [1.0, 0.0], [], "emb", "cosine"
        )
        self.assertEqual(len(result), 0)


class TestVectorFilterOperator(unittest.TestCase):
    """Test pre-filter operator."""

    def test_equality_filter(self) -> None:
        rows = [
            {"id": 1, "status": "active", "emb": [1.0]},
            {"id": 2, "status": "inactive", "emb": [2.0]},
        ]
        op = VectorFilterOperator()
        result = op.apply(rows, [Predicate(column="status", operator="=", value="active")])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)


class TestVectorAggregateOperator(unittest.TestCase):
    """Test aggregate operations."""

    def test_centroid_computation(self) -> None:
        agg = VectorAggregateOperator()
        centroid = agg.compute_centroid([[1.0, 0.0], [0.0, 1.0]])
        self.assertAlmostEqual(centroid[0], 0.5)
        self.assertAlmostEqual(centroid[1], 0.5)


class TestVectorIndexManager(unittest.TestCase):
    """Test vector index management."""

    def test_create_and_list(self) -> None:
        catalog = MetadataCatalog()
        registry = BackendRegistry()
        mgr = VectorIndexManager(catalog, registry)
        idx = mgr.create_index(
            "t1", "docs", "embedding", "HNSW", "cosine",
            params={"dimensions": 128}
        )
        self.assertIn("vi_docs_embedding_hnsw", idx.index_name)
        indexes = mgr.list_indexes("t1")
        self.assertEqual(len(indexes), 1)

    def test_drop_index(self) -> None:
        catalog = MetadataCatalog()
        registry = BackendRegistry()
        mgr = VectorIndexManager(catalog, registry)
        idx = mgr.create_index("t1", "docs", "emb", "HNSW", "cosine",
                               params={"dimensions": 64})
        mgr.drop_index("t1", idx.index_name)
        self.assertEqual(len(mgr.list_indexes("t1")), 0)


if __name__ == "__main__":
    unittest.main()
