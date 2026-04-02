"""Unit tests for F30 What-If Optimization Replay."""
from __future__ import annotations

import json
import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from metamind.core.catalog.metadata import MetadataCatalog
from metamind.core.cost.model import CostModel
from metamind.core.replay.recorder import (
    OptimizationSimulator,
    ReplayRecorder,
    ReplayResult,
    ReplayScenario,
    WhatIfAPI,
)
from metamind.core.types import ColumnMeta, IndexMeta, LogicalNode, TableMeta


class TestScenarioCreation(unittest.TestCase):
    """Test scenario creation and persistence."""

    def setUp(self) -> None:
        self.catalog = MetadataCatalog()
        self.cost_model = CostModel()
        self.simulator = OptimizationSimulator(self.catalog, self.cost_model)

    def test_scenario_has_id(self) -> None:
        scenario = self.simulator.create_scenario(
            tenant_id="t1",
            name="Test Scenario",
            changes=[{"type": "add_index", "table": "orders", "column": "status"}],
        )
        self.assertIsNotNone(scenario.scenario_id)
        self.assertTrue(len(scenario.scenario_id) > 0)

    def test_scenario_persists_in_list(self) -> None:
        self.simulator.create_scenario(
            tenant_id="t1",
            name="Scenario A",
            changes=[{"type": "add_index", "table": "t", "column": "c"}],
        )
        scenarios = self.simulator.list_scenarios("t1")
        self.assertEqual(len(scenarios), 1)
        self.assertEqual(scenarios[0].name, "Scenario A")

    def test_multiple_scenarios(self) -> None:
        self.simulator.create_scenario("t1", "A", [{"type": "add_index", "table": "t", "column": "a"}])
        self.simulator.create_scenario("t1", "B", [{"type": "add_index", "table": "t", "column": "b"}])
        self.simulator.create_scenario("t2", "C", [{"type": "add_index", "table": "t", "column": "c"}])
        self.assertEqual(len(self.simulator.list_scenarios("t1")), 2)
        self.assertEqual(len(self.simulator.list_scenarios("t2")), 1)


class TestApplyHypotheticalChanges(unittest.TestCase):
    """Test that hypothetical changes are applied to copies, not originals."""

    def setUp(self) -> None:
        self.catalog = MetadataCatalog()
        self.catalog.register_table(
            "t1",
            TableMeta(
                table_name="orders",
                schema_name="public",
                tenant_id="t1",
                columns=[ColumnMeta(name="status", dtype="varchar")],
                row_count=100_000,
            ),
        )
        self.cost_model = CostModel()
        self.simulator = OptimizationSimulator(self.catalog, self.cost_model)

    def test_original_catalog_unchanged(self) -> None:
        original_indexes = self.catalog.get_indexes("t1", "orders")
        self.assertEqual(len(original_indexes), 0)

        modified = self.simulator._apply_hypothetical_changes(
            self.catalog, "t1",
            [{"type": "add_index", "table": "orders", "column": "status"}],
        )

        # Original should still have no indexes
        self.assertEqual(len(self.catalog.get_indexes("t1", "orders")), 0)

        # Modified should have the new index
        mod_indexes = modified.get_indexes("t1", "orders")
        self.assertEqual(len(mod_indexes), 1)
        self.assertIn("status", mod_indexes[0].columns)

    def test_add_index_reduces_cost(self) -> None:
        # SeqScan cost without index
        node_seq = LogicalNode(
            node_type="SeqScan",
            properties={"table": "orders"},
            estimated_rows=100_000,
        )
        seq_cv = self.cost_model.estimate(node_seq, {"row_count": 100_000})
        seq_cost = self.cost_model.total_cost(seq_cv)

        # IndexScan cost with index
        node_idx = LogicalNode(
            node_type="IndexScan",
            properties={"table": "orders"},
            estimated_rows=10_000,
        )
        idx_cv = self.cost_model.estimate(node_idx, {"row_count": 100_000, "selectivity": 0.1})
        idx_cost = self.cost_model.total_cost(idx_cv)

        self.assertLess(idx_cost, seq_cost)


class TestReplayRecorder(unittest.TestCase):
    """Test optimization recording."""

    def test_record_and_load(self) -> None:
        recorder = ReplayRecorder()
        recorder.record(
            tenant_id="t1",
            query_id="q1",
            sql="SELECT * FROM orders",
            logical_plan_json='{"type": "SeqScan"}',
            optimization_context={"total_cost": 150.0},
        )
        recorder.record(
            tenant_id="t1",
            query_id="q2",
            sql="SELECT id FROM orders WHERE status = 'active'",
            logical_plan_json='{"type": "IndexScan"}',
            optimization_context={"total_cost": 25.0},
        )

        history = recorder.load_history("t1")
        self.assertEqual(len(history), 2)

    def test_load_filters_by_tenant(self) -> None:
        recorder = ReplayRecorder()
        recorder.record("t1", "q1", "SELECT 1", "{}", {"total_cost": 10.0})
        recorder.record("t2", "q2", "SELECT 2", "{}", {"total_cost": 20.0})

        t1_history = recorder.load_history("t1")
        self.assertEqual(len(t1_history), 1)


class TestRunScenario(unittest.TestCase):
    """Test full scenario simulation."""

    def setUp(self) -> None:
        self.catalog = MetadataCatalog()
        self.catalog.register_table(
            "t1",
            TableMeta(
                table_name="orders",
                schema_name="public",
                tenant_id="t1",
                columns=[ColumnMeta(name="status", dtype="varchar")],
                row_count=100_000,
            ),
        )
        self.catalog.update_statistics("t1", "orders", {"row_count": 100_000})
        self.cost_model = CostModel()
        self.simulator = OptimizationSimulator(self.catalog, self.cost_model)

        # Pre-record some queries
        self.simulator._recorder.record(
            "t1", "q1", "SELECT * FROM orders",
            '{"type": "SeqScan"}', {"total_cost": 1000.0}
        )
        self.simulator._recorder.record(
            "t1", "q2", "SELECT id FROM orders WHERE status = 'active'",
            '{"type": "SeqScan"}', {"total_cost": 500.0}
        )

    def test_replay_result_structure(self) -> None:
        scenario = self.simulator.create_scenario(
            "t1", "Add index on orders.status",
            [{"type": "add_index", "table": "orders", "column": "status"}],
        )
        result = self.simulator.run_scenario(scenario, "t1")

        self.assertIsInstance(result, ReplayResult)
        self.assertEqual(result.scenario_id, scenario.scenario_id)
        self.assertGreater(result.queries_replayed, 0)

    def test_per_query_breakdown_exists(self) -> None:
        scenario = self.simulator.create_scenario(
            "t1", "Test",
            [{"type": "add_index", "table": "orders", "column": "status"}],
        )
        result = self.simulator.run_scenario(scenario, "t1")

        self.assertTrue(len(result.per_query_results) > 0)
        for pq in result.per_query_results:
            self.assertIn("original_cost", pq)
            self.assertIn("simulated_cost", pq)
            self.assertIn("delta_pct", pq)

    def test_add_index_shows_improvement(self) -> None:
        scenario = self.simulator.create_scenario(
            "t1", "Add status index",
            [{"type": "add_index", "table": "orders", "column": "status"}],
        )
        result = self.simulator.run_scenario(scenario, "t1")
        # Adding an index should reduce cost
        self.assertLess(result.simulated_total_cost, result.original_total_cost)
        self.assertGreater(result.cost_improvement_pct, 0)

    def test_recommendation_text_generated(self) -> None:
        scenario = self.simulator.create_scenario(
            "t1", "Test",
            [{"type": "add_index", "table": "orders", "column": "status"}],
        )
        result = self.simulator.run_scenario(scenario, "t1")
        self.assertIsInstance(result.recommendation, str)
        self.assertTrue(len(result.recommendation) > 10)


class TestWhatIfAPI(unittest.TestCase):
    """Test high-level WhatIfAPI convenience methods."""

    def setUp(self) -> None:
        self.catalog = MetadataCatalog()
        self.catalog.register_table(
            "t1",
            TableMeta(
                table_name="events",
                schema_name="public",
                tenant_id="t1",
                columns=[ColumnMeta(name="user_id", dtype="int")],
                row_count=1_000_000,
            ),
        )
        self.catalog.update_statistics("t1", "events", {"row_count": 1_000_000})
        self.cost_model = CostModel()
        self.simulator = OptimizationSimulator(self.catalog, self.cost_model)
        self.whatif = WhatIfAPI(self.simulator)

        self.simulator._recorder.record(
            "t1", "q1", "SELECT * FROM events",
            "{}", {"total_cost": 5000.0}
        )

    def test_simulate_add_index(self) -> None:
        result = self.whatif.simulate_add_index("t1", "events", "user_id")
        self.assertIsInstance(result, ReplayResult)

    def test_simulate_enable_feature(self) -> None:
        result = self.whatif.simulate_enable_feature("t1", "F09_cache")
        self.assertIsInstance(result, ReplayResult)

    def test_simulate_migrate_table(self) -> None:
        result = self.whatif.simulate_migrate_table("t1", "events", "duckdb")
        self.assertIsInstance(result, ReplayResult)


class TestScenarioResultRetrieval(unittest.TestCase):
    """Test getting results for completed scenarios."""

    def test_get_result_after_run(self) -> None:
        catalog = MetadataCatalog()
        catalog.register_table(
            "t1",
            TableMeta(table_name="t", schema_name="s", tenant_id="t1", row_count=100)
        )
        sim = OptimizationSimulator(catalog, CostModel())
        sim._recorder.record("t1", "q1", "SELECT * FROM t", "{}", {"total_cost": 50.0})
        scenario = sim.create_scenario(
            "t1", "test", [{"type": "add_index", "table": "t", "column": "c"}]
        )
        sim.run_scenario(scenario, "t1")
        result = sim.get_result(scenario.scenario_id)
        self.assertIsNotNone(result)

    def test_get_nonexistent_result(self) -> None:
        sim = OptimizationSimulator(MetadataCatalog(), CostModel())
        result = sim.get_result("nonexistent")
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
