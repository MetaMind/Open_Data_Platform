"""
Unit Tests — AI Synthesis Layer

File: tests/unit/test_synthesis.py
"""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, MagicMock

import numpy as np

from metamind.synthesis.workload_profiler import WorkloadProfiler, WorkloadStats, _extract_structure
from metamind.synthesis.plan_feature_extractor import PlanFeatureExtractor
from metamind.synthesis.rule_generator import RuleGenerator, SynthesizedRule
from metamind.synthesis.feedback_trainer import FeedbackTrainer, _NEW_SAMPLE_THRESHOLD


class TestWorkloadProfiler(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.mock_engine = MagicMock()
        self.profiler = WorkloadProfiler(self.mock_engine)

    async def test_record_execution_writes_correct_schema(self) -> None:
        mock_conn = AsyncMock()
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        self.mock_engine.begin.return_value = mock_ctx

        await self.profiler.record_execution(
            query_id="q1", sql="SELECT * FROM orders",
            plan_type="direct", engine="trino",
            runtime_ms=234.5, row_count=1000, tenant_id="acme",
        )
        params = mock_conn.execute.call_args[0][1]
        assert params["qid"] == "q1"
        assert params["tid"] == "acme"
        assert params["rt"] == 234.5

    async def test_record_execution_extracts_join_count(self) -> None:
        mock_conn = AsyncMock()
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        self.mock_engine.begin.return_value = mock_ctx

        await self.profiler.record_execution(
            query_id="q2",
            sql="SELECT a.id FROM orders a JOIN customers b ON a.id = b.id",
            plan_type="direct", engine="oracle",
            runtime_ms=100.0, row_count=50, tenant_id="acme",
        )
        params = mock_conn.execute.call_args[0][1]
        assert params["joins"] >= 1

    async def test_get_workload_stats_aggregates_correctly(self) -> None:
        mock_conn = AsyncMock()
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        self.mock_engine.connect.return_value = mock_ctx

        agg_row = MagicMock()
        agg_row.total = 100; agg_row.avg_rt = 350.0; agg_row.p95_rt = 900.0
        agg_row.join_heavy = 20; agg_row.agg_heavy = 40; agg_row.filter_only = 30

        table_row = MagicMock(); table_row.tbl = "orders"
        slow_row = MagicMock(); slow_row.sql_fingerprint = "abc123"

        r1 = MagicMock(); r1.fetchone.return_value = agg_row
        r2 = MagicMock(); r2.fetchall.return_value = [table_row]
        r3 = MagicMock(); r3.fetchall.return_value = [slow_row]
        mock_conn.execute = AsyncMock(side_effect=[r1, r2, r3])

        stats = await self.profiler.get_workload_stats("acme", window_hours=24)
        assert stats.total_queries == 100
        assert stats.avg_runtime_ms == 350.0
        assert stats.join_heavy_pct == 20.0
        assert "orders" in stats.top_tables
        assert "abc123" in stats.slow_query_fingerprints

    async def test_get_workload_stats_empty_tenant(self) -> None:
        mock_conn = AsyncMock()
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        self.mock_engine.connect.return_value = mock_ctx

        agg_row = MagicMock(); agg_row.total = 0
        r1 = MagicMock(); r1.fetchone.return_value = agg_row
        mock_conn.execute = AsyncMock(return_value=r1)

        stats = await self.profiler.get_workload_stats("empty")
        assert stats.total_queries == 0

    def test_extract_structure_counts_joins(self) -> None:
        sql = "SELECT a.x FROM a JOIN b ON a.id=b.id JOIN c ON b.cid=c.id"
        s = _extract_structure(sql)
        assert s.join_count >= 2

    def test_extract_structure_generates_16char_fingerprint(self) -> None:
        s = _extract_structure("SELECT 1")
        assert len(s.fingerprint) == 16

    def test_extract_structure_handles_bad_sql(self) -> None:
        s = _extract_structure("NOT VALID SQL !@#$%^")
        assert isinstance(s.fingerprint, str)


class TestPlanFeatureExtractor(unittest.TestCase):

    def setUp(self) -> None:
        self.ex = PlanFeatureExtractor()

    def test_table_and_join_count(self) -> None:
        plan = {"node_type": "join", "children": [
            {"node_type": "scan", "table_name": "orders", "children": []},
            {"node_type": "scan", "table_name": "customers", "children": []},
        ]}
        f = self.ex.extract(plan)
        assert f.num_tables == 2 and f.num_joins == 1

    def test_aggregate_detected(self) -> None:
        plan = {"node_type": "aggregate", "children": [
            {"node_type": "scan", "table_name": "sales", "children": []}
        ]}
        assert self.ex.extract(plan).num_aggregates == 1

    def test_cross_engine_flag_on_iceberg(self) -> None:
        plan = {"node_type": "scan", "table_name": "e", "source_engine": "iceberg", "children": []}
        assert self.ex.extract(plan).cross_engine_flag is True

    def test_no_cross_engine_flag_oracle(self) -> None:
        plan = {"node_type": "scan", "table_name": "t", "source_engine": "oracle", "children": []}
        assert self.ex.extract(plan).cross_engine_flag is False

    def test_partition_pruning_from_stats(self) -> None:
        plan = {"node_type": "scan", "table_name": "events", "children": []}
        f = self.ex.extract(plan, {"events": {"row_count": 1_000_000, "partitioned": True}})
        assert f.partition_pruning_possible is True

    def test_limit_detected(self) -> None:
        plan = {"node_type": "limit", "children": [
            {"node_type": "scan", "table_name": "t", "children": []}
        ]}
        assert self.ex.extract(plan).has_limit is True

    def test_subquery_detected(self) -> None:
        plan = {"node_type": "subquery", "children": [
            {"node_type": "scan", "table_name": "t", "children": []}
        ]}
        assert self.ex.extract(plan).has_subquery is True

    def test_complexity_increases_with_joins(self) -> None:
        simple = {"node_type": "scan", "table_name": "t", "children": []}
        complex_plan = {"node_type": "join", "children": [
            {"node_type": "join", "children": [
                {"node_type": "scan", "table_name": "a", "children": []},
                {"node_type": "scan", "table_name": "b", "children": []},
            ]},
            {"node_type": "scan", "table_name": "c", "children": []},
        ]}
        assert self.ex.extract(complex_plan).complexity_score > self.ex.extract(simple).complexity_score

    def test_to_dict_has_15_keys(self) -> None:
        plan = {"node_type": "scan", "table_name": "x", "children": []}
        assert len(self.ex.extract(plan).to_dict()) == 15


class TestRuleGenerator(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.mock_engine = MagicMock()
        self.rule_gen = RuleGenerator(self.mock_engine)

    def _set_patterns(self, patterns: list) -> None:
        self.rule_gen._fetch_recent_patterns = AsyncMock(return_value=patterns)

    async def test_engine_affinity_rule_generated(self) -> None:
        self._set_patterns([
            {"engine": "trino", "runtime_ms": 200, "join_count": 1,
             "agg_count": 0, "filter_count": 0, "tables": ["orders"]},
            {"engine": "oracle", "runtime_ms": 900, "join_count": 1,
             "agg_count": 0, "filter_count": 0, "tables": ["orders"]},
        ])
        rules = await self.rule_gen.generate_rules("acme", WorkloadStats(total_queries=10))
        affinity = [r for r in rules if r.rule_type == "engine_affinity"]
        assert len(affinity) >= 1
        assert affinity[0].transformation["preferred_engine"] == "trino"

    async def test_pushdown_rule_generated_when_filter_helps(self) -> None:
        self._set_patterns([
            {"engine": "trino", "runtime_ms": 100, "join_count": 1,
             "agg_count": 0, "filter_count": 1, "tables": ["orders"]},
            {"engine": "trino", "runtime_ms": 600, "join_count": 1,
             "agg_count": 0, "filter_count": 0, "tables": ["orders"]},
        ])
        rules = await self.rule_gen.generate_rules("acme", WorkloadStats(total_queries=20))
        assert any(r.rule_type == "pushdown" for r in rules)

    async def test_all_confidences_in_range(self) -> None:
        self._set_patterns([
            {"engine": "spark", "runtime_ms": 50, "join_count": 0,
             "agg_count": 1, "filter_count": 1, "tables": ["events"]},
            {"engine": "trino", "runtime_ms": 500, "join_count": 0,
             "agg_count": 1, "filter_count": 0, "tables": ["events"]},
        ])
        rules = await self.rule_gen.generate_rules(
            "acme", WorkloadStats(total_queries=50, agg_heavy_pct=60)
        )
        for r in rules:
            assert 0.0 <= r.confidence <= 1.0, f"{r.name}: confidence={r.confidence}"

    async def test_register_rules_calls_db(self) -> None:
        mock_conn = AsyncMock()
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        self.mock_engine.begin.return_value = mock_ctx

        rules = [SynthesizedRule(
            name="r1", rule_type="pushdown",
            condition={"table": "orders"}, transformation={"action": "push"},
            confidence=0.8, tenant_id="acme",
        )]
        await self.rule_gen.register_rules(rules)
        assert mock_conn.execute.call_count >= 1

    async def test_retire_stale_rules_returns_correct_count(self) -> None:
        mock_conn = AsyncMock()
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        self.mock_engine.begin.return_value = mock_ctx

        result = MagicMock(); result.rowcount = 3
        mock_conn.execute = AsyncMock(return_value=result)

        assert await self.rule_gen.retire_stale_rules("acme") == 3


class TestFeedbackTrainer(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.mock_db = MagicMock()
        self.mock_dataset = MagicMock()
        self.mock_cost_model = MagicMock()
        self.mock_drift = MagicMock()
        self.trainer = FeedbackTrainer(
            db_engine=self.mock_db,
            dataset_builder=self.mock_dataset,
            cost_model=self.mock_cost_model,
            drift_detector=self.mock_drift,
        )

    async def test_should_retrain_true_on_sample_threshold(self) -> None:
        self.trainer._get_drift_summary = AsyncMock(return_value={"max_psi": 0.0})
        self.trainer._get_last_model_id = AsyncMock(return_value=None)
        self.mock_dataset.count_new_samples = AsyncMock(return_value=_NEW_SAMPLE_THRESHOLD + 1)
        assert await self.trainer._should_retrain("acme") is True

    async def test_should_retrain_false_below_threshold(self) -> None:
        self.trainer._get_drift_summary = AsyncMock(return_value={"max_psi": 0.0})
        self.trainer._get_last_model_id = AsyncMock(return_value=None)
        self.mock_dataset.count_new_samples = AsyncMock(return_value=10)
        assert await self.trainer._should_retrain("acme") is False

    async def test_should_retrain_true_on_drift(self) -> None:
        self.trainer._get_drift_summary = AsyncMock(return_value={"max_psi": 0.25})
        self.mock_dataset.count_new_samples = AsyncMock(return_value=0)
        assert await self.trainer._should_retrain("acme") is True

    async def test_retrain_skipped_with_no_samples(self) -> None:
        self.mock_dataset.export_to_numpy = AsyncMock(
            return_value=(np.empty((0, 15)), np.empty((0,)))
        )
        result = await self.trainer._retrain("acme")
        assert result.skipped_reason == "no samples"

    async def test_retrain_calls_cost_model_train(self) -> None:
        X = np.random.rand(600, 15).astype(np.float32)
        y = np.random.rand(600).astype(np.float32)
        self.mock_dataset.export_to_numpy = AsyncMock(return_value=(X, y))
        self.trainer._evaluate_current_model = AsyncMock(return_value=100.0)
        self.trainer._persist_model_version = AsyncMock()

        metrics = MagicMock(); metrics.mae = 80.0
        self.mock_cost_model.train = MagicMock(return_value=metrics)

        result = await self.trainer._retrain("acme")
        self.mock_cost_model.train.assert_called_once()
        assert result.samples_used == 600
        assert result.mae_after == 80.0
        assert result.improvement_pct > 0


if __name__ == "__main__":
    unittest.main()
