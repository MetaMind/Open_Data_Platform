"""
Integration Tests — AI Synthesis Cycle

File: tests/integration/test_synthesis_cycle.py

End-to-end tests verifying that a full synthesis cycle produces rules,
retires stale ones, and triggers retraining when data drifts.
All external dependencies are mocked.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np

from metamind.synthesis.synthesis_engine import SynthesisEngine, SynthesisCycleResult
from metamind.synthesis.workload_profiler import WorkloadStats
from metamind.synthesis.rule_generator import SynthesizedRule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db_engine() -> tuple:
    engine = MagicMock()
    conn = AsyncMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    engine.begin.return_value = ctx
    engine.connect.return_value = ctx
    return engine, conn


def _active_stats() -> WorkloadStats:
    return WorkloadStats(
        total_queries=500,
        join_heavy_pct=40.0,
        agg_heavy_pct=60.0,
        filter_only_pct=10.0,
        avg_runtime_ms=350.0,
        p95_runtime_ms=900.0,
        top_tables=["orders", "customers"],
        slow_query_fingerprints=["fp1", "fp2"],
    )


def _make_rule(name: str, rtype: str = "engine_affinity") -> SynthesizedRule:
    return SynthesizedRule(
        name=name,
        rule_type=rtype,
        condition={"table": "orders"},
        transformation={"preferred_engine": "trino"},
        confidence=0.85,
        support_count=20,
        tenant_id="acme",
    )


# ---------------------------------------------------------------------------
# SynthesisEngine end-to-end
# ---------------------------------------------------------------------------

class TestSynthesisCycleEndToEnd(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.engine, self.conn = _make_db_engine()
        self.cost_model = MagicMock()
        self.drift_detector = MagicMock()
        self.synth = SynthesisEngine(
            db_engine=self.engine,
            cost_model=self.cost_model,
            drift_detector=self.drift_detector,
            active_tenant_ids=["acme"],
        )

    async def test_cycle_produces_rules_and_records_metrics(self) -> None:
        """Full cycle should set rules_generated and update the metrics counter."""
        self.synth._profiler.get_workload_stats = AsyncMock(return_value=_active_stats())
        rules = [_make_rule("r1"), _make_rule("r2")]
        self.synth._rule_gen.generate_rules = AsyncMock(return_value=rules)
        self.synth._rule_gen.register_rules = AsyncMock()
        self.synth._rule_gen.retire_stale_rules = AsyncMock(return_value=1)
        self.synth._trainer.retrain_if_needed = AsyncMock(
            return_value=MagicMock(skipped_reason="thresholds not met")
        )

        result = await self.synth.run_synthesis_cycle("acme")

        assert result.rules_generated == 2
        assert result.rules_retired == 1
        assert result.retrained is False
        assert result.cycle_duration_ms >= 0
        assert self.synth.metrics["rules_generated_total"] == 2

    async def test_cycle_triggers_retrain_when_skipped_reason_empty(self) -> None:
        """retrained=True when FeedbackTrainer returns result with no skipped_reason."""
        from metamind.synthesis.feedback_trainer import RetrainResult
        self.synth._profiler.get_workload_stats = AsyncMock(return_value=_active_stats())
        self.synth._rule_gen.generate_rules = AsyncMock(return_value=[_make_rule("r1")])
        self.synth._rule_gen.register_rules = AsyncMock()
        self.synth._rule_gen.retire_stale_rules = AsyncMock(return_value=0)
        self.synth._trainer.retrain_if_needed = AsyncMock(
            return_value=RetrainResult(
                tenant_id="acme", samples_used=600,
                mae_before=50.0, mae_after=35.0,
                improvement_pct=30.0, skipped_reason="",
            )
        )

        result = await self.synth.run_synthesis_cycle("acme")

        assert result.retrained is True
        assert result.mae_before == 50.0
        assert result.mae_after == 35.0
        assert self.synth.metrics["retrain_count"] == 1

    async def test_cycle_skips_when_no_queries(self) -> None:
        """Cycle with zero queries should skip rule gen and return early."""
        self.synth._profiler.get_workload_stats = AsyncMock(
            return_value=WorkloadStats(total_queries=0)
        )
        self.synth._rule_gen.generate_rules = AsyncMock()

        result = await self.synth.run_synthesis_cycle("acme")

        assert result.rules_generated == 0
        self.synth._rule_gen.generate_rules.assert_not_called()

    async def test_cycle_records_error_and_increments_error_metric(self) -> None:
        """Unexpected exception should be captured in result.error, not re-raised."""
        self.synth._profiler.get_workload_stats = AsyncMock(
            side_effect=RuntimeError("DB connection lost")
        )

        result = await self.synth.run_synthesis_cycle("acme")

        assert result.error is not None
        assert "DB connection lost" in result.error
        assert self.synth.metrics["cycles_errored"] == 1

    async def test_multiple_cycles_accumulate_metrics(self) -> None:
        """Running the cycle twice should double the counters."""
        self.synth._profiler.get_workload_stats = AsyncMock(return_value=_active_stats())
        self.synth._rule_gen.generate_rules = AsyncMock(return_value=[_make_rule("r1")])
        self.synth._rule_gen.register_rules = AsyncMock()
        self.synth._rule_gen.retire_stale_rules = AsyncMock(return_value=0)
        self.synth._trainer.retrain_if_needed = AsyncMock(
            return_value=MagicMock(skipped_reason="thresholds not met")
        )

        await self.synth.run_synthesis_cycle("acme")
        await self.synth.run_synthesis_cycle("acme")

        assert self.synth.metrics["rules_generated_total"] == 2
        assert self.synth.metrics["cycles_completed"] == 2


# ---------------------------------------------------------------------------
# Rule retirement
# ---------------------------------------------------------------------------

class TestRuleRetirement(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.engine, self.conn = _make_db_engine()
        from metamind.synthesis.rule_generator import RuleGenerator
        self.rg = RuleGenerator(self.engine)

    async def test_retire_stale_rules_executes_update(self) -> None:
        result = MagicMock()
        result.rowcount = 5
        self.conn.execute = AsyncMock(return_value=result)

        retired = await self.rg.retire_stale_rules("acme", max_age_days=30)
        assert retired == 5

    async def test_retire_returns_zero_on_db_error(self) -> None:
        self.conn.execute = AsyncMock(side_effect=Exception("DB error"))
        retired = await self.rg.retire_stale_rules("acme", max_age_days=30)
        assert retired == 0


# ---------------------------------------------------------------------------
# Synthesis status API endpoint
# ---------------------------------------------------------------------------

class TestSynthesisStatusEndpoint(unittest.IsolatedAsyncioTestCase):

    async def test_synthesis_status_disabled_when_no_engine(self) -> None:
        ctx = MagicMock()
        ctx._synthesis_engine = None
        req = MagicMock()
        req.app.state.context = ctx

        from metamind.api.query_routes import synthesis_status
        result = await synthesis_status(req)

        assert result["status"] == "disabled"

    async def test_synthesis_status_running_returns_metrics(self) -> None:
        synth_engine = MagicMock()
        synth_engine.get_status.return_value = {
            "active_tenants": 2,
            "background_running": True,
            "metrics": {"rules_generated_total": 10, "retrain_count": 1},
        }
        ctx = MagicMock()
        ctx._synthesis_engine = synth_engine
        req = MagicMock()
        req.app.state.context = ctx

        from metamind.api.query_routes import synthesis_status
        result = await synthesis_status(req)

        assert result["status"] == "running"
        assert result["active_tenants"] == 2

    async def test_run_synthesis_endpoint_triggers_cycle(self) -> None:
        synth_engine = AsyncMock()
        synth_engine.run_synthesis_cycle = AsyncMock(
            return_value=SynthesisCycleResult(
                tenant_id="acme",
                rules_generated=3,
                rules_retired=1,
                retrained=False,
                cycle_duration_ms=200,
            )
        )
        ctx = MagicMock()
        ctx._synthesis_engine = synth_engine
        req = MagicMock()
        req.app.state.context = ctx

        from metamind.api.query_routes import run_synthesis
        result = await run_synthesis(req, tenant_id="acme")

        assert result["rules_generated"] == 3
        assert result["rules_retired"] == 1
        synth_engine.run_synthesis_cycle.assert_called_once_with("acme")

    async def test_run_synthesis_raises_503_when_engine_missing(self) -> None:
        from fastapi import HTTPException
        ctx = MagicMock()
        ctx._synthesis_engine = None
        req = MagicMock()
        req.app.state.context = ctx

        from metamind.api.query_routes import run_synthesis
        with self.assertRaises(HTTPException) as cm:
            await run_synthesis(req, tenant_id="acme")
        assert cm.exception.status_code == 503


# ---------------------------------------------------------------------------
# Retraining on drift detection
# ---------------------------------------------------------------------------

class TestRetrainOnDrift(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.engine, self.conn = _make_db_engine()
        self.cost_model = MagicMock()
        self.drift_detector = MagicMock()
        from metamind.synthesis.training_dataset import TrainingDatasetBuilder
        self.dataset = TrainingDatasetBuilder(self.engine)

        from metamind.synthesis.feedback_trainer import FeedbackTrainer
        self.trainer = FeedbackTrainer(
            db_engine=self.engine,
            dataset_builder=self.dataset,
            cost_model=self.cost_model,
            drift_detector=self.drift_detector,
        )

    async def test_should_retrain_true_when_drift_exceeds_threshold(self) -> None:
        self.trainer._get_drift_summary = AsyncMock(return_value={"max_psi": 0.30})
        self.trainer._get_last_model_id = AsyncMock(return_value="model-1")
        self.dataset.count_new_samples = AsyncMock(return_value=0)

        result = await self.trainer._should_retrain("acme")
        assert result is True

    async def test_should_retrain_false_when_drift_low_and_few_samples(self) -> None:
        self.trainer._get_drift_summary = AsyncMock(return_value={"max_psi": 0.05})
        self.trainer._get_last_model_id = AsyncMock(return_value="model-1")
        self.dataset.count_new_samples = AsyncMock(return_value=50)

        result = await self.trainer._should_retrain("acme")
        assert result is False


if __name__ == "__main__":
    unittest.main()
