"""
Synthesis Engine — AI Synthesis Layer Orchestrator

File: metamind/synthesis/synthesis_engine.py
Role: Senior ML Engineer
Dependencies: metamind.synthesis.* sub-modules, asyncio

Top-level controller that ties workload profiling, plan feature extraction,
rule generation, and feedback-driven retraining into a single synthesis
cycle.  Runs as a background asyncio task, firing on a configurable interval.

Exposes Prometheus-compatible metric counters via a plain dict so that
metamind/observability/metrics.py can scrape them.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncEngine

from metamind.synthesis.workload_profiler import WorkloadProfiler
from metamind.synthesis.plan_feature_extractor import PlanFeatureExtractor
from metamind.synthesis.rule_generator import RuleGenerator
from metamind.synthesis.feedback_trainer import FeedbackTrainer, RetrainResult
from metamind.synthesis.training_dataset import TrainingDatasetBuilder

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------

@dataclass
class SynthesisCycleResult:
    """Summary of one completed synthesis cycle."""
    tenant_id: str
    rules_generated: int = 0
    rules_retired: int = 0
    retrained: bool = False
    mae_before: float = 0.0
    mae_after: float = 0.0
    cycle_duration_ms: int = 0
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# SynthesisEngine
# ---------------------------------------------------------------------------

class SynthesisEngine:
    """
    Orchestrates the full AI synthesis pipeline for a set of tenants.

    Lifecycle:
    1. ``run_synthesis_cycle(tenant_id)`` — single synchronous cycle for one tenant
    2. ``start_background_synthesis(interval_minutes)`` — spawns an asyncio background
       task that calls run_synthesis_cycle for each active tenant on a schedule.
    3. ``stop()`` — cancels the background task gracefully.

    Metrics are available via ``self.metrics`` dict:
    - ``rules_generated_total``
    - ``retrain_count``
    - ``cycle_duration_ms_last``
    """

    def __init__(
        self,
        db_engine: AsyncEngine,
        cost_model: Any,
        drift_detector: Any,
        active_tenant_ids: Optional[List[str]] = None,
    ) -> None:
        self._db = db_engine
        self._active_tenants: List[str] = active_tenant_ids or []
        self._bg_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]

        # Sub-components
        self._profiler = WorkloadProfiler(db_engine)
        self._extractor = PlanFeatureExtractor()
        self._rule_gen = RuleGenerator(db_engine)
        self._dataset_builder = TrainingDatasetBuilder(db_engine)
        self._trainer = FeedbackTrainer(
            db_engine=db_engine,
            dataset_builder=self._dataset_builder,
            cost_model=cost_model,
            drift_detector=drift_detector,
        )

        # Prometheus-style counters
        self.metrics: Dict[str, Any] = {
            "rules_generated_total": 0,
            "retrain_count": 0,
            "cycle_duration_ms_last": 0,
            "cycles_completed": 0,
            "cycles_errored": 0,
        }

    # ------------------------------------------------------------------
    # Single-cycle public API
    # ------------------------------------------------------------------

    async def run_synthesis_cycle(self, tenant_id: str) -> SynthesisCycleResult:
        """
        Execute the full synthesis pipeline for one tenant:
        1. Profile workload
        2. Generate rules
        3. Register / retire rules
        4. Retrain cost model if needed

        Returns a SynthesisCycleResult summary.
        """
        start = time.monotonic()
        result = SynthesisCycleResult(tenant_id=tenant_id)
        try:
            # Step 1: Workload profiling
            workload_stats = await self._profiler.get_workload_stats(tenant_id)
            logger.info(
                "SynthesisEngine cycle tenant=%s total_queries=%d",
                tenant_id,
                workload_stats.total_queries,
            )

            if workload_stats.total_queries == 0:
                logger.info(
                    "SynthesisEngine: no queries for tenant=%s, skipping", tenant_id
                )
                return result

            # Step 2: Rule generation
            rules = await self._rule_gen.generate_rules(tenant_id, workload_stats)
            result.rules_generated = len(rules)

            # Step 3: Register + retire
            await self._rule_gen.register_rules(rules)
            result.rules_retired = await self._rule_gen.retire_stale_rules(tenant_id)

            # Step 4: Conditional retraining
            retrain_result: Optional[RetrainResult] = await self._trainer.retrain_if_needed(
                tenant_id
            )
            if retrain_result and not retrain_result.skipped_reason:
                result.retrained = True
                result.mae_before = retrain_result.mae_before
                result.mae_after = retrain_result.mae_after
                self.metrics["retrain_count"] += 1

            # Update metrics
            self.metrics["rules_generated_total"] += result.rules_generated
            self.metrics["cycles_completed"] += 1

        except Exception as exc:
            logger.error(
                "SynthesisEngine.run_synthesis_cycle failed tenant=%s: %s",
                tenant_id,
                exc,
            )
            result.error = str(exc)
            self.metrics["cycles_errored"] += 1
        finally:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            result.cycle_duration_ms = elapsed_ms
            self.metrics["cycle_duration_ms_last"] = elapsed_ms

        return result

    # ------------------------------------------------------------------
    # Background task management
    # ------------------------------------------------------------------

    async def start_background_synthesis(
        self, interval_minutes: int = 60
    ) -> None:
        """
        Spawn a background asyncio task that runs synthesis cycles for all
        active tenants every *interval_minutes* minutes.
        """
        if self._bg_task and not self._bg_task.done():
            logger.warning(
                "SynthesisEngine: background task already running, ignoring start()"
            )
            return
        self._bg_task = asyncio.create_task(
            self._background_loop(interval_minutes),
            name="synthesis_engine_bg",
        )
        logger.info(
            "SynthesisEngine: background synthesis started interval=%d min",
            interval_minutes,
        )

    async def stop(self) -> None:
        """Cancel the background synthesis task and wait for it to finish."""
        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                logger.info("SynthesisEngine.stop: background task cancelled cleanly")
            logger.info("SynthesisEngine: background synthesis stopped")

    def add_tenant(self, tenant_id: str) -> None:
        """Register a new tenant for background synthesis cycles."""
        if tenant_id not in self._active_tenants:
            self._active_tenants.append(tenant_id)

    def get_status(self) -> Dict[str, Any]:
        """Return current engine status and metrics."""
        return {
            "active_tenants": len(self._active_tenants),
            "background_running": bool(
                self._bg_task and not self._bg_task.done()
            ),
            "metrics": dict(self.metrics),
        }

    # ------------------------------------------------------------------
    # Internal background loop
    # ------------------------------------------------------------------

    async def _background_loop(self, interval_minutes: int) -> None:
        """Infinite loop: run synthesis for each tenant, then sleep."""
        interval_seconds = interval_minutes * 60
        while True:
            try:
                if not self._active_tenants:
                    logger.info(
                        "SynthesisEngine: no active tenants registered, sleeping"
                    )
                else:
                    for tenant_id in list(self._active_tenants):
                        result = await self.run_synthesis_cycle(tenant_id)
                        logger.info(
                            "SynthesisEngine bg cycle tenant=%s rules=%d retrained=%s "
                            "duration_ms=%d error=%s",
                            tenant_id,
                            result.rules_generated,
                            result.retrained,
                            result.cycle_duration_ms,
                            result.error or "none",
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "SynthesisEngine background loop unexpected error: %s", exc
                )
            await asyncio.sleep(interval_seconds)
