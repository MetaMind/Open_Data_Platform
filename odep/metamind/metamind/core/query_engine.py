"""MetaMind QueryEngine — central orchestration pipeline for all query execution."""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from sqlalchemy.engine import Engine

from metamind.config.feature_flags import FeatureFlags, FeatureFlagManager
from metamind.config.settings import AppSettings
from metamind.core.backends.connector import QueryResult
from metamind.core.backends.registry import BackendRegistry, get_registry
from metamind.core.cache.plan_cache import PlanCache
from metamind.core.costing.cost_model import CostModel, CostWeights
from metamind.core.logical.builder import LogicalPlanBuilder
from metamind.core.logical.inference import PredicateInference
from metamind.core.memo.optimizer import CascadesOptimizer
from metamind.core.metadata.catalog import MetadataCatalog
from metamind.core.workload.classifier import WorkloadClassifier, WorkloadRouter
from metamind.core.rewrite.ai_tuner import AIQueryTuner, TuneResult  # Task 01
from metamind.core.logical.plan_cache import PlanCache as LogicalPlanCache  # Task 02
from metamind.core.security.rls_rewriter import RLSRewriter  # Task 04
from metamind.core.security.query_firewall import QueryFirewall  # Task 06
from metamind.ml.online_learner import OnlineCostLearner  # Task 11

logger = logging.getLogger(__name__)


@dataclass
class QueryContext:
    """Runtime context for a single query execution."""

    query_id: str
    tenant_id: str
    sql: str
    backend_hint: Optional[str] = None
    timeout_seconds: Optional[int] = None
    cost_weights: Optional[CostWeights] = None
    dry_run: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryPipelineResult:
    """Full result including optimization metadata."""

    result: QueryResult
    query_id: str
    optimization_tier: int
    cache_hit: bool
    workload_type: str
    backend_used: str
    optimization_ms: float
    total_ms: float
    plan_cost: float
    flags_used: list[str] = field(default_factory=list)


class QueryEngine:
    """Central query orchestration engine for MetaMind.

    Implements the full optimization pipeline:
    SQL → Parse → Feature Flags → Cache → Classify → Infer →
    Optimize (Cascades + DPccp) → Plan → Execute → Feedback

    All operations are tenant-scoped. Feature flags control which
    F01-F30 capabilities are enabled per tenant.
    """

    def __init__(
        self,
        db_engine: Engine,
        redis: Optional[object],
        settings: AppSettings,
        backend_registry: Optional[BackendRegistry] = None,
    ) -> None:
        """Initialize query engine with all subsystems."""
        self._db_engine = db_engine
        self._settings = settings
        self._catalog = MetadataCatalog(db_engine)
        self._cost_model = CostModel()
        self._plan_cache = PlanCache(
            redis_client=redis,
            ttl_seconds=settings.plan_cache_ttl_seconds,
        )
        self._classifier = WorkloadClassifier()
        self._router = WorkloadRouter()
        self._builder = LogicalPlanBuilder()
        self._inference = PredicateInference()
        self._registry = backend_registry or get_registry()
        # Task 01: AI query tuner (llm_client injected externally when available)
        self._ai_tuner: Optional[AIQueryTuner] = None
        # Task 02: Redis-backed logical plan cache with sqlglot normalization
        self._logical_plan_cache: Optional[LogicalPlanCache] = (
            LogicalPlanCache(redis, ttl_seconds=300) if redis is not None else None
        )
        # Task 04: Row-level security rewriter
        self._rls_rewriter = RLSRewriter(db_engine)
        # Task 06: Query firewall (denylist/allowlist)
        self._query_firewall: Optional[QueryFirewall] = (
            QueryFirewall(db_engine, redis) if redis is not None else None
        )
        # Task 11: Online cost model learner
        self._online_learner: Optional[OnlineCostLearner] = None

    async def execute(self, ctx: QueryContext) -> QueryPipelineResult:
        """Execute the full MetaMind optimization + execution pipeline.

        Async so firewall.check() and rls_rewriter.rewrite() are properly awaited
        and the AI tuner is dispatched without get_event_loop() hacks (fixes W-01).

        Args:
            ctx: QueryContext with all execution parameters.

        Returns:
            QueryPipelineResult with result data and full optimization metadata.
        """
        import asyncio
        pipeline_start = time.monotonic()

        # 1. Load feature flags for this tenant
        flag_mgr = FeatureFlagManager(self._db_engine, ctx.tenant_id)
        flags = flag_mgr.get_flags()
        flags_used: list[str] = []

        try:
            # Task 06: Full async firewall check — denylist AND allowlist mode (fixes W-01)
            if self._query_firewall is not None and flags.F36_query_firewall:
                decision = await self._query_firewall.check(ctx.sql, ctx.tenant_id)
                if not decision.allowed:
                    raise PermissionError(
                        f"Query blocked by firewall ({decision.reason}). "
                        f"fp={decision.fingerprint[:16]}"
                    )
                flags_used.append("F36_query_firewall")

            # Task 04: RLS rewrite — now properly awaited (fixes W-01)
            if flags.F31_rls_enforcement:
                user_roles: list[str] = ctx.metadata.get("user_roles", [])
                rewritten = await self._rls_rewriter.rewrite(
                    ctx.sql, ctx.tenant_id, user_roles
                )
                if rewritten != ctx.sql:
                    flags_used.append("F31_rls_enforcement")
                    ctx = QueryContext(
                        query_id=ctx.query_id, tenant_id=ctx.tenant_id,
                        sql=rewritten, backend_hint=ctx.backend_hint,
                        timeout_seconds=ctx.timeout_seconds,
                        cost_weights=ctx.cost_weights, dry_run=ctx.dry_run,
                        metadata=ctx.metadata,
                    )

            # 2. F09: Plan cache lookup
            if flags.F09_plan_caching:
                cached = self._plan_cache.get(ctx.sql, ctx.tenant_id)
                if cached:
                    flags_used.append("F09_plan_caching")
                    backend = cached.backend or self._default_backend()
                    result = self._execute_on_backend(ctx.sql, backend, ctx)
                    result.cache_hit = True
                    total_ms = (time.monotonic() - pipeline_start) * 1000
                    return QueryPipelineResult(
                        result=result,
                        query_id=ctx.query_id,
                        optimization_tier=0,
                        cache_hit=True,
                        workload_type="cached",
                        backend_used=backend,
                        optimization_ms=0.0,
                        total_ms=total_ms,
                        plan_cost=cached.estimated_cost,
                        flags_used=flags_used,
                    )

            opt_start = time.monotonic()

            # Task 02: Check logical plan cache before running planner
            cached_logical_plan = None
            if self._logical_plan_cache is not None:
                cached_logical_plan = self._logical_plan_cache.get(ctx.sql, ctx.tenant_id)

            # 3. Parse SQL to logical plan
            root = cached_logical_plan or self._builder.build(ctx.sql)
            if cached_logical_plan is not None:
                logger.debug("PlanCache logical HIT for query %s", ctx.query_id)

            # 4. F05: Predicate inference
            if flags.F05_predicate_inference:
                root = self._inference.infer(root)
                flags_used.append("F05_predicate_inference")

            # 5. F24: Workload classification and routing
            workload_type = "exploration"
            available_backends = self._registry.list_backends()
            selected_backend = ctx.backend_hint

            if flags.F24_workload_classification:
                wtype = self._classifier.classify(root, ctx.sql)
                workload_type = wtype.value
                strategy = self._router.get_optimization_strategy(wtype)
                flags_used.append("F24_workload_classification")

                if not selected_backend and available_backends:
                    selected_backend = self._router.route(wtype, available_backends)
                    flags_used.append("F24_routing")

            if not selected_backend:
                selected_backend = self._default_backend()

            # 6. Cascades optimizer (F04, F12)
            optimizer = CascadesOptimizer(
                cost_model=self._cost_model,
                flags=flags,
                budget=10000,
            )
            optimized = optimizer.optimize(root)

            optimization_ms = (time.monotonic() - opt_start) * 1000
            plan_cost = getattr(optimized, "_estimated_cost", 0.0) or 0.0
            tier = 1 if flags.F12_optimization_tiering else 3

            # 7. F09: Store plan in cache
            if flags.F09_plan_caching:
                self._plan_cache.put(
                    ctx.sql, ctx.tenant_id,
                    repr(optimized), selected_backend, plan_cost
                )
                flags_used.append("F09_cache_store")

            # 8. Execute on selected backend
            if ctx.dry_run:
                total_ms = (time.monotonic() - pipeline_start) * 1000
                result = QueryResult(
                    columns=[], rows=[], row_count=0,
                    duration_ms=0.0, backend=selected_backend,
                )
                return QueryPipelineResult(
                    result=result,
                    query_id=ctx.query_id,
                    optimization_tier=tier,
                    cache_hit=False,
                    workload_type=workload_type,
                    backend_used=selected_backend,
                    optimization_ms=optimization_ms,
                    total_ms=total_ms,
                    plan_cost=plan_cost,
                    flags_used=flags_used,
                )

            result = self._execute_on_backend(ctx.sql, selected_backend, ctx)

            # Task 01: AI auto-tuner — dispatch as background task (fixes W-01)
            if self._ai_tuner is not None and flags.F35_ai_tuner and result.duration_ms > 0:
                execution_stats = {
                    "duration_ms": result.duration_ms,
                    "explain_plan": "",
                    "slow_predicates": [],
                }
                try:
                    asyncio.create_task(
                        self._ai_tuner.tune(ctx.sql, execution_stats, ctx.tenant_id)
                    )
                except Exception as tuner_exc:
                    logger.error("AIQueryTuner dispatch error: %s", tuner_exc)

            # Task 11: Feed actual stats to online cost learner post-execution
            if self._online_learner is not None:
                try:
                    self._online_learner.record(
                        query_id=ctx.query_id,
                        features={"duration_ms": result.duration_ms},
                        predicted_cost=plan_cost,
                        actual_cost=result.duration_ms,
                    )
                    if self._online_learner.should_update():
                        self._online_learner.partial_fit()
                except Exception as ol_exc:
                    logger.error("OnlineCostLearner feed error: %s", ol_exc)

            # 9. F20: Record feedback for adaptive learning
            if flags.F20_regret_minimization:
                self._record_feedback(ctx, result, plan_cost, flags)
                flags_used.append("F20_regret_minimization")

            total_ms = (time.monotonic() - pipeline_start) * 1000
            logger.info(
                "Query %s completed: backend=%s tier=%d opt=%.1fms total=%.1fms",
                ctx.query_id, selected_backend, tier, optimization_ms, total_ms
            )

            return QueryPipelineResult(
                result=result,
                query_id=ctx.query_id,
                optimization_tier=tier,
                cache_hit=False,
                workload_type=workload_type,
                backend_used=selected_backend,
                optimization_ms=optimization_ms,
                total_ms=total_ms,
                plan_cost=plan_cost,
                flags_used=flags_used,
            )

        except Exception as exc:
            logger.error("Query %s failed: %s", ctx.query_id, exc, exc_info=True)
            raise

    async def execute_sql(
        self,
        sql: str,
        tenant_id: str,
        backend_hint: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> QueryPipelineResult:
        """Convenience method for simple SQL execution."""
        ctx = QueryContext(
            query_id=str(uuid.uuid4())[:12],
            tenant_id=tenant_id,
            sql=sql,
            backend_hint=backend_hint,
            timeout_seconds=timeout,
        )
        return await self.execute(ctx)

    def _execute_on_backend(
        self, sql: str, backend_id: str, ctx: QueryContext
    ) -> QueryResult:
        """Execute SQL on the specified backend connector."""
        connector = self._registry.get(backend_id)
        if connector is None:
            # Fallback: execute directly on metadata DB
            logger.warning(
                "Backend %s not registered, falling back to metadata DB", backend_id
            )
            from metamind.core.backends.connector import QueryResult as QR
            import sqlalchemy as sa

            start = time.monotonic()
            with self._db_engine.connect() as conn:
                try:
                    result = conn.execute(sa.text(sql))
                    rows = [dict(r._mapping) for r in result.fetchall()]
                    cols = list(rows[0].keys()) if rows else []
                    duration = (time.monotonic() - start) * 1000
                    return QR(
                        columns=cols, rows=rows, row_count=len(rows),
                        duration_ms=duration, backend="metamind-internal",
                        query_id=ctx.query_id,
                    )
                except Exception as exc:
                    raise RuntimeError(f"Query execution failed: {exc}") from exc

        return connector.execute(
            sql,
            timeout_seconds=ctx.timeout_seconds or self._settings.queue_timeout_seconds,
        )

    def _record_feedback(
        self,
        ctx: QueryContext,
        result: QueryResult,
        predicted_cost: float,
        flags: FeatureFlags,
    ) -> None:
        """Record execution feedback for adaptive learning."""
        try:
            from metamind.core.adaptive.regret import OptimizationDecision, RegretTracker
            tracker = RegretTracker(self._db_engine, ctx.tenant_id)
            decision = OptimizationDecision(
                query_id=ctx.query_id,
                tenant_id=ctx.tenant_id,
                decision_type="engine_routing",
                chosen_option=result.backend,
                alternatives=[],
                predicted_cost=predicted_cost,
                actual_cost=result.duration_ms,
            )
            tracker.record_decision(decision)
        except Exception as exc:
            logger.warning("Failed to record adaptive feedback: %s", exc)

    def _default_backend(self) -> str:
        """Get the default execution backend."""
        backends = self._registry.list_backends()
        if backends:
            return backends[0]
        return "metamind-internal"
