"""
Query Router - ML-Based Query Routing

File: metamind/core/router.py
Role: Senior ML Engineer
Phase: 1
Dependencies: CDC Monitor, Cost Model, Cache Manager

Intelligent query router using ML cost models and freshness awareness.
Decides: Oracle (fresh) vs. Trino/S3 (fast) vs. Spark (batch) vs. Hybrid (both).
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any, TYPE_CHECKING

import sqlglot
from sqlglot import Expression

if TYPE_CHECKING:
    from metamind.core.cdc_monitor import CDCMonitor
    from metamind.cache.result_cache import CacheManager
    from metamind.ml.cost_model import QueryCostModel
    from metamind.config.settings import AppSettings
    from metamind.core.metadata.catalog import MetadataCatalog
    from metamind.core.logical.planner import CostBasedPlanner
    from metamind.ml.feature_store import FeatureStore
    from metamind.core.control_plane import EngineHealthRegistry, RoutingPolicyManager
    from metamind.core.adaptive_router import CDCLagAdaptiveRouter
    from metamind.core.gpu_router import GPURouter

from metamind.core.workload.noisy_neighbor import NoisyNeighborDetector  # Task 03
from metamind.core.workload.sla_enforcer import SLAEnforcer  # Task 17
from metamind.core.federation.failover_router import FailoverRouter, QueryExecutionError  # Task 18

logger = logging.getLogger(__name__)

from metamind.core.routing_decision import (  # noqa: F401
    ExecutionStrategy,
    RoutingDecision,
)

class QueryRouter:
    """
    Intelligent query router using ML cost models and freshness awareness.
    Decides: Oracle (fresh) vs. Trino/S3 (fast) vs. Spark (batch) vs. Hybrid (both).
    """
    
    # Freshness levels (in seconds)
    FRESHNESS_LEVELS = {
        "realtime": 0,       # Oracle only
        "recent": 300,       # 5 min - S3 if CDC lag < 5min
        "standard": 1800,    # 30 min - S3 default
        "historical": 86400  # 24 hours - S3 definitely
    }
    
    def __init__(
        self,
        catalog: "MetadataCatalog",
        cdc_monitor: "CDCMonitor",
        cost_model: Optional["QueryCostModel"],
        cache_manager: "CacheManager",
        settings: "AppSettings",
        planner: Optional["CostBasedPlanner"] = None,
        feature_store: Optional["FeatureStore"] = None,
        health_registry: Optional["EngineHealthRegistry"] = None,
        policy_manager: Optional["RoutingPolicyManager"] = None,
        adaptive_router: Optional["CDCLagAdaptiveRouter"] = None,
        spark_engine: Optional[Any] = None,
        gpu_router: Optional["GPURouter"] = None,
    ) -> None:
        """Initialize query router with all optional subsystem references."""
        self.catalog = catalog
        self.cdc = cdc_monitor
        self.cost_model = cost_model
        self.cache = cache_manager
        self.settings = settings
        
        self.planner = planner
        self.feature_store = feature_store
        self.health_registry = health_registry
        self.policy_manager = policy_manager
        self.adaptive_router = adaptive_router
        self.spark_engine = spark_engine
        self.gpu_router = gpu_router
        # Phase 2: noisy-neighbor (T03), SLA enforcer (T17), failover (T18)
        self._noisy_neighbor: NoisyNeighborDetector | None = None
        self._sla_enforcer: SLAEnforcer | None = None
        self._failover_router: FailoverRouter | None = None
        logger.debug("QueryRouter initialized with enhanced routing")
    
    async def route(
        self,
        sql: str,
        tenant_id: str,
        user_context: Dict[str, Any]
    ) -> RoutingDecision:
        """Route SQL to optimal engine and return execution plan."""
        logger.debug(f"Routing query for tenant {tenant_id}")

        # T03: Noisy-neighbor throttle
        if self._noisy_neighbor is not None:
            throttle = self._noisy_neighbor.throttle_factor(tenant_id)
            if throttle < 1.0:
                import asyncio
                await asyncio.sleep((1.0 - throttle) * 0.5)

        parsed = sqlglot.parse_one(sql)
        normalized = parsed.sql(pretty=True)
        cache_key = self._compute_cache_key(normalized, tenant_id)
        
        cached = await self.cache.get(cache_key)
        if cached and not self._is_cache_stale(cached, user_context):
            logger.debug("Cache hit, returning cached result")
            return self._cache_hit_decision(cached, cache_key)
        
        tables = self._extract_tables(parsed)
        features = self._extract_features(parsed, tables)
        
        logical_plan = None
        if self.planner:
            logical_plan = self.planner.extract_logical_plan(sql)
            if logical_plan:
                # Estimate cardinality
                cardinality = self.planner.estimate_cardinality(logical_plan, tenant_id)
                features["estimated_cardinality"] = cardinality.estimated_rows
                features["cardinality_confidence"] = cardinality.confidence
        
        # 5. Check if this is a batch job (should go to Spark)
        is_batch_job = False
        if self.spark_engine:
            is_batch_job = self.spark_engine.is_batch_job(features)
        
        freshness_tolerance = self._infer_freshness(parsed, user_context)
        
        if self.policy_manager and self.health_registry:
            engine_health = await self.health_registry.get_all_health()
            policy = await self.policy_manager.evaluate_policies(
                tenant_id, features, engine_health
            )
            if policy:
                return self._apply_policy_decision(
                    policy, sql, tables, features, freshness_tolerance, cache_key, logical_plan
                )
        
        if self.adaptive_router and len(tables) > 0:
            recommendation = await self.adaptive_router.get_routing_recommendation(
                tables, freshness_tolerance
            )
            logger.debug(f"Adaptive routing recommendation: {recommendation}")
        
        candidates = self._evaluate_sources(
            tables, features, freshness_tolerance, tenant_id, is_batch_job
        )
        
        best = self._select_optimal(candidates, features)
        
        if best["strategy"] == "hybrid":
            rewritten = self._generate_hybrid_sql(parsed, best["split_time"], tables)
        elif best["strategy"] == "batch":
            rewritten = sql  # Spark handles the SQL directly
        else:
            rewritten = self._rewrite_for_source(parsed, best["source_type"], tables)
        
        decision = RoutingDecision(
            target_source=best["source_id"],
            target_source_type=best["source_type"],
            execution_strategy=ExecutionStrategy(best["strategy"]),
            freshness_expected_seconds=best["actual_freshness"],
            estimated_cost_ms=best["predicted_cost"],
            confidence=best["confidence"],
            cache_key=cache_key,
            rewritten_sql=rewritten,
            fallback_source=best.get("fallback"),
            hybrid_config=best.get("hybrid_config"),
            reason=best["reason"],
            query_features=features,
            logical_plan=logical_plan,
            is_batch_job=is_batch_job
        )

        # GPU acceleration check — override engine selection for eligible operations
        if self.gpu_router is not None:
            try:
                engine_health = (
                    await self.health_registry.get_all_health()
                    if self.health_registry
                    else {}
                )
                if self.gpu_router.should_use_gpu(features, engine_health=engine_health):
                    logger.info(
                        "QueryRouter: GPU dispatch eligible for tenant=%s rows=%s",
                        tenant_id,
                        features.get("estimated_rows", "unknown"),
                    )
                    result, engine_used = await self.gpu_router.route_with_gpu_fallback(
                        data=None,
                        features=features,
                        tenant_id=tenant_id,
                    )
                    decision.target_source_type = engine_used
                    decision.reason = f"GPU-accelerated ({engine_used})"
            except Exception as exc:
                logger.error(
                    "QueryRouter GPU routing failed tenant=%s: %s — falling through to %s",
                    tenant_id,
                    exc,
                    decision.target_source_type,
                )
        
        logger.info(
            f"Routed query to {decision.target_source} "
            f"(strategy={decision.execution_strategy.value}, "
            f"freshness={decision.freshness_expected_seconds}s, "
            f"est_cost={decision.estimated_cost_ms:.1f}ms, "
            f"batch={decision.is_batch_job})"
        )
        
        return decision
    
    def _extract_tables(self, parsed: Expression) -> List[str]:
        """Extract table names from parsed SQL."""
        tables = []
        for table in parsed.find_all(sqlglot.exp.Table):
            table_name = table.name
            if table.db:
                table_name = f"{table.db}.{table_name}"
            tables.append(table_name)
        return list(set(tables))
    
    def _extract_features(
        self,
        parsed: Expression,
        tables: List[str]
    ) -> Dict[str, Any]:
        """Extract query features; delegates catalog look-up inline."""
        features = {
            "num_tables": len(tables),
            "tables": tables,
            "has_join": bool(parsed.find(sqlglot.exp.Join)),
            "has_aggregate": bool(parsed.find(sqlglot.exp.AggFunc)),
            "has_where": bool(parsed.find(sqlglot.exp.Where)),
            "has_group_by": bool(parsed.find(sqlglot.exp.Group)),
            "has_order_by": bool(parsed.find(sqlglot.exp.Order)),
            "has_limit": bool(parsed.find(sqlglot.exp.Limit)),
            "has_subquery": bool(parsed.find(sqlglot.exp.Subquery)),
            "query_length": len(parsed.sql()),
        }
        features["num_joins"] = len(list(parsed.find_all(sqlglot.exp.Join)))
        features["num_aggregates"] = len(list(parsed.find_all(sqlglot.exp.AggFunc)))
        features["complexity_score"] = (
            features["num_tables"] * 2
            + features["num_joins"] * 3
            + features["num_aggregates"] * 2
            + (1 if features["has_subquery"] else 0) * 5
        )
        total_rows = 0
        for table in tables:
            try:
                table_meta = self.catalog.get_table(table)
                if table_meta and table_meta.row_count:
                    total_rows += table_meta.row_count
            except Exception as exc:
                logger.error(
                    "QueryRouter._extract_features: catalog lookup failed table=%s: %s",
                    table, exc,
                )
        features["total_table_rows"] = total_rows
        features["estimated_rows"] = total_rows
        return features

    def _infer_freshness(
        self,
        parsed: Expression,
        user_context: Dict[str, Any]
    ) -> int:
        """Infer freshness requirements from query and context."""
        if "freshness_tolerance_seconds" in user_context:
            return user_context["freshness_tolerance_seconds"]
        where_clause = parsed.find(sqlglot.exp.Where)
        if where_clause:
            where_sql = where_clause.sql().upper()
            if any(kw in where_sql for kw in ["NOW()", "CURRENT", "TODAY", "SYSDATE"]):
                return self.FRESHNESS_LEVELS["recent"]
        return self.FRESHNESS_LEVELS["standard"]

    def _evaluate_sources(
        self,
        tables: List[str],
        features: Dict[str, Any],
        freshness_tolerance: int,
        tenant_id: str,
        is_batch_job: bool
    ) -> List[Dict]:
        """Evaluate Oracle vs S3 vs Spark vs Hybrid candidates."""
        candidates = []
        
        if is_batch_job and self.spark_engine:
            spark_cost = self._predict_cost(features, "spark")
            candidates.append({
                "source_id": f"{tenant_id}_spark",
                "source_type": "spark",
                "strategy": "batch",
                "actual_freshness": 0,  # Spark reads from S3
                "predicted_cost": spark_cost,
                "confidence": 0.8,
                "reason": f"Batch job detected ({features.get('estimated_rows', 0):,} rows, {features.get('num_joins', 0)} joins)"
            })
        
        oracle_cost = self._predict_cost(features, "oracle")
        candidates.append({
            "source_id": f"{tenant_id}_oracle",
            "source_type": "oracle",
            "strategy": "direct",
            "actual_freshness": 0,
            "predicted_cost": oracle_cost,
            "confidence": 0.9,
            "reason": "Source of truth, real-time data"
        })
        
        if tables:
            table_lags = {
                table: self.cdc.get_lag(table, "s3")
                for table in tables
            }
            max_lag = max(table_lags.values())
            s3_fresh = all(lag <= freshness_tolerance for lag in table_lags.values())
        else:
            max_lag = 0
            s3_fresh = True
        
        if s3_fresh:
            s3_cost = self._predict_cost(features, "s3_iceberg")
            candidates.append({
                "source_id": f"{tenant_id}_s3",
                "source_type": "trino",
                "strategy": "direct",
                "actual_freshness": max_lag,
                "predicted_cost": s3_cost,
                "confidence": 0.85,
                "reason": f"CDC lag acceptable ({max_lag}s)"
            })
        else:
            # Offer hybrid: recent from Oracle, history from S3
            hybrid_cost = self._estimate_hybrid_cost(features, max_lag)
            split_time = max_lag + 60  # Add buffer
            candidates.append({
                "source_id": f"{tenant_id}_s3",
                "source_type": "trino",
                "strategy": "hybrid",
                "actual_freshness": 0,  # Oracle part covers recent
                "predicted_cost": hybrid_cost,
                "confidence": 0.8,
                "split_time": f"NOW() - INTERVAL '{split_time}' SECOND",
                "fallback": f"{tenant_id}_oracle",
                "hybrid_config": {
                    "oracle_time_window": f"last {split_time}s",
                    "s3_time_window": f"before {split_time}s"
                },
                "reason": f"Hybrid: Oracle(last {split_time}s) + S3(history)"
            })
        
        return candidates
    
    def _predict_cost(self, features: Dict[str, Any], source: str) -> float:
        """Predict query cost for a source."""
        if self.cost_model:
            try:
                return self.cost_model.predict(features, source)
            except Exception as e:
                logger.warning(f"Cost model prediction failed: {e}, using heuristic")
        
        base_cost = 100.0  # Base 100ms
        
        if source == "oracle":
            base_cost *= 3.0
        elif source == "spark":
            base_cost *= 5.0  # Higher overhead for Spark
        
        complexity = features.get("complexity_score", 0)
        base_cost += complexity * 10
        
        return base_cost
    
    def _estimate_hybrid_cost(
        self,
        features: Dict[str, Any],
        max_lag: int
    ) -> float:
        """Estimate cost for hybrid execution."""
        oracle_cost = self._predict_cost(features, "oracle") * 0.3
        s3_cost = self._predict_cost(features, "s3_iceberg") * 0.9
        union_overhead = 50
        
        return oracle_cost + s3_cost + union_overhead
    
    def _select_optimal(
        self,
        candidates: List[Dict],
        features: Dict[str, Any]
    ) -> Dict:
        """
        Score candidates: cost + freshness_penalty + confidence_bonus.
        
        Args:
            candidates: List of candidate sources
            features: Query features
            
        Returns:
            Best candidate
        """
        scored = []
        for c in candidates:
            freshness_penalty = c["actual_freshness"] * 10
            confidence_bonus = c["predicted_cost"] * (c["confidence"] - 0.5) * -0.1
            
            if c["source_type"] == "oracle" and features.get("complexity_score", 0) > 10:
                complexity_penalty = c["predicted_cost"] * 0.2
            else:
                complexity_penalty = 0
            
            score = (
                c["predicted_cost"] +
                freshness_penalty +
                confidence_bonus +
                complexity_penalty
            )
            
            scored.append({**c, "score": score})
        
        return min(scored, key=lambda x: x["score"])
    
    def _generate_hybrid_sql(self, parsed: Expression, split_time: str, tables: List[str]) -> str:
        """Delegate hybrid SQL generation."""
        from metamind.core.routing_decision import generate_hybrid_sql
        return generate_hybrid_sql(parsed, split_time, tables)

    def _rewrite_for_source(self, parsed: Expression, source_type: str, tables: List[str]) -> str:
        """Delegate SQL dialect rewrite."""
        from metamind.core.routing_decision import rewrite_for_source
        return rewrite_for_source(parsed, source_type, tables)

    def _find_time_column(self, parsed: Expression, tables: List[str]) -> str:
        """Delegate time column discovery."""
        from metamind.core.routing_decision import find_time_column
        return find_time_column(parsed, tables)

    def _compute_cache_key(self, sql: str, tenant_id: str) -> str:
        """SHA256 hash of normalized SQL + tenant."""
        import hashlib
        normalized = sql.lower().strip()
        return hashlib.sha256(f"{tenant_id}:{normalized}".encode()).hexdigest()[:32]

    def _is_cache_stale(self, cached: Dict[str, Any], user_context: Dict[str, Any]) -> bool:
        """Check if cached result is stale."""
        if user_context.get("freshness_tolerance_seconds", 300) == 0:
            return True
        cached_at = cached.get("cached_at")
        if cached_at:
            from datetime import datetime
            age_seconds = (datetime.now() - cached_at).total_seconds()
            if age_seconds > 300:
                return True
        return False

    def _cache_hit_decision(self, cached: Dict[str, Any], cache_key: str) -> RoutingDecision:
        """Create routing decision for cache hit."""
        return RoutingDecision(
            target_source="cache", target_source_type="cache",
            execution_strategy=ExecutionStrategy.CACHED,
            freshness_expected_seconds=0, estimated_cost_ms=10.0, confidence=1.0,
            cache_key=cache_key, rewritten_sql="-- cached result",
            fallback_source=None, hybrid_config=None,
            reason="Cache hit - returning cached result",
        )

    def _apply_policy_decision(
        self, policy: Any, sql: str, tables: List[str],
        features: Dict[str, Any], freshness_tolerance: int,
        cache_key: str, logical_plan: Any
    ) -> RoutingDecision:
        """Apply routing policy decision."""
        return RoutingDecision(
            target_source=policy.target_engine,
            target_source_type=policy.target_engine.split("_")[-1],
            execution_strategy=ExecutionStrategy.DIRECT,
            freshness_expected_seconds=freshness_tolerance,
            estimated_cost_ms=0, confidence=0.9,
            cache_key=cache_key, rewritten_sql=sql,
            fallback_source=getattr(policy, "fallback_engine", None),
            hybrid_config=None,
            reason=f"Applied routing policy: {getattr(policy, 'name', 'unknown')}",
            query_features=features, logical_plan=logical_plan,
        )
