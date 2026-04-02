"""
Feature Store for ML

File: metamind/ml/feature_store.py
Role: ML Engineer
Phase: 1
Dependencies: Redis, PostgreSQL

Stores and retrieves features for ML models:
- Query shape features
- Historical performance metrics
- Engine load metrics
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)


@dataclass
class QueryShapeFeatures:
    """Query shape features for ML."""
    query_fingerprint: str
    num_tables: int
    num_joins: int
    num_aggregates: int
    num_filters: int
    has_subquery: bool
    has_group_by: bool
    has_order_by: bool
    has_limit: bool
    query_length: int
    complexity_score: int
    estimated_cardinality: int
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class HistoricalPerformanceMetrics:
    """Historical performance metrics for queries."""
    query_fingerprint: str
    engine: str
    avg_execution_time_ms: float
    p50_execution_time_ms: float
    p95_execution_time_ms: float
    p99_execution_time_ms: float
    success_rate: float
    cache_hit_rate: float
    sample_count: int
    last_executed: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "last_executed": self.last_executed.isoformat() if self.last_executed else None
        }


@dataclass
class EngineLoadMetrics:
    """Real-time engine load metrics."""
    engine_name: str
    timestamp: datetime
    active_queries: int
    queued_queries: int
    avg_cpu_percent: float
    avg_memory_percent: float
    network_io_mbps: float
    disk_io_mbps: float
    connection_pool_utilization: float
    error_rate: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "timestamp": self.timestamp.isoformat() if self.timestamp else None
        }


class FeatureStore:
    """
    Feature store for ML models.
    
    Provides:
    - Query shape features
    - Historical performance metrics
    - Engine load metrics
    """
    
    def __init__(
        self,
        redis_client: Any,
        db_engine: Any,
        feature_ttl_seconds: int = 3600
    ):
        """
        Initialize feature store.
        
        Args:
            redis_client: Redis client for caching
            db_engine: Database engine for persistence
            feature_ttl_seconds: TTL for cached features
        """
        self.redis = redis_client
        self.db_engine = db_engine
        self.feature_ttl = feature_ttl_seconds
        logger.debug("FeatureStore initialized")
    
    def compute_query_fingerprint(self, sql: str) -> str:
        """
        Compute query fingerprint for feature lookup.
        
        Args:
            sql: SQL query
            
        Returns:
            Query fingerprint hash
        """
        # Normalize SQL
        normalized = sql.lower().strip()
        normalized = " ".join(normalized.split())  # Remove extra whitespace
        
        # Hash
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]
    
    async def get_query_shape_features(
        self,
        sql: str,
        use_cache: bool = True
    ) -> Optional[QueryShapeFeatures]:
        """
        Get query shape features.
        
        Args:
            sql: SQL query
            use_cache: Whether to use cache
            
        Returns:
            Query shape features or None
        """
        fingerprint = self.compute_query_fingerprint(sql)
        
        # Try cache first
        if use_cache:
            cached = await self.redis.get(f"features:shape:{fingerprint}")
            if cached:
                return QueryShapeFeatures(**json.loads(cached))
        
        # Compute features
        features = self._compute_shape_features(sql, fingerprint)
        
        # Cache
        if use_cache:
            await self.redis.setex(
                f"features:shape:{fingerprint}",
                self.feature_ttl,
                json.dumps(features.to_dict())
            )
        
        return features
    
    def _compute_shape_features(
        self,
        sql: str,
        fingerprint: str
    ) -> QueryShapeFeatures:
        """Compute query shape features from SQL."""
        try:
            import sqlglot
            parsed = sqlglot.parse_one(sql)
            
            # Count features
            num_tables = len(list(parsed.find_all(sqlglot.exp.Table)))
            num_joins = len(list(parsed.find_all(sqlglot.exp.Join)))
            num_aggregates = len(list(parsed.find_all(sqlglot.exp.AggFunc)))
            num_filters = len(list(parsed.find_all(sqlglot.exp.Where)))
            
            has_subquery = bool(parsed.find(sqlglot.exp.Subquery))
            has_group_by = bool(parsed.find(sqlglot.exp.Group))
            has_order_by = bool(parsed.find(sqlglot.exp.Order))
            has_limit = bool(parsed.find(sqlglot.exp.Limit))
            
            complexity_score = (
                num_tables * 2 +
                num_joins * 3 +
                num_aggregates * 2 +
                (5 if has_subquery else 0)
            )
            
            return QueryShapeFeatures(
                query_fingerprint=fingerprint,
                num_tables=num_tables,
                num_joins=num_joins,
                num_aggregates=num_aggregates,
                num_filters=num_filters,
                has_subquery=has_subquery,
                has_group_by=has_group_by,
                has_order_by=has_order_by,
                has_limit=has_limit,
                query_length=len(sql),
                complexity_score=complexity_score,
                estimated_cardinality=0  # Will be filled by planner
            )
            
        except Exception as e:
            logger.warning(f"Failed to compute shape features: {e}")
            return QueryShapeFeatures(
                query_fingerprint=fingerprint,
                num_tables=0,
                num_joins=0,
                num_aggregates=0,
                num_filters=0,
                has_subquery=False,
                has_group_by=False,
                has_order_by=False,
                has_limit=False,
                query_length=len(sql),
                complexity_score=0,
                estimated_cardinality=0
            )
    
    async def get_historical_performance(
        self,
        query_fingerprint: str,
        engine: str,
        lookback_days: int = 7
    ) -> Optional[HistoricalPerformanceMetrics]:
        """
        Get historical performance metrics for a query.
        
        Args:
            query_fingerprint: Query fingerprint
            engine: Engine name
            lookback_days: Days to look back
            
        Returns:
            Historical performance metrics or None
        """
        cache_key = f"features:perf:{engine}:{query_fingerprint}"
        
        # Try cache
        cached = await self.redis.get(cache_key)
        if cached:
            data = json.loads(cached)
            data["last_executed"] = datetime.fromisoformat(data["last_executed"])
            return HistoricalPerformanceMetrics(**data)
        
        # Query database
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(
                    text("""
                    SELECT 
                        AVG(total_time_ms) as avg_time,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_time_ms) as p50,
                        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_time_ms) as p95,
                        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_time_ms) as p99,
                        AVG(CASE WHEN status = 'success' THEN 1.0 ELSE 0.0 END) as success_rate,
                        AVG(CASE WHEN cache_hit THEN 1.0 ELSE 0.0 END) as cache_hit_rate,
                        COUNT(*) as sample_count,
                        MAX(submitted_at) as last_executed
                    FROM mm_query_logs
                    WHERE query_features->>'query_fingerprint' = :fingerprint
                    AND target_source LIKE :engine_pattern
                    AND submitted_at > NOW() - (:days * INTERVAL '1 day')
                    """),
                    {
                        "fingerprint": query_fingerprint,
                        "engine_pattern": f"%{engine}%",
                        "days": lookback_days
                    }
                ).fetchone()
                
                if result and result.sample_count > 0:
                    metrics = HistoricalPerformanceMetrics(
                        query_fingerprint=query_fingerprint,
                        engine=engine,
                        avg_execution_time_ms=float(result.avg_time or 0),
                        p50_execution_time_ms=float(result.p50 or 0),
                        p95_execution_time_ms=float(result.p95 or 0),
                        p99_execution_time_ms=float(result.p99 or 0),
                        success_rate=float(result.success_rate or 0),
                        cache_hit_rate=float(result.cache_hit_rate or 0),
                        sample_count=result.sample_count,
                        last_executed=result.last_executed
                    )
                    
                    # Cache
                    await self.redis.setex(
                        cache_key,
                        300,  # 5 minute TTL for performance metrics
                        json.dumps(metrics.to_dict())
                    )
                    
                    return metrics
                    
        except Exception as e:
            logger.warning(f"Failed to get historical performance: {e}")
        
        return None
    
    async def get_engine_load(self, engine: str) -> Optional[EngineLoadMetrics]:
        """
        Get current engine load metrics.
        
        Args:
            engine: Engine name
            
        Returns:
            Engine load metrics or None
        """
        cache_key = f"features:load:{engine}"
        
        # Try cache (short TTL for real-time data)
        cached = await self.redis.get(cache_key)
        if cached:
            data = json.loads(cached)
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])
            return EngineLoadMetrics(**data)
        
        return None
    
    async def update_engine_load(
        self,
        engine: str,
        metrics: EngineLoadMetrics
    ) -> None:
        """
        Update engine load metrics.
        
        Args:
            engine: Engine name
            metrics: Load metrics
        """
        cache_key = f"features:load:{engine}"
        
        await self.redis.setex(
            cache_key,
            60,  # 1 minute TTL
            json.dumps(metrics.to_dict())
        )
    
    async def get_all_features_for_prediction(
        self,
        sql: str,
        engine: str
    ) -> Dict[str, Any]:
        """
        Get all features needed for cost prediction.
        
        Args:
            sql: SQL query
            engine: Target engine
            
        Returns:
            Combined feature dictionary
        """
        fingerprint = self.compute_query_fingerprint(sql)
        
        # Get shape features
        shape = await self.get_query_shape_features(sql)
        
        # Get historical performance
        historical = await self.get_historical_performance(fingerprint, engine)
        
        # Get engine load
        load = await self.get_engine_load(engine)
        
        features = {}
        
        if shape:
            features.update({
                "num_tables": shape.num_tables,
                "num_joins": shape.num_joins,
                "num_aggregates": shape.num_aggregates,
                "has_subquery": shape.has_subquery,
                "has_group_by": shape.has_group_by,
                "has_order_by": shape.has_order_by,
                "has_limit": shape.has_limit,
                "complexity_score": shape.complexity_score
            })
        
        if historical:
            features.update({
                "historical_avg_time_ms": historical.avg_execution_time_ms,
                "historical_p95_time_ms": historical.p95_execution_time_ms,
                "historical_success_rate": historical.success_rate,
                "historical_cache_hit_rate": historical.cache_hit_rate,
                "historical_samples": historical.sample_count
            })
        
        if load:
            features.update({
                "engine_active_queries": load.active_queries,
                "engine_cpu_percent": load.avg_cpu_percent,
                "engine_memory_percent": load.avg_memory_percent,
                "engine_error_rate": load.error_rate
            })
        
        return features
    
    async def record_query_execution(
        self,
        query_fingerprint: str,
        engine: str,
        execution_time_ms: int,
        status: str,
        cache_hit: bool
    ) -> None:
        """
        Record query execution for historical tracking.
        
        Args:
            query_fingerprint: Query fingerprint
            engine: Engine used
            execution_time_ms: Execution time
            status: Query status
            cache_hit: Whether cache was hit
        """
        # Invalidate cached performance metrics
        cache_key = f"features:perf:{engine}:{query_fingerprint}"
        await self.redis.delete(cache_key)
        
        logger.debug(f"Recorded execution: {query_fingerprint} on {engine}")
