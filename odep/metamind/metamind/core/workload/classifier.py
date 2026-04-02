"""F24 — Workload Classification and Routing."""
from __future__ import annotations

import logging
import re
from enum import Enum
from typing import Optional

import numpy as np

from metamind.core.logical.nodes import (
    AggregateNode, JoinNode, LimitNode, LogicalNode, ScanNode, VectorSearchNode,
)

logger = logging.getLogger(__name__)


class WorkloadType(str, Enum):
    """ML-classified workload categories for routing decisions."""

    POINT_LOOKUP = "point_lookup"
    DASHBOARD_AGGREGATE = "dashboard"
    ADHOC_EXPLORATION = "exploration"
    ETL_PIPELINE = "etl"
    ML_FEATURE_EXTRACTION = "ml"
    VECTOR_SEARCH = "vector"


class WorkloadClassifier:
    """ML-based workload classifier using rule-based features + optional XGBoost.

    Classifies queries into WorkloadType for routing and optimization strategy.
    Falls back to rule-based classification when ML model not trained.
    """

    def __init__(self) -> None:
        """Initialize classifier. Model loaded lazily when available."""
        self._model: Optional[object] = None

    def classify(self, root: LogicalNode, sql: str = "") -> WorkloadType:
        """Classify a logical plan into a WorkloadType.

        Args:
            root: Root of logical plan tree
            sql: Original SQL string for pattern matching

        Returns:
            WorkloadType classification
        """
        features = self.extract_features(root, sql)

        # Rule-based classification (ML model overrides if available)
        if self._model is not None:
            try:
                pred = self._model.predict(features.reshape(1, -1))[0]  # type: ignore[union-attr]
                return WorkloadType(pred)
            except Exception as exc:
                logger.warning("ML classifier failed, using rules: %s", exc)

        return self._rule_classify(features, root, sql)

    def extract_features(self, root: LogicalNode, sql: str = "") -> np.ndarray:
        """Extract numerical feature vector from logical plan.

        Features:
        0: join_count
        1: agg_count
        2: scan_count
        3: has_limit
        4: has_vector_search
        5: predicate_count
        6: sql_length_bucket (0-4)
        7: has_order_by
        8: complexity_score
        """
        stats = self._collect_stats(root)
        sql_len = len(sql)
        sql_len_bucket = min(4, sql_len // 200)

        return np.array([
            stats["joins"],
            stats["aggs"],
            stats["scans"],
            float(stats["has_limit"]),
            float(stats["has_vector"]),
            stats["predicates"],
            float(sql_len_bucket),
            float(stats["has_sort"]),
            float(stats["joins"] * 2 + stats["aggs"] + stats["scans"]),
        ], dtype=np.float32)

    def _rule_classify(
        self, features: np.ndarray, root: LogicalNode, sql: str
    ) -> WorkloadType:
        """Rule-based fallback classification."""
        joins = int(features[0])
        aggs = int(features[1])
        scans = int(features[2])
        has_limit = bool(features[3])
        has_vector = bool(features[4])
        predicates = int(features[5])

        # Vector search
        if has_vector:
            return WorkloadType.VECTOR_SEARCH

        # Point lookup: single table, equality predicate, limit 1
        if scans == 1 and joins == 0 and predicates >= 1 and has_limit:
            return WorkloadType.POINT_LOOKUP

        # ETL: many scans, many joins, no limit, no aggregation
        if scans >= 3 and joins >= 2 and aggs == 0 and not has_limit:
            return WorkloadType.ETL_PIPELINE

        # ML feature extraction: many scans, aggregations, large result set
        if aggs >= 2 and scans >= 2 and joins >= 1:
            return WorkloadType.ML_FEATURE_EXTRACTION

        # Dashboard: aggregation present, moderate joins
        if aggs >= 1 and joins <= 4:
            return WorkloadType.DASHBOARD_AGGREGATE

        # Default: ad-hoc exploration
        return WorkloadType.ADHOC_EXPLORATION

    def _collect_stats(self, node: LogicalNode) -> dict[str, object]:
        """Collect plan tree statistics for feature extraction."""
        stats: dict[str, object] = {
            "joins": 0, "aggs": 0, "scans": 0, "predicates": 0,
            "has_limit": False, "has_vector": False, "has_sort": False,
        }
        self._collect_recursive(node, stats)
        return stats

    def _collect_recursive(self, node: LogicalNode, stats: dict[str, object]) -> None:
        """Recursively collect plan stats."""
        if isinstance(node, JoinNode):
            stats["joins"] = int(stats["joins"]) + 1  # type: ignore[arg-type]
        elif isinstance(node, AggregateNode):
            stats["aggs"] = int(stats["aggs"]) + 1  # type: ignore[arg-type]
            stats["predicates"] = int(stats["predicates"]) + len(node.having)  # type: ignore[arg-type]
        elif isinstance(node, ScanNode):
            stats["scans"] = int(stats["scans"]) + 1  # type: ignore[arg-type]
            stats["predicates"] = int(stats["predicates"]) + len(node.predicates)  # type: ignore[arg-type]
        elif isinstance(node, LimitNode):
            stats["has_limit"] = True
        elif isinstance(node, VectorSearchNode):
            stats["has_vector"] = True

        for child in node.children:
            self._collect_recursive(child, stats)


class WorkloadRouter:
    """Routes queries to optimal backend based on workload classification."""

    # Default routing strategy per workload type
    _ROUTING_PREFERENCES: dict[WorkloadType, list[str]] = {
        WorkloadType.POINT_LOOKUP: ["postgres", "duckdb"],
        WorkloadType.DASHBOARD_AGGREGATE: ["duckdb", "spark", "snowflake", "bigquery"],
        WorkloadType.ADHOC_EXPLORATION: ["duckdb", "postgres"],
        WorkloadType.ETL_PIPELINE: ["spark", "flink", "snowflake"],
        WorkloadType.ML_FEATURE_EXTRACTION: ["spark", "bigquery", "duckdb"],
        WorkloadType.VECTOR_SEARCH: ["pgvector", "lance", "duckdb"],
    }

    def route(
        self,
        workload_type: WorkloadType,
        available_backends: list[str],
        cost_hints: Optional[dict[str, float]] = None,
    ) -> str:
        """Select optimal backend for workload type from available backends.

        Args:
            workload_type: Classified workload type
            available_backends: List of available backend IDs
            cost_hints: Optional cost estimates per backend

        Returns:
            Selected backend ID
        """
        preferences = self._ROUTING_PREFERENCES.get(workload_type, [])

        # If cost hints provided, pick cheapest
        if cost_hints and available_backends:
            valid = {b: cost_hints[b] for b in available_backends if b in cost_hints}
            if valid:
                selected = min(valid, key=lambda b: valid[b])
                logger.debug("Cost-based routing: %s → %s", workload_type.value, selected)
                return selected

        # Preference-based routing
        for preferred in preferences:
            for backend in available_backends:
                if preferred in backend.lower():
                    logger.debug("Preference routing: %s → %s", workload_type.value, backend)
                    return backend

        # Fallback: first available
        if available_backends:
            logger.debug("Fallback routing: %s → %s", workload_type.value, available_backends[0])
            return available_backends[0]

        raise ValueError(f"No available backends for workload type {workload_type.value}")

    def get_optimization_strategy(self, workload_type: WorkloadType) -> dict[str, object]:
        """Return optimization parameters for a workload type."""
        strategies: dict[WorkloadType, dict[str, object]] = {
            WorkloadType.POINT_LOOKUP: {
                "use_index": True, "enable_cache": True,
                "optimization_tier": 1, "timeout_seconds": 5,
            },
            WorkloadType.DASHBOARD_AGGREGATE: {
                "use_mv": True, "enable_cache": True,
                "optimization_tier": 2, "timeout_seconds": 30,
            },
            WorkloadType.ADHOC_EXPLORATION: {
                "use_mv": False, "enable_cache": False,
                "optimization_tier": 2, "timeout_seconds": 120,
            },
            WorkloadType.ETL_PIPELINE: {
                "use_mv": False, "enable_cache": False,
                "optimization_tier": 3, "timeout_seconds": 3600,
            },
            WorkloadType.ML_FEATURE_EXTRACTION: {
                "use_mv": True, "enable_cache": True,
                "optimization_tier": 3, "timeout_seconds": 600,
            },
            WorkloadType.VECTOR_SEARCH: {
                "use_index": True, "enable_cache": True,
                "optimization_tier": 1, "timeout_seconds": 10,
            },
        }
        return strategies.get(workload_type, {"optimization_tier": 2, "timeout_seconds": 60})
