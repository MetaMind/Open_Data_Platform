"""F01 Enhanced feature extraction for learned cardinality models.

Extends the base CardinalityFeatureExtractor with additional features:
- Query-level features (join count, subquery depth, aggregation type)
- Cross-column correlation features
- Historical error features (how wrong were previous estimates)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

import numpy as np

from metamind.core.learned.hybrid_estimator import (
    CardinalityFeatureExtractor as BaseExtractor,
    CardinalityFeatures,
)
from metamind.core.logical.nodes import (
    LogicalNode, FilterNode, JoinNode, AggregateNode, ScanNode,
    Predicate, SortNode, LimitNode,
)
from metamind.core.metadata.models import TableMeta

logger = logging.getLogger(__name__)

# Re-export base class for backward compatibility
__all__ = ["CardinalityFeatureExtractor", "EnhancedFeatureExtractor", "CardinalityFeatures"]


@dataclass
class EnhancedFeatures:
    """Extended feature vector including query-structural features."""

    base_features: CardinalityFeatures
    join_count: int = 0
    subquery_depth: int = 0
    has_aggregation: bool = False
    has_sort: bool = False
    has_limit: bool = False
    limit_value: int = 0
    table_count: int = 1
    predicate_complexity: float = 0.0  # normalized complexity score
    historical_error_ratio: float = 1.0  # prev estimate / actual

    def to_array(self) -> np.ndarray:
        """Convert to numpy feature array (base features + extended)."""
        base = self.base_features.to_array()
        extended = np.array([
            self.join_count,
            self.subquery_depth,
            float(self.has_aggregation),
            float(self.has_sort),
            float(self.has_limit),
            np.log1p(self.limit_value),
            self.table_count,
            self.predicate_complexity,
            np.log1p(self.historical_error_ratio),
        ], dtype=np.float32)
        return np.concatenate([base, extended])


class EnhancedFeatureExtractor:
    """Extracts enriched feature vectors from logical plan trees.

    Combines base predicate features (from CardinalityFeatureExtractor)
    with structural query features for improved model accuracy.
    """

    def __init__(self, error_history: Optional[dict[str, float]] = None) -> None:
        """Initialize with optional historical error ratios per table."""
        self._base = BaseExtractor()
        self._error_history = error_history or {}

    def extract_from_plan(
        self,
        root: LogicalNode,
        table_meta_map: dict[str, TableMeta],
    ) -> list[EnhancedFeatures]:
        """Extract features from a full logical plan tree.

        Walks the plan tree, extracts features at each scan/filter boundary,
        and enriches with structural information.

        Args:
            root: Root of the logical plan tree.
            table_meta_map: Map of table_name -> TableMeta.

        Returns:
            List of EnhancedFeatures, one per scan node in the plan.
        """
        structural = self._analyze_structure(root)
        results: list[EnhancedFeatures] = []
        self._walk(root, table_meta_map, structural, results, depth=0)
        return results

    def extract(
        self,
        predicates: list[Predicate],
        table_meta: TableMeta,
        plan_context: Optional[dict[str, Any]] = None,
    ) -> EnhancedFeatures:
        """Extract enhanced features for a single table with predicates.

        Args:
            predicates: Filter predicates for this table.
            table_meta: Table metadata.
            plan_context: Optional structural context from plan analysis.

        Returns:
            EnhancedFeatures instance.
        """
        base = self._base.extract(predicates, table_meta)
        ctx = plan_context or {}

        # Compute predicate complexity
        complexity = 0.0
        for p in predicates:
            if p.operator in ("IN",):
                complexity += 2.0
            elif p.operator in ("<", ">", "<=", ">=", "BETWEEN"):
                complexity += 1.5
            elif p.operator in ("LIKE",):
                complexity += 3.0
            else:
                complexity += 1.0
        complexity /= max(1, len(predicates))

        error_ratio = self._error_history.get(table_meta.table_name, 1.0)

        return EnhancedFeatures(
            base_features=base,
            join_count=ctx.get("join_count", 0),
            subquery_depth=ctx.get("subquery_depth", 0),
            has_aggregation=ctx.get("has_aggregation", False),
            has_sort=ctx.get("has_sort", False),
            has_limit=ctx.get("has_limit", False),
            limit_value=ctx.get("limit_value", 0),
            table_count=ctx.get("table_count", 1),
            predicate_complexity=complexity,
            historical_error_ratio=error_ratio,
        )

    def _analyze_structure(self, root: LogicalNode) -> dict[str, Any]:
        """Analyze structural properties of the plan tree."""
        context: dict[str, Any] = {
            "join_count": 0,
            "subquery_depth": 0,
            "has_aggregation": False,
            "has_sort": False,
            "has_limit": False,
            "limit_value": 0,
            "table_count": 0,
        }
        self._count_structure(root, context, depth=0)
        return context

    def _count_structure(
        self, node: LogicalNode, ctx: dict[str, Any], depth: int
    ) -> None:
        """Recursively count structural elements."""
        if isinstance(node, JoinNode):
            ctx["join_count"] += 1
        elif isinstance(node, AggregateNode):
            ctx["has_aggregation"] = True
        elif isinstance(node, SortNode):
            ctx["has_sort"] = True
        elif isinstance(node, LimitNode):
            ctx["has_limit"] = True
            ctx["limit_value"] = node.limit
        elif isinstance(node, ScanNode):
            ctx["table_count"] += 1

        ctx["subquery_depth"] = max(ctx["subquery_depth"], depth)

        for child in node.children:
            self._count_structure(child, ctx, depth + 1)

    def _walk(
        self,
        node: LogicalNode,
        meta_map: dict[str, TableMeta],
        structural: dict[str, Any],
        results: list[EnhancedFeatures],
        depth: int,
    ) -> None:
        """Walk plan tree extracting features at scan+filter boundaries."""
        if isinstance(node, FilterNode):
            # Check if child is a scan (common pattern: scan -> filter)
            child = node.child
            if isinstance(child, ScanNode) and child.table_name in meta_map:
                features = self.extract(
                    node.predicates,
                    meta_map[child.table_name],
                    structural,
                )
                results.append(features)

        elif isinstance(node, ScanNode):
            if node.table_name in meta_map:
                features = self.extract([], meta_map[node.table_name], structural)
                results.append(features)

        for child in node.children:
            self._walk(child, meta_map, structural, results, depth + 1)

    def update_error_history(self, table: str, estimated: float, actual: float) -> None:
        """Update the historical error ratio for a table.

        Uses exponential moving average to smooth error tracking.
        """
        if actual <= 0:
            return
        ratio = estimated / actual
        alpha = 0.3  # EMA smoothing factor
        prev = self._error_history.get(table, 1.0)
        self._error_history[table] = alpha * ratio + (1 - alpha) * prev
