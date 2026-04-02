"""
Plan Feature Extractor — ML-Ready Feature Vectors from Logical Plans

File: metamind/synthesis/plan_feature_extractor.py
Role: ML Engineer
Dependencies: metamind.core.logical.planner, metamind.ml.feature_store

Converts a logical plan dictionary (as produced by CostBasedPlanner) and
optional table statistics into a PlanFeatures dataclass suitable for ML
model inference and training-dataset construction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data-transfer object
# ---------------------------------------------------------------------------

@dataclass
class PlanFeatures:
    """ML-ready feature vector derived from a logical query plan."""
    num_tables: int = 0
    num_joins: int = 0
    join_depth: int = 0               # longest join chain depth
    num_aggregates: int = 0
    num_filters: int = 0
    has_subquery: bool = False
    estimated_output_rows: float = 0.0
    scan_selectivity: float = 1.0     # 0–1; lower = more selective
    cross_engine_flag: bool = False   # query spans >1 engine
    partition_pruning_possible: bool = False
    has_window_function: bool = False
    num_sort_nodes: int = 0
    has_limit: bool = False
    avg_table_size_rows: float = 0.0
    complexity_score: float = 0.0     # derived composite score

    def to_dict(self) -> Dict[str, Any]:
        """Return feature dict suitable for model inference."""
        return {
            "num_tables": self.num_tables,
            "num_joins": self.num_joins,
            "join_depth": self.join_depth,
            "num_aggregates": self.num_aggregates,
            "num_filters": self.num_filters,
            "has_subquery": int(self.has_subquery),
            "estimated_output_rows": self.estimated_output_rows,
            "scan_selectivity": self.scan_selectivity,
            "cross_engine_flag": int(self.cross_engine_flag),
            "partition_pruning_possible": int(self.partition_pruning_possible),
            "has_window_function": int(self.has_window_function),
            "num_sort_nodes": self.num_sort_nodes,
            "has_limit": int(self.has_limit),
            "avg_table_size_rows": self.avg_table_size_rows,
            "complexity_score": self.complexity_score,
        }


# ---------------------------------------------------------------------------
# PlanFeatureExtractor
# ---------------------------------------------------------------------------

_NODE_TYPE_MAP = {
    "join": "join",
    "aggregate": "aggregate",
    "filter": "filter",
    "scan": "scan",
    "sort": "sort",
    "limit": "limit",
    "subquery": "subquery",
    "window": "window",
    "union": "union",
}

# Engine keywords that identify non-local engines in cross-engine plans
_REMOTE_ENGINE_MARKERS = {"trino", "spark", "s3", "iceberg", "bigquery", "synapse"}


class PlanFeatureExtractor:
    """
    Translates a logical plan tree (dict) into a flat PlanFeatures vector.

    The logical plan is expected to have the structure produced by
    ``metamind.core.logical.planner.CostBasedPlanner``:

    .. code-block:: python

        {
            "node_type": "join",
            "node_id": "n1",
            "children": [...],
            "table_name": "orders",
            "estimated_rows": 50000,
            "source_engine": "oracle",
            ...
        }

    Falls back gracefully when table_stats or plan dict keys are absent.
    """

    def extract(
        self,
        logical_plan: Dict[str, Any],
        table_stats: Optional[Dict[str, Any]] = None,
    ) -> PlanFeatures:
        """
        Walk the plan tree and accumulate feature counts.

        Parameters
        ----------
        logical_plan : dict
            Root node of the logical plan produced by CostBasedPlanner.
        table_stats : dict, optional
            Mapping of table_name → {"row_count": int, "partitioned": bool, ...}
            used to populate size/selectivity features.

        Returns
        -------
        PlanFeatures
            Populated feature vector.
        """
        if table_stats is None:
            table_stats = {}

        counter: Dict[str, int] = {
            "num_tables": 0,
            "num_joins": 0,
            "join_depth": 0,
            "num_aggregates": 0,
            "num_filters": 0,
            "num_sort_nodes": 0,
            "num_subqueries": 0,
            "num_windows": 0,
            "has_limit": 0,
        }
        engines_seen: List[str] = []
        table_names: List[str] = []
        row_estimates: List[float] = []

        self._walk(logical_plan, counter, engines_seen, table_names, row_estimates, depth=0)

        # Compute selectivity from min row estimate vs first scan estimate
        scan_selectivity = 1.0
        if len(row_estimates) >= 2:
            max_rows = max(row_estimates)
            min_rows = min(row_estimates)
            scan_selectivity = float(min_rows / max_rows) if max_rows > 0 else 1.0

        # Partition pruning: any table in table_stats marked as partitioned
        partition_pruning = any(
            table_stats.get(t, {}).get("partitioned", False) for t in table_names
        )

        # Average table size from stats
        sizes = [
            float(table_stats[t]["row_count"])
            for t in table_names
            if t in table_stats and "row_count" in table_stats[t]
        ]
        avg_size = float(sum(sizes) / len(sizes)) if sizes else 0.0

        estimated_output = float(logical_plan.get("estimated_rows", 0) or 0)

        # Cross-engine detection
        unique_engines = {e.lower() for e in engines_seen}
        cross_engine = bool(
            len(unique_engines) > 1
            or any(e in _REMOTE_ENGINE_MARKERS for e in unique_engines)
        )

        # Composite complexity score (heuristic, range 0–100)
        complexity = min(
            100.0,
            counter["num_joins"] * 10.0
            + counter["num_aggregates"] * 5.0
            + counter["num_filters"] * 2.0
            + counter["num_subqueries"] * 15.0
            + counter["join_depth"] * 8.0,
        )

        return PlanFeatures(
            num_tables=counter["num_tables"],
            num_joins=counter["num_joins"],
            join_depth=counter["join_depth"],
            num_aggregates=counter["num_aggregates"],
            num_filters=counter["num_filters"],
            has_subquery=counter["num_subqueries"] > 0,
            estimated_output_rows=estimated_output,
            scan_selectivity=round(scan_selectivity, 4),
            cross_engine_flag=cross_engine,
            partition_pruning_possible=partition_pruning,
            has_window_function=counter["num_windows"] > 0,
            num_sort_nodes=counter["num_sort_nodes"],
            has_limit=bool(counter["has_limit"]),
            avg_table_size_rows=avg_size,
            complexity_score=round(complexity, 2),
        )

    # ------------------------------------------------------------------
    # Internal traversal
    # ------------------------------------------------------------------

    def _walk(
        self,
        node: Dict[str, Any],
        counter: Dict[str, int],
        engines: List[str],
        tables: List[str],
        row_estimates: List[float],
        depth: int,
    ) -> None:
        """Recursively walk the plan tree accumulating counts."""
        if not isinstance(node, dict):
            return

        node_type = str(node.get("node_type", "")).lower()

        if node_type == "scan":
            counter["num_tables"] += 1
            tbl = node.get("table_name", "")
            if tbl:
                tables.append(tbl)
            src = node.get("source_engine", node.get("engine", ""))
            if src:
                engines.append(src)
            est = node.get("estimated_rows", node.get("row_count", 0))
            if est:
                row_estimates.append(float(est))

        elif node_type == "join":
            counter["num_joins"] += 1
            counter["join_depth"] = max(counter["join_depth"], depth)

        elif node_type == "aggregate":
            counter["num_aggregates"] += 1

        elif node_type == "filter":
            counter["num_filters"] += 1

        elif node_type == "sort":
            counter["num_sort_nodes"] += 1

        elif node_type == "limit":
            counter["has_limit"] = 1

        elif node_type == "subquery":
            counter["num_subqueries"] += 1

        elif node_type == "window":
            counter["num_windows"] += 1

        # Propagate estimated rows from non-scan nodes for selectivity
        if node_type not in ("scan",):
            est = node.get("estimated_rows", 0)
            if est:
                row_estimates.append(float(est))

        for child in node.get("children", []):
            self._walk(
                child,
                counter,
                engines,
                tables,
                row_estimates,
                depth + (1 if node_type == "join" else 0),
            )
