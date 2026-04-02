"""Cost model for query plan cost estimation."""
from __future__ import annotations

import logging
import math
from typing import Any, Optional

from metamind.core.types import CostVector, CostWeights, LogicalNode

logger = logging.getLogger(__name__)


class CostModel:
    """Multi-objective cost model for query plan estimation."""

    def __init__(self, weights: Optional[CostWeights] = None) -> None:
        self.weights = weights or CostWeights()

    def estimate(self, node: LogicalNode, stats: dict[str, Any]) -> CostVector:
        node_type = node.node_type
        row_count = stats.get("row_count", 1000)
        col_count = stats.get("col_count", 10)
        avg_width = stats.get("avg_width", 50)

        if node_type == "SeqScan":
            return CostVector(
                cpu=row_count * 0.01,
                io=row_count * avg_width / 8192.0,
            )
        elif node_type == "IndexScan":
            selectivity = stats.get("selectivity", 0.1)
            selected_rows = row_count * selectivity
            return CostVector(
                cpu=selected_rows * 0.02,
                io=math.log2(max(row_count, 1)) + selected_rows * avg_width / 8192.0,
            )
        elif node_type == "HashJoin":
            left_rows = stats.get("left_rows", row_count)
            right_rows = stats.get("right_rows", row_count)
            return CostVector(
                cpu=(left_rows + right_rows) * 0.02,
                io=left_rows * avg_width / 8192.0,
                memory=right_rows * avg_width,
            )
        elif node_type == "Sort":
            return CostVector(
                cpu=row_count * math.log2(max(row_count, 1)) * 0.05,
                memory=row_count * avg_width,
            )
        elif node_type == "Aggregate":
            return CostVector(cpu=row_count * 0.03)
        else:
            return CostVector(cpu=row_count * 0.01)

    def total_cost(self, cost_vector: CostVector) -> float:
        return (
            cost_vector.cpu * self.weights.cpu_weight
            + cost_vector.io * self.weights.io_weight
            + cost_vector.network * self.weights.network_weight
            + cost_vector.memory * self.weights.memory_weight
        )

    def compare(self, a: CostVector, b: CostVector) -> int:
        total_a = self.total_cost(a)
        total_b = self.total_cost(b)
        if total_a < total_b:
            return -1
        elif total_a > total_b:
            return 1
        return 0
