"""Cascades-style query optimizer with memo group management."""
from __future__ import annotations

import logging
from typing import Any, Optional

from metamind.core.catalog.metadata import MetadataCatalog
from metamind.core.cost.model import CostModel
from metamind.core.types import CostVector, LogicalNode

logger = logging.getLogger(__name__)


class CascadesOptimizer:
    """Cascades-style top-down query optimizer with branch-and-bound pruning."""

    def __init__(
        self,
        catalog: MetadataCatalog,
        cost_model: CostModel,
        tenant_id: str,
    ) -> None:
        self.catalog = catalog
        self.cost_model = cost_model
        self.tenant_id = tenant_id
        self._memo: dict[str, list[LogicalNode]] = {}

    def optimize(self, plan: LogicalNode) -> LogicalNode:
        """Optimize a logical plan using branch-and-bound."""
        best = self._optimize_node(plan)
        logger.debug(
            "Optimization complete: cost=%.2f rows=%.0f",
            best.estimated_cost,
            best.estimated_rows,
        )
        return best

    def _optimize_node(self, node: LogicalNode) -> LogicalNode:
        optimized_children = [self._optimize_node(c) for c in node.children]
        node.children = optimized_children

        alternatives = self._generate_alternatives(node)
        best = node
        best_cost = self._compute_cost(node)

        for alt in alternatives:
            alt_cost = self._compute_cost(alt)
            if alt_cost < best_cost:
                best = alt
                best_cost = alt_cost

        best.estimated_cost = best_cost
        return best

    def _generate_alternatives(self, node: LogicalNode) -> list[LogicalNode]:
        alternatives: list[LogicalNode] = []
        if node.node_type == "SeqScan":
            table_name = node.properties.get("table", "")
            indexes = self.catalog.get_indexes(self.tenant_id, table_name)
            for idx in indexes:
                alt = LogicalNode(
                    node_type="IndexScan",
                    children=[],
                    properties={**node.properties, "index": idx.index_name},
                    estimated_rows=node.estimated_rows * 0.1,
                )
                alternatives.append(alt)
        return alternatives

    def _compute_cost(self, node: LogicalNode) -> float:
        table_name = node.properties.get("table", "")
        stats = self.catalog.get_statistics(self.tenant_id, table_name)
        if not stats:
            stats = {"row_count": max(node.estimated_rows, 1000)}
        cv = self.cost_model.estimate(node, stats)
        child_cost = sum(c.estimated_cost for c in node.children)
        return self.cost_model.total_cost(cv) + child_cost


def create_optimizer(
    catalog: MetadataCatalog,
    cost_model: Optional[CostModel] = None,
    tenant_id: str = "default",
) -> CascadesOptimizer:
    """Factory function for creating optimizers."""
    if cost_model is None:
        cost_model = CostModel()
    return CascadesOptimizer(catalog=catalog, cost_model=cost_model, tenant_id=tenant_id)
