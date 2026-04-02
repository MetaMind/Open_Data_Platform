"""Mid-query re-optimization."""
from __future__ import annotations
import logging
from metamind.core.logical.nodes import LogicalNode
logger = logging.getLogger(__name__)
class Reoptimizer:
    def should_reoptimize(self, estimated: float, actual: float) -> bool:
        ratio = max(estimated, actual) / max(1.0, min(estimated, actual))
        return ratio > 10.0
    def reoptimize(self, node: LogicalNode, actual_rows: float) -> LogicalNode:
        node.estimated_rows = actual_rows
        logger.info("Re-optimizing with actual_rows=%.0f", actual_rows)
        return node
