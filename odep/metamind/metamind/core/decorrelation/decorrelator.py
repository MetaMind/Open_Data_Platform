"""F07 Subquery decorrelation."""
from __future__ import annotations
import logging
from metamind.core.logical.nodes import LogicalNode, ScanNode
logger = logging.getLogger(__name__)
class SubqueryDecorrelator:
    def decorrelate(self, root: LogicalNode) -> LogicalNode:
        return self._visit(root)
    def _visit(self, node: LogicalNode) -> LogicalNode:
        node.children = [self._visit(c) for c in node.children]
        return node
