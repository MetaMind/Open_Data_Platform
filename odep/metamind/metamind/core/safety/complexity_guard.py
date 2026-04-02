"""F12 Complexity guard."""
from __future__ import annotations
import logging
from metamind.core.logical.nodes import LogicalNode, JoinNode, FilterNode, ScanNode
logger = logging.getLogger(__name__)
class ComplexityGuard:
    def __init__(self, max_joins: int = 20, max_predicates: int = 100) -> None:
        self._max_joins = max_joins
        self._max_predicates = max_predicates
    def check(self, root: LogicalNode) -> tuple[bool, str]:
        stats = self._collect(root)
        if stats["joins"] > self._max_joins:
            return False, f"Too many joins: {stats['joins']}"
        return True, ""
    def _collect(self, node: LogicalNode) -> dict[str, int]:
        joins = 1 if isinstance(node, JoinNode) else 0
        preds = len(getattr(node, 'predicates', []))
        for child in node.children:
            cs = self._collect(child)
            joins += cs["joins"]; preds += cs["predicates"]
        return {"joins": joins, "predicates": preds}
