"""Physical plan extractor from logical plan."""
from __future__ import annotations
from metamind.core.logical.nodes import LogicalNode
from metamind.core.physical.nodes import PhysicalScan, HashJoinOp
from metamind.core.logical.nodes import ScanNode, JoinNode

class PhysicalPlanExtractor:
    def extract(self, node: LogicalNode, backend: str = "postgres") -> LogicalNode:
        if isinstance(node, ScanNode):
            p = PhysicalScan(table_name=node.table_name, backend=backend)
            return p
        if isinstance(node, JoinNode):
            p = HashJoinOp(backend=backend)
            p.children = [self.extract(c, backend) for c in node.children]
            return p
        for i, child in enumerate(node.children):
            node.children[i] = self.extract(child, backend)
        return node
