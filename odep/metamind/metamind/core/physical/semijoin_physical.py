"""Physical semi-join implementation."""
from __future__ import annotations
from metamind.core.logical.nodes import LogicalNode, SemiJoinNode
from metamind.core.physical.nodes import HashJoinOp

def convert_semijoin(node: SemiJoinNode) -> HashJoinOp:
    join = HashJoinOp(left_key=node.left_key, right_key=node.right_key)
    join.children = node.children
    return join
