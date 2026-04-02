"""Plan inspector for debugging."""
from __future__ import annotations
from metamind.core.logical.nodes import LogicalNode
def print_plan(node: LogicalNode, indent: int = 0) -> str:
    lines = ["  " * indent + repr(node)]
    for child in node.children:
        lines.append(print_plan(child, indent + 1))
    return "\n".join(lines)
