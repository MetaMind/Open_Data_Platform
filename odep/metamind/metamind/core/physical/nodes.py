"""Physical plan nodes."""
from __future__ import annotations
from dataclasses import dataclass, field
from metamind.core.logical.nodes import LogicalNode

@dataclass
class PhysicalScan(LogicalNode):
    table_name: str = ""; backend: str = "postgres"; index_name: str = ""
    def __post_init__(self) -> None: super().__init__()
    def accept(self, visitor: object) -> object: return None
    def __repr__(self) -> str: return f"PhysicalScan({self.table_name}@{self.backend})"

@dataclass
class HashJoinOp(LogicalNode):
    left_key: str = ""; right_key: str = ""; backend: str = "postgres"
    def __post_init__(self) -> None: super().__init__()
    def accept(self, visitor: object) -> object: return None
    def __repr__(self) -> str: return f"HashJoin({self.left_key}={self.right_key})"

@dataclass
class BroadcastJoinOp(HashJoinOp):
    broadcast_side: str = "right"
    def __repr__(self) -> str: return f"BroadcastJoin({self.left_key}={self.right_key}, broadcast={self.broadcast_side})"
