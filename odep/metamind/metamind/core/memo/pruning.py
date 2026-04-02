"""Branch-and-bound pruning for Cascades."""
from __future__ import annotations
import math
class BranchBoundPruner:
    def should_prune(self, current_cost: float, upper_bound: float) -> bool:
        return current_cost >= upper_bound
    def update_bound(self, cost: float, current_bound: float) -> float:
        return min(cost, current_bound)
