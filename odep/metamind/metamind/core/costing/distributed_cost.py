"""Distributed query cost model."""
from __future__ import annotations
from metamind.core.costing.cost_model import CostModel, CostVector
class DistributedCostModel:
    def __init__(self, base: CostModel) -> None:
        self._base = base
    def shuffle_cost(self, rows: float, row_width_bytes: int, num_partitions: int) -> CostVector:
        size_mb = rows * row_width_bytes / (1024 * 1024)
        return self._base.network_transfer_cost(int(size_mb * 1024 * 1024), "any", "any")
