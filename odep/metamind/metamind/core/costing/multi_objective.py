"""F27 Multi-objective cost optimizer."""
from __future__ import annotations
from metamind.core.costing.cost_model import CostVector, CostWeights
class MultiObjectiveCost:
    def pareto_dominant(self, a: CostVector, b: CostVector) -> bool:
        return (a.latency_ms <= b.latency_ms and a.cloud_cost_usd <= b.cloud_cost_usd and (a.latency_ms < b.latency_ms or a.cloud_cost_usd < b.cloud_cost_usd))
    def weighted_scalarize(self, cv: CostVector, weights: CostWeights) -> float:
        return cv.weighted_total(weights)
