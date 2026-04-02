"""MV selection by cost."""
from __future__ import annotations
from metamind.core.metadata.models import MaterializedViewMeta
class MVSelector:
    def select_best(self, candidates: list[MaterializedViewMeta]) -> MaterializedViewMeta | None:
        if not candidates: return None
        return min(candidates, key=lambda mv: mv.cost_estimate)
