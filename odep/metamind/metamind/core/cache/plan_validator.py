"""Cached plan validator — checks plan is still valid."""
from __future__ import annotations
from metamind.core.cache.plan_cache import CachedPlan
class PlanValidator:
    def is_valid(self, plan: CachedPlan, current_stats_version: int) -> bool:
        return plan.is_valid and plan.age_seconds < 86400
