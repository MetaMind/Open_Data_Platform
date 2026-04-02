"""Cache invalidation strategies."""
from __future__ import annotations
class InvalidationManager:
    def invalidate_table(self, tenant_id: str, table_name: str, plan_cache: object, result_cache: object) -> None:
        if hasattr(plan_cache, 'invalidate'):
            plan_cache.invalidate(tenant_id, table_name)
