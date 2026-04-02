"""F23 Budget enforcement."""
from __future__ import annotations
import logging, sqlalchemy as sa
from sqlalchemy.engine import Engine
logger = logging.getLogger(__name__)
class BudgetEnforcer:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
    def check_budget(self, tenant_id: str, estimated_cost: float) -> tuple[bool, str]:
        stmt = sa.text("SELECT monthly_limit, current_spend, enforcement FROM mm_cloud_budgets WHERE tenant_id=:tid")
        with self._engine.connect() as conn:
            row = conn.execute(stmt, {"tid": tenant_id}).fetchone()
        if row is None: return True, ""
        limit, spend, enforce = float(row[0]), float(row[1]), bool(row[2])
        if enforce and (spend + estimated_cost) > limit:
            return False, f"Query would exceed budget: ${spend:.2f} + ${estimated_cost:.4f} > ${limit:.2f}"
        return True, ""
