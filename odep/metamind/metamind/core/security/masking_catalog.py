"""Column masking rules."""
from __future__ import annotations
import logging
import sqlalchemy as sa
from sqlalchemy.engine import Engine
logger = logging.getLogger(__name__)
class MaskingCatalog:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
    def get_masking_rules(self, tenant_id: str, schema: str, table: str) -> list[dict[str, object]]:
        stmt = sa.text("SELECT column_name, masking_type, masking_params FROM mm_masking_rules WHERE tenant_id=:tid AND schema_name=:s AND table_name=:t AND is_active=TRUE")
        with self._engine.connect() as conn:
            rows = conn.execute(stmt, {"tid": tenant_id, "s": schema, "t": table}).fetchall()
        return [{"column": r[0], "type": r[1], "params": r[2]} for r in rows]
