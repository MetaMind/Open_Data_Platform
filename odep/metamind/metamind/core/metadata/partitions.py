"""Partition metadata operations."""
from __future__ import annotations
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from metamind.core.metadata.models import PartitionMeta
class PartitionManager:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
    def list_partitions(self, tenant_id: str, table_id: int) -> list[PartitionMeta]:
        stmt = sa.text("SELECT partition_name, partition_type, partition_key, lower_bound, upper_bound, row_count, is_prunable FROM mm_partitions WHERE tenant_id=:tid AND table_id=:tid2")
        with self._engine.connect() as conn:
            rows = conn.execute(stmt, {"tid": tenant_id, "tid2": table_id}).fetchall()
        return [PartitionMeta(partition_name=r[0], partition_type=r[1], partition_key=r[2], lower_bound=r[3], upper_bound=r[4], row_count=int(r[5] or 0), is_prunable=bool(r[6])) for r in rows]
