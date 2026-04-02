"""Metadata version tracking."""
from __future__ import annotations
import sqlalchemy as sa
from sqlalchemy.engine import Engine
class MetadataVersioning:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
    def get_version(self, tenant_id: str, table_name: str) -> int:
        stmt = sa.text("SELECT COALESCE(MAX(version),0) FROM mm_metadata_versions WHERE tenant_id=:tid AND table_name=:tbl")
        with self._engine.connect() as conn:
            row = conn.execute(stmt, {"tid": tenant_id, "tbl": table_name}).fetchone()
        return int(row[0]) if row else 0
    def bump(self, tenant_id: str, table_name: str, change_type: str = "stats_update") -> None:
        stmt = sa.text("INSERT INTO mm_metadata_versions(tenant_id,table_name,version,change_type) SELECT :tid,:tbl,COALESCE(MAX(version),0)+1,:ct FROM mm_metadata_versions WHERE tenant_id=:tid AND table_name=:tbl")
        with self._engine.begin() as conn:
            conn.execute(stmt, {"tid": tenant_id, "tbl": table_name, "ct": change_type})
