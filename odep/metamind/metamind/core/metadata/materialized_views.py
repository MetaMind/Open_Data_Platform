"""Materialized view metadata operations."""
from __future__ import annotations
from sqlalchemy.engine import Engine
from metamind.core.metadata.catalog import MetadataCatalog
class MVCatalog:
    def __init__(self, engine: Engine) -> None:
        self._catalog = MetadataCatalog(engine)
    def get_mvs(self, tenant_id: str) -> list[object]:
        return self._catalog.get_mvs(tenant_id)
