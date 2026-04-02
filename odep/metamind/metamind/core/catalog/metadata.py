"""Metadata catalog for tenant schema, statistics, and index metadata."""
from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from metamind.core.types import ColumnMeta, IndexMeta, TableMeta

logger = logging.getLogger(__name__)


@dataclass
class CatalogSnapshot:
    """Immutable snapshot of catalog state for what-if simulation."""

    tables: dict[str, TableMeta]
    indexes: dict[str, list[IndexMeta]]
    statistics: dict[str, dict[str, Any]]
    vector_indexes: dict[str, dict[str, Any]]

    def deep_copy(self) -> CatalogSnapshot:
        return CatalogSnapshot(
            tables=copy.deepcopy(self.tables),
            indexes=copy.deepcopy(self.indexes),
            statistics=copy.deepcopy(self.statistics),
            vector_indexes=copy.deepcopy(self.vector_indexes),
        )


class MetadataCatalog:
    """Central metadata registry for all tenant schemas and statistics."""

    def __init__(self) -> None:
        self._tables: dict[str, dict[str, TableMeta]] = {}
        self._indexes: dict[str, dict[str, list[IndexMeta]]] = {}
        self._statistics: dict[str, dict[str, dict[str, Any]]] = {}
        self._vector_indexes: dict[str, dict[str, dict[str, Any]]] = {}

    def register_table(self, tenant_id: str, table: TableMeta) -> None:
        if tenant_id not in self._tables:
            self._tables[tenant_id] = {}
        self._tables[tenant_id][table.table_name] = table
        logger.debug("Registered table %s for tenant %s", table.table_name, tenant_id)

    def get_table(self, tenant_id: str, table_name: str) -> Optional[TableMeta]:
        return self._tables.get(tenant_id, {}).get(table_name)

    def list_tables(self, tenant_id: str) -> list[TableMeta]:
        return list(self._tables.get(tenant_id, {}).values())

    def register_index(self, tenant_id: str, table_name: str, index: IndexMeta) -> None:
        if tenant_id not in self._indexes:
            self._indexes[tenant_id] = {}
        if table_name not in self._indexes[tenant_id]:
            self._indexes[tenant_id][table_name] = []
        self._indexes[tenant_id][table_name].append(index)

    def get_indexes(self, tenant_id: str, table_name: str) -> list[IndexMeta]:
        return self._indexes.get(tenant_id, {}).get(table_name, [])

    def update_statistics(
        self, tenant_id: str, table_name: str, stats: dict[str, Any]
    ) -> None:
        if tenant_id not in self._statistics:
            self._statistics[tenant_id] = {}
        self._statistics[tenant_id][table_name] = stats

    def get_statistics(self, tenant_id: str, table_name: str) -> dict[str, Any]:
        return self._statistics.get(tenant_id, {}).get(table_name, {})

    def register_vector_index(
        self, tenant_id: str, key: str, meta: dict[str, Any]
    ) -> None:
        if tenant_id not in self._vector_indexes:
            self._vector_indexes[tenant_id] = {}
        self._vector_indexes[tenant_id][key] = meta

    def get_vector_index(
        self, tenant_id: str, table: str, column: str
    ) -> Optional[dict[str, Any]]:
        key = f"{table}.{column}"
        return self._vector_indexes.get(tenant_id, {}).get(key)

    def list_vector_indexes(self, tenant_id: str) -> list[dict[str, Any]]:
        return list(self._vector_indexes.get(tenant_id, {}).values())

    def remove_vector_index(self, tenant_id: str, key: str) -> None:
        if tenant_id in self._vector_indexes:
            self._vector_indexes[tenant_id].pop(key, None)

    def snapshot(self, tenant_id: str) -> CatalogSnapshot:
        return CatalogSnapshot(
            tables=copy.deepcopy(self._tables.get(tenant_id, {})),
            indexes=copy.deepcopy(self._indexes.get(tenant_id, {})),
            statistics=copy.deepcopy(self._statistics.get(tenant_id, {})),
            vector_indexes=copy.deepcopy(self._vector_indexes.get(tenant_id, {})),
        )

    def from_snapshot(self, tenant_id: str, snap: CatalogSnapshot) -> None:
        self._tables[tenant_id] = snap.tables
        self._indexes[tenant_id] = snap.indexes
        self._statistics[tenant_id] = snap.statistics
        self._vector_indexes[tenant_id] = snap.vector_indexes

    def table_exists(self, tenant_id: str, table_name: str) -> bool:
        return table_name in self._tables.get(tenant_id, {})

    def column_exists(self, tenant_id: str, table_name: str, column_name: str) -> bool:
        table = self.get_table(tenant_id, table_name)
        if table is None:
            return False
        return any(c.name == column_name for c in table.columns)

    def get_column(
        self, tenant_id: str, table_name: str, column_name: str
    ) -> Optional[ColumnMeta]:
        table = self.get_table(tenant_id, table_name)
        if table is None:
            return None
        for col in table.columns:
            if col.name == column_name:
                return col
        return None
