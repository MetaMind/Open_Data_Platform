"""PyArrow vector execution engine."""
from __future__ import annotations
import logging
import pyarrow as pa
logger = logging.getLogger(__name__)
class ArrowEngine:
    def filter(self, table: pa.Table, column: str, value: object) -> pa.Table:
        import pyarrow.compute as pc
        return table.filter(pc.equal(table[column], value))
    def aggregate(self, table: pa.Table, group_by: list[str], agg_col: str) -> pa.Table:
        return table.group_by(group_by).aggregate([(agg_col, "sum")])
