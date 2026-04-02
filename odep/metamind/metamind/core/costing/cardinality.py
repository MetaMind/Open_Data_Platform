"""Cardinality estimation facade."""
from __future__ import annotations
from metamind.core.costing.histograms import HistogramEstimator
from metamind.core.logical.nodes import Predicate
from metamind.core.metadata.models import ColumnMeta, TableMeta
class CardinalityEstimator:
    def __init__(self) -> None:
        self._hist = HistogramEstimator()
    def estimate(self, predicates: list[Predicate], table_meta: TableMeta) -> float:
        col_map = {c.column_name.lower(): c for c in table_meta.columns}
        return self._hist.estimate_cardinality(table_meta.row_count, predicates, col_map)
