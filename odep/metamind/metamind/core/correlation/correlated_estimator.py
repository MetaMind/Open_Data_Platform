"""F02 Correlation-aware cardinality estimator."""
from __future__ import annotations
from metamind.core.logical.nodes import Predicate
from metamind.core.metadata.models import TableMeta
from metamind.core.correlation.dependency_graph import DependencyGraph
from metamind.core.costing.histograms import HistogramEstimator

class CorrelatedEstimator:
    def __init__(self, dep_graph: DependencyGraph) -> None:
        self._graph = dep_graph
        self._hist = HistogramEstimator()
    def estimate(self, predicates: list[Predicate], table_meta: TableMeta) -> float:
        col_map = {c.column_name.lower(): c for c in table_meta.columns}
        base = float(table_meta.row_count)
        sel = 1.0
        for pred in predicates:
            col = pred.column.split(".")[-1].lower()
            corr = 0.0
            for other in predicates:
                if other is pred: continue
                oc = other.column.split(".")[-1].lower()
                corr = max(corr, abs(self._graph.get_correlation(col, oc)))
            col_meta = col_map.get(col)
            s = self._hist.estimate_selectivity(pred, col_meta)
            adjustment = 1.0 + corr * 0.5
            sel *= (s ** (1.0 / adjustment))
        return max(1.0, base * sel)
