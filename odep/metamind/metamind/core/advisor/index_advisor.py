"""F21 Index advisor - recommends indexes based on workload analysis.

Analyzes query patterns (filter predicates, join keys, ORDER BY columns)
to recommend covering indexes that would improve plan quality.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any, Optional

from metamind.core.logical.nodes import (
    LogicalNode, FilterNode, JoinNode, SortNode, ScanNode, NodeVisitor,
)

logger = logging.getLogger(__name__)


@dataclass
class IndexRecommendation:
    """A recommended index with cost/benefit analysis."""

    table_name: str
    schema_name: str
    columns: list[str]
    index_type: str  # btree, hash, gin, brin
    reason: str
    estimated_speedup: float  # multiplier (e.g. 10.0 = 10x faster)
    estimated_size_mb: float
    priority: float  # 0.0 to 1.0
    supporting_queries: int  # how many workload queries benefit

    def to_ddl(self, index_name: Optional[str] = None) -> str:
        """Generate CREATE INDEX DDL statement."""
        name = index_name or f"idx_{self.table_name}_{'_'.join(self.columns)}"
        cols = ", ".join(self.columns)
        using = f" USING {self.index_type}" if self.index_type != "btree" else ""
        return f"CREATE INDEX {name} ON {self.schema_name}.{self.table_name}{using} ({cols})"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict."""
        return {
            "table": self.table_name, "schema": self.schema_name,
            "columns": self.columns, "type": self.index_type,
            "reason": self.reason, "speedup": self.estimated_speedup,
            "size_mb": self.estimated_size_mb, "priority": self.priority,
            "ddl": self.to_ddl(),
        }


class _PredicateCollector(NodeVisitor):
    """Collects filter columns, join keys, and sort keys from a plan tree."""

    def __init__(self) -> None:
        self.filter_columns: list[tuple[str, str]] = []  # (table, column)
        self.join_keys: list[tuple[str, str]] = []
        self.sort_columns: list[tuple[str, str]] = []
        self.scanned_tables: set[str] = set()

    def visit(self, node: LogicalNode) -> None:
        if isinstance(node, ScanNode):
            self.scanned_tables.add(node.table_name)

        if isinstance(node, FilterNode):
            for pred in node.predicates:
                table = self._resolve_table(pred, node)
                self.filter_columns.append((table, pred.column))

        if isinstance(node, JoinNode):
            if node.condition:
                parts = node.condition.replace("=", " ").split()
                for p in parts:
                    if "." in p:
                        tbl, col = p.split(".", 1)
                        self.join_keys.append((tbl, col))

        if isinstance(node, SortNode):
            for sk in node.sort_keys:
                self.sort_columns.append(("", sk.column))

        for child in node.children:
            self.visit(child)

    def _resolve_table(self, pred: Any, node: LogicalNode) -> str:
        if hasattr(pred, "table_name") and pred.table_name:
            return pred.table_name
        # Walk down to find the scan node
        current = node
        while hasattr(current, "child") and current.child:
            current = current.child
        if isinstance(current, ScanNode):
            return current.table_name
        return ""


class IndexAdvisor:
    """Recommends indexes based on workload query patterns.

    Analyzes a set of representative queries (logical plans) and identifies:
    1. Frequently filtered columns (candidates for B-tree index)
    2. Join key columns (candidates for hash or B-tree index)
    3. Sort columns (candidates for B-tree index with ordering)
    4. Multi-column patterns (candidates for composite indexes)
    """

    def __init__(self, existing_indexes: Optional[list[dict[str, Any]]] = None) -> None:
        """Initialize with optional list of existing indexes to avoid duplicates."""
        self._existing: set[str] = set()
        if existing_indexes:
            for idx in existing_indexes:
                key = f"{idx.get('table', '')}:{','.join(idx.get('columns', []))}"
                self._existing.add(key)

    def recommend(
        self,
        tenant_id: str,
        plans: list[LogicalNode],
        catalog: Any,
        max_recommendations: int = 10,
    ) -> list[IndexRecommendation]:
        """Analyze workload plans and recommend indexes.

        Args:
            tenant_id: Tenant identifier.
            plans: List of logical plan roots from recent workload.
            catalog: MetadataCatalog for table metadata.
            max_recommendations: Maximum number of recommendations.

        Returns:
            List of IndexRecommendation sorted by priority.
        """
        # Phase 1: Collect column usage patterns across all plans
        filter_counts: Counter[tuple[str, str]] = Counter()
        join_counts: Counter[tuple[str, str]] = Counter()
        sort_counts: Counter[tuple[str, str]] = Counter()

        for plan in plans:
            collector = _PredicateCollector()
            collector.visit(plan)
            filter_counts.update(collector.filter_columns)
            join_counts.update(collector.join_keys)
            sort_counts.update(collector.sort_columns)

        # Phase 2: Score and generate recommendations
        recommendations: list[IndexRecommendation] = []

        # Filter-based indexes (highest impact)
        for (table, column), count in filter_counts.most_common(20):
            if not table or self._index_exists(table, [column]):
                continue

            row_count = self._get_row_count(catalog, tenant_id, table)
            ndv = self._get_ndv(catalog, tenant_id, table, column)
            selectivity = 1.0 / max(1, ndv) if ndv > 0 else 0.5

            # Speedup estimate: full scan / index scan
            speedup = min(1000.0, max(1.1, 1.0 / max(0.001, selectivity)))

            recommendations.append(IndexRecommendation(
                table_name=table,
                schema_name="public",
                columns=[column],
                index_type="btree",
                reason=f"Column appears in WHERE clause of {count} queries (selectivity={selectivity:.4f})",
                estimated_speedup=speedup,
                estimated_size_mb=self._estimate_index_size(row_count, 1),
                priority=min(1.0, (count / max(1, len(plans))) * speedup / 100),
                supporting_queries=count,
            ))

        # Join key indexes
        for (table, column), count in join_counts.most_common(10):
            if not table or self._index_exists(table, [column]):
                continue

            row_count = self._get_row_count(catalog, tenant_id, table)
            recommendations.append(IndexRecommendation(
                table_name=table,
                schema_name="public",
                columns=[column],
                index_type="hash",
                reason=f"Column used as join key in {count} queries",
                estimated_speedup=min(100.0, max(2.0, row_count / 10000)),
                estimated_size_mb=self._estimate_index_size(row_count, 1),
                priority=min(1.0, count / max(1, len(plans)) * 0.8),
                supporting_queries=count,
            ))

        # Composite indexes for filter+sort patterns
        for (table, col), fcount in filter_counts.most_common(5):
            for (_, scol), scount in sort_counts.most_common(5):
                if col == scol or not table:
                    continue
                if self._index_exists(table, [col, scol]):
                    continue
                if fcount >= 2 and scount >= 2:
                    row_count = self._get_row_count(catalog, tenant_id, table)
                    recommendations.append(IndexRecommendation(
                        table_name=table,
                        schema_name="public",
                        columns=[col, scol],
                        index_type="btree",
                        reason=f"Composite: filter on {col} ({fcount}x) + sort on {scol} ({scount}x)",
                        estimated_speedup=min(50.0, max(3.0, row_count / 5000)),
                        estimated_size_mb=self._estimate_index_size(row_count, 2),
                        priority=min(1.0, (fcount + scount) / max(1, len(plans) * 2)),
                        supporting_queries=fcount + scount,
                    ))

        recommendations.sort(key=lambda r: r.priority, reverse=True)
        return recommendations[:max_recommendations]

    def _index_exists(self, table: str, columns: list[str]) -> bool:
        key = f"{table}:{','.join(columns)}"
        return key in self._existing

    def _get_row_count(self, catalog: Any, tid: str, table: str) -> int:
        try:
            meta = catalog.get_table(tid, "public", table)
            return meta.row_count if meta else 10000
        except Exception:
            return 10000

    def _get_ndv(self, catalog: Any, tid: str, table: str, column: str) -> int:
        try:
            meta = catalog.get_table(tid, "public", table)
            if meta:
                for c in meta.columns:
                    if c.column_name == column:
                        return c.ndv or 100
        except Exception:
            logger.error("Unhandled exception in index_advisor.py: %s", exc)
        return 100

    def _estimate_index_size(self, row_count: int, num_columns: int) -> float:
        bytes_per_entry = 8 + num_columns * 16  # pointer + column data
        overhead = 1.3  # B-tree overhead
        return (row_count * bytes_per_entry * overhead) / (1024 * 1024)
