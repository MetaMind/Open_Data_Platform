"""F21 Materialized view advisor - recommends MVs for repeated query patterns.

Identifies frequently executed aggregate/join patterns and recommends
pre-computed materialized views that would serve them.
"""
from __future__ import annotations

import hashlib
import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any, Optional

from metamind.core.logical.nodes import (
    LogicalNode, AggregateNode, JoinNode, ScanNode, FilterNode,
)

logger = logging.getLogger(__name__)


@dataclass
class MVRecommendation:
    """A recommended materialized view."""

    mv_name: str
    base_tables: list[str]
    group_by_columns: list[str]
    aggregate_expressions: list[str]
    filter_predicates: list[str]
    estimated_speedup: float
    storage_cost_mb: float
    refresh_frequency: str  # HOURLY, DAILY, ON_COMMIT
    priority: float
    supporting_queries: int
    create_sql: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize."""
        return {
            "mv_name": self.mv_name, "base_tables": self.base_tables,
            "group_by": self.group_by_columns, "aggregates": self.aggregate_expressions,
            "speedup": self.estimated_speedup, "storage_mb": self.storage_cost_mb,
            "refresh": self.refresh_frequency, "priority": self.priority,
            "create_sql": self.create_sql,
        }


@dataclass
class _AggPattern:
    """A detected aggregate pattern from a query plan."""

    tables: tuple[str, ...]
    group_by: tuple[str, ...]
    aggregates: tuple[str, ...]
    filters: tuple[str, ...]

    @property
    def fingerprint(self) -> str:
        raw = f"{self.tables}|{self.group_by}|{self.aggregates}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]


class MVAdvisor:
    """Recommends materialized views based on aggregate query patterns.

    Scans the workload for repeated aggregate + join patterns and suggests
    pre-computed MVs that would turn expensive queries into simple scans.
    """

    def recommend(
        self,
        tenant_id: str,
        plans: list[LogicalNode],
        catalog: Any = None,
        max_recommendations: int = 5,
    ) -> list[MVRecommendation]:
        """Analyze workload and recommend materialized views.

        Args:
            tenant_id: Tenant identifier.
            plans: List of logical plan roots.
            catalog: Optional MetadataCatalog for size estimation.
            max_recommendations: Max recommendations.

        Returns:
            Sorted list of MVRecommendation.
        """
        # Phase 1: Extract aggregate patterns from all plans
        pattern_counts: Counter[str] = Counter()
        patterns: dict[str, _AggPattern] = {}

        for plan in plans:
            found = self._extract_agg_patterns(plan)
            for pattern in found:
                fp = pattern.fingerprint
                pattern_counts[fp] += 1
                patterns[fp] = pattern

        # Phase 2: Generate recommendations for repeated patterns
        recommendations: list[MVRecommendation] = []

        for fp, count in pattern_counts.most_common(max_recommendations * 2):
            if count < 2:  # only recommend for patterns seen 2+ times
                continue

            pattern = patterns[fp]
            if not pattern.tables or not pattern.aggregates:
                continue

            mv_name = f"mv_{tenant_id}_{fp}"
            tables_str = ", ".join(pattern.tables)
            group_str = ", ".join(pattern.group_by) if pattern.group_by else ""
            agg_str = ", ".join(pattern.aggregates)

            # Build CREATE MV SQL
            select_parts = []
            if pattern.group_by:
                select_parts.extend(pattern.group_by)
            select_parts.extend(pattern.aggregates)

            where_clause = ""
            if pattern.filters:
                where_clause = " WHERE " + " AND ".join(pattern.filters)

            group_clause = ""
            if pattern.group_by:
                group_clause = " GROUP BY " + ", ".join(pattern.group_by)

            create_sql = (
                f"CREATE MATERIALIZED VIEW {mv_name} AS "
                f"SELECT {', '.join(select_parts)} "
                f"FROM {tables_str}{where_clause}{group_clause}"
            )

            # Estimate storage: group_count * row_size
            group_count = self._estimate_group_count(catalog, tenant_id, pattern)
            storage_mb = (group_count * len(select_parts) * 32) / (1024 * 1024)

            # Speedup: pre-computed agg is ~100x faster than recomputing
            row_count = self._estimate_base_rows(catalog, tenant_id, pattern)
            speedup = max(2.0, min(1000.0, row_count / max(1, group_count)))

            refresh = "HOURLY" if count > 10 else "DAILY"

            recommendations.append(MVRecommendation(
                mv_name=mv_name,
                base_tables=list(pattern.tables),
                group_by_columns=list(pattern.group_by),
                aggregate_expressions=list(pattern.aggregates),
                filter_predicates=list(pattern.filters),
                estimated_speedup=speedup,
                storage_cost_mb=storage_mb,
                refresh_frequency=refresh,
                priority=min(1.0, (count / max(1, len(plans))) * min(speedup, 100) / 100),
                supporting_queries=count,
                create_sql=create_sql,
            ))

        recommendations.sort(key=lambda r: r.priority, reverse=True)
        return recommendations[:max_recommendations]

    def _extract_agg_patterns(self, node: LogicalNode) -> list[_AggPattern]:
        """Extract aggregate patterns from a plan tree."""
        results: list[_AggPattern] = []
        self._visit_for_patterns(node, results)
        return results

    def _visit_for_patterns(self, node: LogicalNode,
                            results: list[_AggPattern]) -> None:
        """Recursively find AggregateNode patterns."""
        if isinstance(node, AggregateNode):
            tables = self._collect_tables(node)
            group_by = tuple(node.group_by) if node.group_by else ()
            aggregates = tuple(
                f"{e.func.value.upper()}({e.column})" for e in node.aggregates
            )
            filters = tuple(self._collect_filters(node))

            results.append(_AggPattern(
                tables=tuple(sorted(tables)),
                group_by=group_by,
                aggregates=aggregates,
                filters=filters,
            ))

        for child in node.children:
            self._visit_for_patterns(child, results)

    def _collect_tables(self, node: LogicalNode) -> set[str]:
        """Collect all table names referenced under a node."""
        tables: set[str] = set()
        if isinstance(node, ScanNode):
            tables.add(node.table_name)
        for child in node.children:
            tables.update(self._collect_tables(child))
        return tables

    def _collect_filters(self, node: LogicalNode) -> list[str]:
        """Collect filter predicates under a node."""
        filters: list[str] = []
        if isinstance(node, FilterNode):
            for p in node.predicates:
                filters.append(f"{p.column} {p.operator} {p.value!r}")
        for child in node.children:
            filters.extend(self._collect_filters(child))
        return filters

    def _estimate_group_count(self, catalog: Any, tid: str,
                              pattern: _AggPattern) -> int:
        if not pattern.group_by:
            return 1
        # Rough estimate: product of NDVs for group-by columns (capped)
        ndv_product = 1
        for col in pattern.group_by:
            ndv_product *= 100  # default NDV estimate
        return min(ndv_product, 10_000_000)

    def _estimate_base_rows(self, catalog: Any, tid: str,
                            pattern: _AggPattern) -> int:
        return 1_000_000  # default estimate
