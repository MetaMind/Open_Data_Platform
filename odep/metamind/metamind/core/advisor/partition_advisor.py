"""F21 Partition advisor - recommends partitioning strategies based on workload.

Analyzes filter predicates across the workload to identify columns that
would benefit from range or list partitioning.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any, Optional

from metamind.core.logical.nodes import LogicalNode, FilterNode, ScanNode

logger = logging.getLogger(__name__)


@dataclass
class PartitionRecommendation:
    """A recommended partitioning strategy."""

    table_name: str
    schema_name: str
    partition_column: str
    partition_type: str  # range, list, hash
    reason: str
    estimated_pruning_ratio: float  # fraction of partitions pruned on avg query
    estimated_speedup: float
    priority: float
    supporting_queries: int
    suggested_intervals: Optional[str] = None  # e.g. "MONTHLY", "DAILY"

    def to_dict(self) -> dict[str, Any]:
        """Serialize."""
        return {
            "table": self.table_name, "schema": self.schema_name,
            "column": self.partition_column, "type": self.partition_type,
            "reason": self.reason, "pruning_ratio": self.estimated_pruning_ratio,
            "speedup": self.estimated_speedup, "priority": self.priority,
            "interval": self.suggested_intervals,
        }


# Column name patterns that suggest temporal partitioning
_DATE_PATTERNS = {"date", "created", "updated", "timestamp", "time", "day", "month", "year",
                  "created_at", "updated_at", "event_time", "order_date", "txn_date"}


class PartitionAdvisor:
    """Recommends table partitioning based on workload filter patterns.

    Identifies high-selectivity filter columns that appear across many queries
    and recommends range partitioning for dates or hash partitioning for
    high-cardinality categorical columns.
    """

    def recommend(
        self,
        tenant_id: str,
        plans: list[LogicalNode],
        catalog: Any,
        max_recommendations: int = 5,
    ) -> list[PartitionRecommendation]:
        """Analyze workload and recommend partitioning strategies.

        Args:
            tenant_id: Tenant identifier.
            plans: List of logical plan roots from workload.
            catalog: MetadataCatalog.
            max_recommendations: Max recommendations to return.

        Returns:
            Sorted list of PartitionRecommendation.
        """
        # Collect filter column frequencies per table
        table_filter_cols: dict[str, Counter[str]] = {}

        for plan in plans:
            self._collect_filters(plan, table_filter_cols)

        recommendations: list[PartitionRecommendation] = []

        for table, col_counts in table_filter_cols.items():
            row_count = self._get_row_count(catalog, tenant_id, table)

            # Only recommend partitioning for large tables
            if row_count < 1_000_000:
                continue

            for column, count in col_counts.most_common(3):
                # Determine partition type based on column name/type
                is_temporal = any(pat in column.lower() for pat in _DATE_PATTERNS)
                col_meta = self._get_column_meta(catalog, tenant_id, table, column)
                ndv = col_meta.get("ndv", 100) if col_meta else 100

                if is_temporal:
                    partition_type = "range"
                    interval = "MONTHLY" if row_count > 100_000_000 else "DAILY"
                    num_partitions = 365 if interval == "DAILY" else 12
                    pruning_ratio = max(0.5, 1.0 - (1.0 / num_partitions))
                elif ndv < 50:
                    partition_type = "list"
                    pruning_ratio = max(0.3, 1.0 - (1.0 / max(1, ndv)))
                    interval = None
                else:
                    partition_type = "hash"
                    num_buckets = min(64, max(4, ndv // 100))
                    pruning_ratio = max(0.3, 1.0 - (1.0 / num_buckets))
                    interval = f"{num_buckets} buckets"

                speedup = max(1.5, 1.0 / max(0.01, 1.0 - pruning_ratio))
                priority = min(1.0, (count / max(1, len(plans))) * pruning_ratio)

                recommendations.append(PartitionRecommendation(
                    table_name=table,
                    schema_name="public",
                    partition_column=column,
                    partition_type=partition_type,
                    reason=f"Column filtered in {count}/{len(plans)} queries, "
                           f"row_count={row_count:,}, est. {pruning_ratio:.0%} partition pruning",
                    estimated_pruning_ratio=pruning_ratio,
                    estimated_speedup=speedup,
                    priority=priority,
                    supporting_queries=count,
                    suggested_intervals=interval,
                ))

        recommendations.sort(key=lambda r: r.priority, reverse=True)
        return recommendations[:max_recommendations]

    def _collect_filters(self, node: LogicalNode,
                         table_cols: dict[str, Counter[str]]) -> None:
        """Recursively collect filter columns mapped to their source tables."""
        if isinstance(node, FilterNode):
            table_name = self._find_scan_table(node)
            if table_name:
                if table_name not in table_cols:
                    table_cols[table_name] = Counter()
                for pred in node.predicates:
                    table_cols[table_name][pred.column] += 1

        for child in node.children:
            self._collect_filters(child, table_cols)

    def _find_scan_table(self, node: LogicalNode) -> Optional[str]:
        """Walk down to find the ScanNode under a filter."""
        current = node
        while hasattr(current, "child") and current.child:
            current = current.child
        return current.table_name if isinstance(current, ScanNode) else None

    def _get_row_count(self, catalog: Any, tid: str, table: str) -> int:
        try:
            meta = catalog.get_table(tid, "public", table)
            return meta.row_count if meta else 0
        except Exception:
            return 0

    def _get_column_meta(self, catalog: Any, tid: str, table: str,
                         column: str) -> Optional[dict[str, Any]]:
        try:
            meta = catalog.get_table(tid, "public", table)
            if meta:
                for c in meta.columns:
                    if c.column_name == column:
                        return {"ndv": c.ndv or 100, "null_fraction": c.null_fraction or 0.0}
        except Exception:
            logger.error("Unhandled exception in partition_advisor.py: %s", exc)
        return None
