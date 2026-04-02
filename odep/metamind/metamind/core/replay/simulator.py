"""F30 What-if optimization simulator — simplified facade for the replay system.

Re-exports the full OptimizationSimulator and WhatIfAPI from recorder.py,
and provides additional convenience methods for quick simulations without
full scenario management.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from metamind.core.costing.cost_model import CostModel, CostVector
from metamind.core.metadata.catalog import MetadataCatalog

# Re-export the full implementations from recorder.py
from metamind.core.replay.recorder import (
    OptimizationSimulator,
    ReplayRecorder,
    ReplayResult,
    ReplayScenario,
    WhatIfAPI,
)

logger = logging.getLogger(__name__)

__all__ = [
    "OptimizationSimulator",
    "WhatIfAPI",
    "QuickSimulator",
    "ReplayResult",
    "ReplayScenario",
]


class QuickSimulator:
    """Lightweight simulation interface for quick what-if analysis.

    Unlike the full OptimizationSimulator which requires scenario management
    and replays historical queries, QuickSimulator provides instant cost
    estimation for hypothetical schema changes against a single query.
    """

    def __init__(
        self,
        catalog: MetadataCatalog,
        cost_model: Optional[CostModel] = None,
    ) -> None:
        """Initialize with catalog and optional cost model.

        Args:
            catalog: MetadataCatalog for current schema state.
            cost_model: CostModel for cost estimation (creates default if None).
        """
        self._catalog = catalog
        self._cost_model = cost_model or CostModel()

    def simulate_index(
        self,
        tenant_id: str,
        table: str,
        column: str,
        query_sql: str,
        index_type: str = "btree",
    ) -> dict[str, Any]:
        """Estimate impact of adding an index on a single query.

        Args:
            tenant_id: Tenant identifier.
            table: Table name.
            column: Column to index.
            query_sql: SQL query to evaluate.
            index_type: Type of index (btree, hash, gin, brin).

        Returns:
            Dict with before/after cost estimates and speedup.
        """
        table_meta = self._get_table_meta(tenant_id, table)
        row_count = table_meta.get("row_count", 100000) if table_meta else 100000
        col_ndv = self._get_column_ndv(tenant_id, table, column)

        # Before: full table scan
        before_cost = self._cost_model.scan_cost(
            row_count, row_count * 256, 1.0
        )

        # After: index lookup (selectivity based on NDV)
        selectivity = 1.0 / max(1, col_ndv)
        after_cost = self._cost_model.scan_cost(
            row_count, row_count * 256, selectivity
        )

        before_scalar = self._cost_model.scalar_cost(before_cost)
        after_scalar = self._cost_model.scalar_cost(after_cost)
        speedup = before_scalar / max(0.001, after_scalar)

        return {
            "table": table,
            "column": column,
            "index_type": index_type,
            "before_cost_ms": before_cost.latency_ms,
            "after_cost_ms": after_cost.latency_ms,
            "speedup": round(speedup, 2),
            "recommendation": "beneficial" if speedup > 1.5 else "marginal",
        }

    def simulate_partition(
        self,
        tenant_id: str,
        table: str,
        column: str,
        partition_type: str = "range",
        num_partitions: int = 12,
    ) -> dict[str, Any]:
        """Estimate impact of partitioning a table.

        Args:
            tenant_id: Tenant identifier.
            table: Table name.
            column: Partition column.
            partition_type: range, list, or hash.
            num_partitions: Expected number of partitions.

        Returns:
            Dict with before/after scan estimates and pruning ratio.
        """
        table_meta = self._get_table_meta(tenant_id, table)
        row_count = table_meta.get("row_count", 1000000) if table_meta else 1000000

        pruning_ratio = max(0.0, 1.0 - (1.0 / max(1, num_partitions)))
        effective_rows = int(row_count * (1.0 - pruning_ratio))

        before = self._cost_model.scan_cost(row_count, row_count * 256, 1.0)
        after = self._cost_model.scan_cost(effective_rows, effective_rows * 256, 1.0)

        return {
            "table": table,
            "column": column,
            "partition_type": partition_type,
            "num_partitions": num_partitions,
            "pruning_ratio": round(pruning_ratio, 3),
            "before_scan_rows": row_count,
            "after_scan_rows": effective_rows,
            "before_cost_ms": before.latency_ms,
            "after_cost_ms": after.latency_ms,
            "speedup": round(before.latency_ms / max(0.001, after.latency_ms), 2),
        }

    def simulate_backend_migration(
        self,
        tenant_id: str,
        table: str,
        source_backend: str,
        target_backend: str,
    ) -> dict[str, Any]:
        """Estimate cost impact of migrating a table to a different backend.

        Args:
            tenant_id: Tenant identifier.
            table: Table name.
            source_backend: Current backend.
            target_backend: Target backend.

        Returns:
            Dict with migration cost, ongoing cost comparison, and recommendation.
        """
        table_meta = self._get_table_meta(tenant_id, table)
        row_count = table_meta.get("row_count", 1000000) if table_meta else 1000000
        size_bytes = row_count * 256

        # Migration cost (one-time network transfer)
        migration_cost = self._cost_model.network_transfer_cost(
            size_bytes, source_backend, target_backend
        )

        # Ongoing scan cost comparison
        source_scan = self._cost_model.scan_cost(row_count, size_bytes, 0.5, source_backend)
        target_scan = self._cost_model.scan_cost(row_count, size_bytes, 0.5, target_backend)

        source_scalar = self._cost_model.scalar_cost(source_scan)
        target_scalar = self._cost_model.scalar_cost(target_scan)

        return {
            "table": table,
            "source_backend": source_backend,
            "target_backend": target_backend,
            "migration_cost_ms": migration_cost.latency_ms,
            "migration_cost_usd": migration_cost.cloud_cost_usd,
            "source_query_cost_ms": source_scan.latency_ms,
            "target_query_cost_ms": target_scan.latency_ms,
            "ongoing_speedup": round(source_scalar / max(0.001, target_scalar), 2),
            "recommendation": (
                "beneficial" if target_scalar < source_scalar * 0.8
                else "marginal" if target_scalar < source_scalar
                else "not_recommended"
            ),
        }

    def compare_scenarios(
        self, scenarios: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Compare multiple hypothetical changes side by side.

        Args:
            scenarios: List of scenario dicts, each with "type" and params.

        Returns:
            List of result dicts with costs for each scenario.
        """
        results: list[dict[str, Any]] = []
        for scenario in scenarios:
            stype = scenario.get("type", "")
            try:
                if stype == "add_index":
                    result = self.simulate_index(
                        scenario["tenant_id"], scenario["table"],
                        scenario["column"], scenario.get("query", "SELECT 1"),
                    )
                elif stype == "partition":
                    result = self.simulate_partition(
                        scenario["tenant_id"], scenario["table"],
                        scenario["column"], scenario.get("partition_type", "range"),
                    )
                elif stype == "migrate":
                    result = self.simulate_backend_migration(
                        scenario["tenant_id"], scenario["table"],
                        scenario["source"], scenario["target"],
                    )
                else:
                    result = {"error": f"Unknown scenario type: {stype}"}
            except Exception as exc:
                result = {"error": str(exc)}

            result["scenario"] = scenario
            results.append(result)

        return results

    def _get_table_meta(self, tenant_id: str, table: str) -> Optional[dict[str, Any]]:
        """Get table metadata from catalog."""
        try:
            meta = self._catalog.get_table(tenant_id, "public", table)
            if meta:
                return {"row_count": meta.row_count, "name": meta.table_name}
        except Exception:
            logger.error("Unhandled exception in simulator.py: %s", exc)
        return None

    def _get_column_ndv(self, tenant_id: str, table: str, column: str) -> int:
        """Get column NDV from catalog."""
        try:
            meta = self._catalog.get_table(tenant_id, "public", table)
            if meta:
                for c in meta.columns:
                    if c.column_name == column:
                        return c.ndv if c.ndv > 0 else 100
        except Exception:
            logger.error("Unhandled exception in simulator.py: %s", exc)
        return 100
