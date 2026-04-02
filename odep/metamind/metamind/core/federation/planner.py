"""F14 Cross-engine federation planner - cost-based multi-engine query planning.

Given a logical plan that references tables on different backends, the federation
planner determines:
1. Which sub-plans execute on which backend.
2. Where data transfers happen (and their cost).
3. Which backend hosts the final join/aggregation.
4. The overall cost-optimal execution strategy.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any, Optional

from metamind.core.logical.nodes import (
    LogicalNode, ScanNode, JoinNode, FilterNode, ProjectNode,
    AggregateNode, SortNode, LimitNode,
)
from metamind.core.costing.cost_model import CostModel, CostVector

logger = logging.getLogger(__name__)


@dataclass
class TablePlacement:
    """Where a table lives and its cost profile."""

    table_name: str
    backend_id: str
    estimated_rows: int = 0
    estimated_size_bytes: int = 0
    dialect: str = "postgres"


@dataclass
class SubPlan:
    """A portion of the query that executes on a single backend."""

    backend_id: str
    dialect: str
    node: LogicalNode
    estimated_rows: int = 0
    estimated_cost: float = 0.0
    tables_involved: list[str] = field(default_factory=list)


@dataclass
class DataTransfer:
    """A cross-engine data movement between two sub-plans."""

    source_backend: str
    target_backend: str
    estimated_rows: int = 0
    estimated_bytes: int = 0
    transfer_cost: CostVector = field(default_factory=CostVector)


@dataclass
class FederatedPlan:
    """Complete federated execution plan with cost analysis."""

    sub_plans: list[SubPlan]
    transfers: list[DataTransfer]
    final_backend: str
    total_cost: CostVector
    strategy: str  # "single_engine", "ship_to_largest", "ship_to_cheapest"
    plan_details: dict[str, Any] = field(default_factory=dict)


class FederationPlanner:
    """Cost-based federation planner for cross-engine query execution.

    Strategies:
    1. SINGLE_ENGINE: All tables on one backend. No federation needed.
    2. SHIP_TO_LARGEST: Move smaller tables to the backend with the largest table.
    3. SHIP_TO_CHEAPEST: Move all data to the cheapest execution backend.
    4. SPLIT_PUSHDOWN: Push filters/projections to source, transfer reduced data.
    """

    def __init__(self, cost_model: Optional[CostModel] = None) -> None:
        """Initialize with cost model for comparing strategies."""
        self._cost_model = cost_model or CostModel()

    def plan(
        self,
        root: LogicalNode,
        table_placements: dict[str, TablePlacement],
        available_backends: Optional[list[str]] = None,
    ) -> FederatedPlan:
        """Generate an optimal federated execution plan.

        Args:
            root: Root of the logical plan tree.
            table_placements: Map of table_name -> TablePlacement.
            available_backends: List of available backend IDs.

        Returns:
            FederatedPlan with sub-plans, transfers, and cost analysis.
        """
        # Step 1: Identify which backends are involved
        tables_used = self._collect_tables(root)
        backends_involved = self._get_involved_backends(tables_used, table_placements)

        if not backends_involved:
            # No placement info: default to first available
            default_be = (available_backends[0] if available_backends else "postgres")
            return self._single_engine_plan(root, default_be)

        # Step 2: Single-engine fast path
        if len(backends_involved) == 1:
            backend = list(backends_involved)[0]
            return self._single_engine_plan(root, backend)

        # Step 3: Multi-engine — evaluate strategies
        strategies = [
            self._strategy_ship_to_largest(root, table_placements, backends_involved),
            self._strategy_ship_to_cheapest(root, table_placements, backends_involved),
            self._strategy_split_pushdown(root, table_placements, backends_involved),
        ]

        # Pick lowest cost strategy
        strategies.sort(key=lambda p: self._cost_model.scalar_cost(p.total_cost))
        best = strategies[0]

        logger.info(
            "Federation plan: strategy=%s backends=%s transfers=%d cost=%.2f",
            best.strategy, backends_involved, len(best.transfers),
            self._cost_model.scalar_cost(best.total_cost),
        )
        return best

    def _single_engine_plan(self, root: LogicalNode, backend: str) -> FederatedPlan:
        """Create a trivial plan for single-engine execution."""
        tables = list(self._collect_tables(root))
        sub = SubPlan(
            backend_id=backend, dialect=backend.split("-")[0],
            node=root, tables_involved=tables,
        )
        return FederatedPlan(
            sub_plans=[sub], transfers=[], final_backend=backend,
            total_cost=CostVector(), strategy="single_engine",
        )

    def _strategy_ship_to_largest(
        self, root: LogicalNode,
        placements: dict[str, TablePlacement],
        backends: set[str],
    ) -> FederatedPlan:
        """Move smaller tables to the backend hosting the largest table."""
        # Find backend with the largest table
        backend_sizes: dict[str, int] = {}
        for tp in placements.values():
            if tp.backend_id in backends:
                current = backend_sizes.get(tp.backend_id, 0)
                backend_sizes[tp.backend_id] = current + tp.estimated_size_bytes

        target_backend = max(backend_sizes, key=backend_sizes.get, default="postgres")

        # Calculate transfers for tables not on the target
        transfers: list[DataTransfer] = []
        total_transfer_cost = CostVector()

        for table, tp in placements.items():
            if tp.backend_id != target_backend and tp.backend_id in backends:
                xfer_cost = self._cost_model.network_transfer_cost(
                    tp.estimated_size_bytes, tp.backend_id, target_backend
                )
                transfers.append(DataTransfer(
                    source_backend=tp.backend_id,
                    target_backend=target_backend,
                    estimated_rows=tp.estimated_rows,
                    estimated_bytes=tp.estimated_size_bytes,
                    transfer_cost=xfer_cost,
                ))
                total_transfer_cost = total_transfer_cost + xfer_cost

        # Execution cost on target backend
        total_rows = sum(tp.estimated_rows for tp in placements.values())
        exec_cost = self._estimate_execution_cost(root, target_backend, total_rows)

        return FederatedPlan(
            sub_plans=[SubPlan(
                backend_id=target_backend, dialect=target_backend.split("-")[0],
                node=root, tables_involved=list(placements.keys()),
            )],
            transfers=transfers,
            final_backend=target_backend,
            total_cost=total_transfer_cost + exec_cost,
            strategy="ship_to_largest",
        )

    def _strategy_ship_to_cheapest(
        self, root: LogicalNode,
        placements: dict[str, TablePlacement],
        backends: set[str],
    ) -> FederatedPlan:
        """Move all data to the cheapest-to-execute backend."""
        best_plan: Optional[FederatedPlan] = None
        best_cost = float("inf")

        for candidate in backends:
            transfers: list[DataTransfer] = []
            total_transfer = CostVector()

            for table, tp in placements.items():
                if tp.backend_id != candidate:
                    xfer = self._cost_model.network_transfer_cost(
                        tp.estimated_size_bytes, tp.backend_id, candidate
                    )
                    transfers.append(DataTransfer(
                        source_backend=tp.backend_id, target_backend=candidate,
                        estimated_rows=tp.estimated_rows,
                        estimated_bytes=tp.estimated_size_bytes,
                        transfer_cost=xfer,
                    ))
                    total_transfer = total_transfer + xfer

            total_rows = sum(tp.estimated_rows for tp in placements.values())
            exec_cost = self._estimate_execution_cost(root, candidate, total_rows)
            total = total_transfer + exec_cost
            scalar = self._cost_model.scalar_cost(total)

            if scalar < best_cost:
                best_cost = scalar
                best_plan = FederatedPlan(
                    sub_plans=[SubPlan(
                        backend_id=candidate, dialect=candidate.split("-")[0],
                        node=root, tables_involved=list(placements.keys()),
                    )],
                    transfers=transfers,
                    final_backend=candidate,
                    total_cost=total,
                    strategy="ship_to_cheapest",
                )

        return best_plan or self._single_engine_plan(root, list(backends)[0])

    def _strategy_split_pushdown(
        self, root: LogicalNode,
        placements: dict[str, TablePlacement],
        backends: set[str],
    ) -> FederatedPlan:
        """Push filters and projections to source backends, transfer reduced data.

        For each backend, extract the sub-tree that can execute locally (scans +
        filters + projections), execute there, and ship reduced results to the
        final join/aggregation backend.
        """
        sub_plans: list[SubPlan] = []
        transfers: list[DataTransfer] = []
        total_cost = CostVector()

        # Group tables by backend
        backend_tables: dict[str, list[str]] = {}
        for table, tp in placements.items():
            be = tp.backend_id
            if be not in backend_tables:
                backend_tables[be] = []
            backend_tables[be].append(table)

        # For each backend, create a sub-plan with pushed-down predicates
        for be, tables in backend_tables.items():
            local_nodes = self._extract_local_subtree(root, set(tables))
            if local_nodes is None:
                continue

            # Estimate rows after local filtering (assume 30% selectivity for filters)
            total_rows = sum(
                placements[t].estimated_rows for t in tables if t in placements
            )
            has_filter = self._has_filter(local_nodes)
            estimated_output = int(total_rows * (0.3 if has_filter else 1.0))

            sub_plans.append(SubPlan(
                backend_id=be, dialect=be.split("-")[0],
                node=local_nodes, estimated_rows=estimated_output,
                tables_involved=tables,
            ))

        # Determine final backend (the one with the most local data)
        if sub_plans:
            final_be = max(sub_plans, key=lambda sp: sp.estimated_rows).backend_id
        else:
            final_be = list(backends)[0]

        # Calculate transfers to final backend
        for sp in sub_plans:
            if sp.backend_id != final_be:
                row_bytes = sp.estimated_rows * 256  # ~256 bytes per row estimate
                xfer = self._cost_model.network_transfer_cost(
                    row_bytes, sp.backend_id, final_be
                )
                transfers.append(DataTransfer(
                    source_backend=sp.backend_id, target_backend=final_be,
                    estimated_rows=sp.estimated_rows, estimated_bytes=row_bytes,
                    transfer_cost=xfer,
                ))
                total_cost = total_cost + xfer

        # Add final execution cost
        final_rows = sum(sp.estimated_rows for sp in sub_plans)
        exec_cost = self._estimate_execution_cost(root, final_be, final_rows)
        total_cost = total_cost + exec_cost

        return FederatedPlan(
            sub_plans=sub_plans, transfers=transfers,
            final_backend=final_be, total_cost=total_cost,
            strategy="split_pushdown",
        )

    # ── Helpers ──────────────────────────────────────────────

    def _collect_tables(self, node: LogicalNode) -> set[str]:
        """Collect all table names referenced in the plan tree."""
        tables: set[str] = set()
        if isinstance(node, ScanNode):
            tables.add(node.table_name)
        for child in node.children:
            tables.update(self._collect_tables(child))
        return tables

    def _get_involved_backends(
        self, tables: set[str], placements: dict[str, TablePlacement]
    ) -> set[str]:
        """Get the set of backend IDs that hold the referenced tables."""
        return {placements[t].backend_id for t in tables if t in placements}

    def _estimate_execution_cost(
        self, root: LogicalNode, backend: str, total_rows: int
    ) -> CostVector:
        """Estimate execution cost of the full plan on a single backend."""
        be_type = backend.split("-")[0] if "-" in backend else backend

        # Walk the tree and sum up per-operator costs
        return self._node_cost(root, be_type, total_rows)

    def _node_cost(self, node: LogicalNode, backend: str, rows: int) -> CostVector:
        """Recursively estimate cost for a node."""
        if isinstance(node, ScanNode):
            size = rows * 256
            return self._cost_model.scan_cost(rows, size, 1.0, backend)

        if isinstance(node, FilterNode):
            child_cost = self._node_cost(node.child, backend, rows)
            return child_cost  # filter adds minimal CPU cost

        if isinstance(node, JoinNode):
            left_rows = max(1, rows // 2)
            right_rows = max(1, rows // 2)
            left_cost = self._node_cost(node.left, backend, left_rows)
            right_cost = self._node_cost(node.right, backend, right_rows)
            join_cost = self._cost_model.join_cost(left_rows, right_rows, "hash", backend)
            return left_cost + right_cost + join_cost

        if isinstance(node, AggregateNode):
            child_cost = self._node_cost(node.child, backend, rows)
            groups = max(1, len(node.group_by) * 100) if node.group_by else 1
            agg_cost = self._cost_model.agg_cost(rows, groups, backend)
            return child_cost + agg_cost

        if isinstance(node, SortNode):
            child_cost = self._node_cost(node.child, backend, rows)
            sort_cost = CostVector(
                latency_ms=rows * math.log2(max(2, rows)) * 0.000001 * 1000,
                cpu_units=rows * math.log2(max(2, rows)) * 0.000001,
            )
            return child_cost + sort_cost

        # Default: sum child costs
        total = CostVector()
        for child in node.children:
            total = total + self._node_cost(child, backend, rows)
        return total

    def _extract_local_subtree(
        self, node: LogicalNode, local_tables: set[str]
    ) -> Optional[LogicalNode]:
        """Extract the sub-tree that only references tables in local_tables.

        Returns the node if all its scan nodes reference local tables, else None.
        """
        tables = self._collect_tables(node)
        if tables and tables.issubset(local_tables):
            return node

        # For scans, check directly
        if isinstance(node, ScanNode):
            return node if node.table_name in local_tables else None

        # For filters/projects, check child
        if hasattr(node, "child") and node.child:
            child_sub = self._extract_local_subtree(node.child, local_tables)
            if child_sub:
                return node
            return None

        return None

    def _has_filter(self, node: LogicalNode) -> bool:
        """Check if the node tree contains a FilterNode."""
        if isinstance(node, FilterNode):
            return True
        return any(self._has_filter(c) for c in node.children)
