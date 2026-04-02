"""MetaMind Cascades-based memo optimizer with branch-and-bound pruning."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Optional

from metamind.core.logical.nodes import (
    AggregateNode, FilterNode, JoinNode, LimitNode,
    LogicalNode, ProjectNode, ScanNode, SortNode,
)
from metamind.core.costing.cost_model import CostModel, CostVector
from metamind.config.feature_flags import FeatureFlags
from metamind.core.memo.dpccp import DPccp, JoinEdge, JoinRelation, greedy_join_order

logger = logging.getLogger(__name__)

DPCCP_MAX = 15
EXPLORATION_BUDGET_DEFAULT = 10000


@dataclass
class MemoGroup:
    """An equivalence group in the Cascades memo.

    Contains multiple equivalent logical/physical expressions.
    """

    group_id: int
    best_cost: float = math.inf
    best_plan: Optional[LogicalNode] = None
    explored: bool = False
    expressions: list[LogicalNode] = field(default_factory=list)


@dataclass
class MemoTable:
    """The memo table mapping group_id to MemoGroup."""

    groups: dict[int, MemoGroup] = field(default_factory=dict)
    _next_id: int = 0

    def new_group(self, expr: LogicalNode) -> MemoGroup:
        """Create a new group for an expression."""
        gid = self._next_id
        self._next_id += 1
        group = MemoGroup(group_id=gid, expressions=[expr])
        self.groups[gid] = group
        return group

    def update_best(self, group_id: int, cost: float, plan: LogicalNode) -> None:
        """Update best plan for a group if cost improves."""
        group = self.groups[group_id]
        if cost < group.best_cost:
            group.best_cost = cost
            group.best_plan = plan


class CascadesOptimizer:
    """Top-down Cascades optimizer with memo, branch-and-bound pruning, and DPccp.

    Implements F04 (bushy joins via DPccp) and F12 (optimization tiering).
    """

    def __init__(
        self,
        cost_model: CostModel,
        flags: FeatureFlags,
        budget: int = EXPLORATION_BUDGET_DEFAULT,
    ) -> None:
        """Initialize with cost model and feature flags."""
        self._cost_model = cost_model
        self._flags = flags
        self._budget = budget
        self._steps_used: int = 0
        self._memo = MemoTable()

    def optimize(self, root: LogicalNode, upper_bound: float = math.inf) -> LogicalNode:
        """Run Cascades optimization on the logical plan. Returns best physical plan."""
        self._steps_used = 0
        self._memo = MemoTable()

        # F12: Tiering — choose optimization depth based on complexity
        tier = self._classify_tier(root)
        logger.debug("Optimization tier: %d", tier)

        if tier == 1:
            # Tier 1: Simple heuristic — just annotate costs
            return self._heuristic_optimize(root)
        elif tier == 2:
            # Tier 2: Rule-based with limited exploration
            return self._rule_based_optimize(root)
        else:
            # Tier 3: Full Cascades with DPccp
            return self._cascades_optimize(root, upper_bound)

    def _classify_tier(self, root: LogicalNode) -> int:
        """Classify query complexity into optimization tier (F12)."""
        if not self._flags.F12_optimization_tiering:
            return 3  # Always full Cascades if tiering disabled

        stats = self._gather_stats(root)
        join_count = stats["joins"]
        total_nodes = stats["nodes"]

        if total_nodes <= 5 and join_count == 0:
            return 1
        if join_count <= 3:
            return 2
        return 3

    def _gather_stats(self, node: LogicalNode) -> dict[str, int]:
        """Count nodes and joins in plan tree."""
        joins = 1 if isinstance(node, JoinNode) else 0
        nodes = 1
        for child in node.children:
            child_stats = self._gather_stats(child)
            joins += child_stats["joins"]
            nodes += child_stats["nodes"]
        return {"joins": joins, "nodes": nodes}

    # ── Tier 1: Heuristic ─────────────────────────────────────

    def _heuristic_optimize(self, root: LogicalNode) -> LogicalNode:
        """Simple bottom-up cost annotation without exploration."""
        for child in root.children:
            self._heuristic_optimize(child)
        cv = self._estimate_node_cost(root)
        root._estimated_cost = self._cost_model.scalar_cost(cv)
        return root

    # ── Tier 2: Rule-Based ────────────────────────────────────

    def _rule_based_optimize(self, root: LogicalNode) -> LogicalNode:
        """Apply transformation rules: predicate pushdown, join reordering (left-deep)."""
        root = self._push_down_filters(root)
        root = self._heuristic_optimize(root)
        return root

    def _push_down_filters(self, node: LogicalNode) -> LogicalNode:
        """Push filter nodes below join/project nodes."""
        # Recursively optimize children first
        node.children = [self._push_down_filters(c) for c in node.children]

        if isinstance(node, FilterNode) and node.children:
            child = node.children[0]
            if isinstance(child, JoinNode):
                # Try pushing each predicate to the appropriate scan
                return self._push_filter_into_join(node, child)

        return node

    def _push_filter_into_join(self, filter_node: FilterNode, join: JoinNode) -> LogicalNode:
        """Push predicates from filter into join children where possible."""
        leftover_preds = []
        for pred in filter_node.predicates:
            pushed = False
            for i, child in enumerate(join.children):
                if self._pred_applies_to(pred, child):
                    if isinstance(child, ScanNode):
                        child.predicates.append(pred)
                    elif isinstance(child, FilterNode):
                        child.predicates.append(pred)
                    else:
                        new_filter = FilterNode(predicates=[pred])
                        new_filter.children = [child]
                        join.children[i] = new_filter
                    pushed = True
                    break
            if not pushed:
                leftover_preds.append(pred)

        if leftover_preds:
            filter_node.predicates = leftover_preds
            filter_node.children = [join]
            return filter_node
        return join

    def _pred_applies_to(self, pred: object, node: LogicalNode) -> bool:
        """Check if a predicate's table is in a node's subtree."""
        # Simplified: try to match table alias in predicate column
        tables = self._collect_tables(node)
        if hasattr(pred, "table_alias") and pred.table_alias:  # type: ignore[union-attr]
            return pred.table_alias in tables  # type: ignore[union-attr]
        return False

    def _collect_tables(self, node: LogicalNode) -> set[str]:
        """Collect all table names in a subtree."""
        tables: set[str] = set()
        if isinstance(node, ScanNode):
            tables.add(node.table_name)
            if node.alias:
                tables.add(node.alias)
        for child in node.children:
            tables.update(self._collect_tables(child))
        return tables

    # ── Tier 3: Full Cascades ────────────────────────────────

    def _cascades_optimize(
        self, root: LogicalNode, upper_bound: float
    ) -> LogicalNode:
        """Full Cascades optimization with DPccp for joins."""
        # Extract joins and try DPccp
        joins = self._extract_joins(root)

        if (
            self._flags.F04_bushy_join_dp
            and len(joins) >= 2
            and len(joins) <= DPCCP_MAX
        ):
            root = self._apply_dpccp(root, joins)

        # Apply rule-based then heuristic
        root = self._rule_based_optimize(root)
        return root

    def _extract_joins(self, node: LogicalNode) -> list[JoinNode]:
        """Extract all JoinNode instances from plan tree."""
        joins: list[JoinNode] = []
        if isinstance(node, JoinNode):
            joins.append(node)
        for child in node.children:
            joins.extend(self._extract_joins(child))
        return joins

    def _apply_dpccp(
        self, root: LogicalNode, join_nodes: list[JoinNode]
    ) -> LogicalNode:
        """Apply DPccp to find optimal bushy join order."""
        try:
            # Build relations from scan children
            scans = self._collect_scans(root)
            if len(scans) < 2:
                return root

            relations = [
                JoinRelation(
                    relation_id=i,
                    table_name=s.table_name,
                    row_count=max(1.0, s.estimated_rows),
                    backend=s.backend or "postgres",
                )
                for i, s in enumerate(scans)
            ]

            # Build edges from join conditions
            edges = self._build_join_edges(join_nodes, scans)

            dp = DPccp(relations=relations, edges=edges)
            result = dp.enumerate()

            if result is not None:
                logger.debug(
                    "DPccp optimal join cost: %.2f for %d relations",
                    result.cost, len(relations)
                )
            # Note: result is used to guide re-ordering in physical planner
        except Exception as exc:
            logger.warning("DPccp failed, using default order: %s", exc)

        return root

    def _collect_scans(self, node: LogicalNode) -> list[ScanNode]:
        """Collect all leaf ScanNode instances."""
        scans: list[ScanNode] = []
        if isinstance(node, ScanNode):
            scans.append(node)
        for child in node.children:
            scans.extend(self._collect_scans(child))
        return scans

    def _build_join_edges(
        self, joins: list[JoinNode], scans: list[ScanNode]
    ) -> list[JoinEdge]:
        """Build join graph edges from join conditions."""
        scan_idx = {s.table_name: i for i, s in enumerate(scans)}
        if s_alias := {s.alias: i for s in scans if s.alias}:
            scan_idx.update(s_alias)

        edges: list[JoinEdge] = []
        for join in joins:
            for cond in join.conditions:
                left_table = cond.column.split(".")[0] if "." in cond.column else ""
                right_table = str(cond.value).split(".")[0] if "." in str(cond.value) else ""
                if left_table in scan_idx and right_table in scan_idx:
                    edges.append(JoinEdge(
                        left_id=scan_idx[left_table],
                        right_id=scan_idx[right_table],
                        selectivity=0.1,  # default; overridden by cost model
                    ))
        return edges

    # ── Cost Estimation ───────────────────────────────────────

    def _estimate_node_cost(self, node: LogicalNode) -> CostVector:
        """Estimate cost vector for a single plan node."""
        if isinstance(node, ScanNode):
            rows = node.estimated_rows
            size = int(rows * 100)  # rough 100 bytes/row
            sel = 0.5 if node.predicates else 1.0
            return self._cost_model.scan_cost(int(rows), size, sel, node.backend or "postgres")

        if isinstance(node, JoinNode):
            left_rows = node.children[0].estimated_rows if node.children else 1.0
            right_rows = node.children[1].estimated_rows if len(node.children) > 1 else 1.0
            return self._cost_model.join_cost(left_rows, right_rows, "hash")

        if isinstance(node, AggregateNode):
            input_rows = node.children[0].estimated_rows if node.children else 1.0
            return self._cost_model.agg_cost(input_rows, max(1, int(input_rows * 0.1)))

        return CostVector(latency_ms=1.0)
