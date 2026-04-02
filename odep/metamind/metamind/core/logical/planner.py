"""
Cost-Based Query Planner

File: metamind/core/logical/planner.py
Role: Query Optimizer Engineer
Phase: 1
Dependencies: sqlglot

Implements cost-based query planning with:
- Logical plan extraction
- Engine cost simulation
- Cardinality estimation
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set
from enum import Enum

import sqlglot
from sqlglot import Expression
from metamind.core.catalog.hll_cardinality import HLLCardinalityEstimator  # Task 13

logger = logging.getLogger(__name__)


class PlanNodeType(Enum):
    """Logical plan node types."""
    SCAN = "scan"
    FILTER = "filter"
    PROJECT = "project"
    AGGREGATE = "aggregate"
    JOIN = "join"
    SORT = "sort"
    LIMIT = "limit"
    UNION = "union"
    SUBQUERY = "subquery"


class JoinType(Enum):
    """Join types."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


@dataclass
class LogicalPlanNode:
    """A node in the logical query plan."""
    node_type: PlanNodeType
    node_id: str
    
    # Node-specific properties
    table_name: Optional[str] = None
    columns: List[str] = field(default_factory=list)
    filter_conditions: List[str] = field(default_factory=list)
    join_type: Optional[JoinType] = None
    join_condition: Optional[str] = None
    left_child: Optional["LogicalPlanNode"] = None
    right_child: Optional["LogicalPlanNode"] = None
    aggregate_functions: List[str] = field(default_factory=list)
    group_by_columns: List[str] = field(default_factory=list)
    order_by_columns: List[str] = field(default_factory=list)
    limit_value: Optional[int] = None
    
    # Cost estimates
    estimated_rows: int = 0
    estimated_bytes: int = 0
    estimated_cost: float = 0.0
    
    # Statistics
    selectivity: float = 1.0
    output_cardinality: int = 0


@dataclass
class EngineCostModel:
    """Cost model for a specific engine."""
    engine_name: str
    
    # Base costs (in milliseconds)
    base_scan_cost_per_row: float = 0.001
    base_filter_cost_per_row: float = 0.0005
    base_join_cost_per_row: float = 0.01
    base_aggregate_cost_per_row: float = 0.005
    base_sort_cost_per_row: float = 0.01
    base_network_cost_per_mb: float = 10.0
    
    # Scaling factors
    io_factor: float = 1.0
    cpu_factor: float = 1.0
    memory_factor: float = 1.0
    network_factor: float = 1.0
    
    # Overhead
    startup_overhead_ms: float = 100.0
    shutdown_overhead_ms: float = 50.0


# Import from the dedicated module so callers get a single source of truth
from metamind.core.logical.cardinality_estimator import (  # noqa: E402
    CardinalityEstimate,
    CardinalityEstimator,
)


class CostBasedPlanner:
    """
    Cost-based query planner.
    
    Extracts logical plans from SQL and estimates costs for different engines.
    """
    
    def __init__(self, catalog: Any) -> None:
        """
        Initialize cost-based planner.
        
        Args:
            catalog: Metadata catalog for table statistics
        """
        self.catalog = catalog
        self._node_counter = 0
        self._cardinality_estimator = CardinalityEstimator()
        # Task 13: HLL-based estimator (injected post-init when redis available)
        self._hll_estimator: HLLCardinalityEstimator | None = None
        
        # Engine cost models
        self.engine_costs = {
            "oracle": EngineCostModel(
                engine_name="oracle",
                base_scan_cost_per_row=0.003,
                base_join_cost_per_row=0.03,
                io_factor=1.5,
                cpu_factor=1.2,
                startup_overhead_ms=50.0
            ),
            "trino": EngineCostModel(
                engine_name="trino",
                base_scan_cost_per_row=0.001,
                base_join_cost_per_row=0.008,
                io_factor=0.8,
                network_factor=1.2,
                startup_overhead_ms=200.0
            ),
            "spark": EngineCostModel(
                engine_name="spark",
                base_scan_cost_per_row=0.0005,
                base_join_cost_per_row=0.005,
                io_factor=0.5,
                cpu_factor=0.8,
                startup_overhead_ms=5000.0,  # High overhead for batch jobs
                shutdown_overhead_ms=1000.0
            )
        }
        
        logger.debug("CostBasedPlanner initialized")
    
    def extract_logical_plan(self, sql: str) -> Optional[LogicalPlanNode]:
        """
        Extract logical plan from SQL.
        
        Args:
            sql: SQL query
            
        Returns:
            Root node of logical plan
        """
        try:
            parsed = sqlglot.parse_one(sql)
            self._node_counter = 0
            return self._build_plan_tree(parsed)
        except Exception as e:
            logger.error(f"Failed to extract logical plan: {e}")
            return None
    
    def _build_plan_tree(self, node: Expression) -> LogicalPlanNode:
        """Build plan tree from parsed SQL."""
        self._node_counter += 1
        node_id = f"node_{self._node_counter}"
        
        # Handle SELECT
        if isinstance(node, sqlglot.exp.Select):
            return self._build_select_plan(node, node_id)
        
        # Handle UNION
        if isinstance(node, sqlglot.exp.Union):
            return self._build_union_plan(node, node_id)
        
        # Default: treat as scan
        return LogicalPlanNode(
            node_type=PlanNodeType.SCAN,
            node_id=node_id
        )
    
    def _build_select_plan(
        self,
        select: sqlglot.exp.Select,
        node_id: str
    ) -> LogicalPlanNode:
        """Build plan for SELECT statement."""
        # Start with FROM clause (table scan)
        current_plan = None
        
        if select.args.get("from"):
            from_clause = select.args["from"]
            current_plan = self._build_from_plan(from_clause, f"{node_id}_from")
        
        # Add JOINs
        if select.args.get("joins"):
            for join in select.args["joins"]:
                current_plan = self._build_join_plan(
                    join, current_plan, f"{node_id}_join"
                )
        
        # Add WHERE (filter)
        if select.args.get("where"):
            current_plan = LogicalPlanNode(
                node_type=PlanNodeType.FILTER,
                node_id=f"{node_id}_filter",
                filter_conditions=[select.args["where"].sql()],
                left_child=current_plan
            )
        
        # Add GROUP BY (aggregate)
        if select.args.get("group"):
            agg_node = LogicalPlanNode(
                node_type=PlanNodeType.AGGREGATE,
                node_id=f"{node_id}_agg",
                group_by_columns=[c.name for c in select.args["group"].expressions],
                left_child=current_plan
            )
            
            # Extract aggregate functions
            for expr in select.expressions:
                if isinstance(expr, sqlglot.exp.AggFunc):
                    agg_node.aggregate_functions.append(expr.sql())
            
            current_plan = agg_node
        
        # Add ORDER BY (sort)
        if select.args.get("order"):
            current_plan = LogicalPlanNode(
                node_type=PlanNodeType.SORT,
                node_id=f"{node_id}_sort",
                order_by_columns=[c.sql() for c in select.args["order"].expressions],
                left_child=current_plan
            )
        
        # Add LIMIT
        if select.args.get("limit"):
            limit_value = select.args["limit"].expression
            if isinstance(limit_value, sqlglot.exp.Literal):
                current_plan = LogicalPlanNode(
                    node_type=PlanNodeType.LIMIT,
                    node_id=f"{node_id}_limit",
                    limit_value=int(limit_value.this),
                    left_child=current_plan
                )
        
        # Add PROJECT (column selection)
        columns = []
        for expr in select.expressions:
            if isinstance(expr, sqlglot.exp.Star):
                columns.append("*")
            elif hasattr(expr, "alias") and expr.alias:
                columns.append(expr.alias)
            elif hasattr(expr, "name"):
                columns.append(expr.name)
        
        if columns:
            current_plan = LogicalPlanNode(
                node_type=PlanNodeType.PROJECT,
                node_id=f"{node_id}_project",
                columns=columns,
                left_child=current_plan
            )
        
        return current_plan
    
    def _build_from_plan(
        self,
        from_clause: Any,
        node_id: str
    ) -> LogicalPlanNode:
        """Build plan for FROM clause."""
        if isinstance(from_clause, sqlglot.exp.Table):
            return LogicalPlanNode(
                node_type=PlanNodeType.SCAN,
                node_id=node_id,
                table_name=from_clause.name
            )
        
        if isinstance(from_clause, sqlglot.exp.Subquery):
            return self._build_plan_tree(from_clause.this)
        
        return LogicalPlanNode(
            node_type=PlanNodeType.SCAN,
            node_id=node_id
        )
    
    def _build_join_plan(
        self,
        join: sqlglot.exp.Join,
        left_plan: LogicalPlanNode,
        node_id: str
    ) -> LogicalPlanNode:
        """Build plan for JOIN clause."""
        # Build right side (table)
        right_plan = self._build_from_plan(join.this, f"{node_id}_right")
        
        # Determine join type
        join_type = JoinType.INNER
        if join.side:
            if join.side.upper() == "LEFT":
                join_type = JoinType.LEFT
            elif join.side.upper() == "RIGHT":
                join_type = JoinType.RIGHT
            elif join.side.upper() == "FULL":
                join_type = JoinType.FULL
        
        if join.kind and join.kind.upper() == "CROSS":
            join_type = JoinType.CROSS
        
        # Get join condition
        join_condition = None
        if join.args.get("on"):
            join_condition = join.args["on"].sql()
        
        return LogicalPlanNode(
            node_type=PlanNodeType.JOIN,
            node_id=node_id,
            join_type=join_type,
            join_condition=join_condition,
            left_child=left_plan,
            right_child=right_plan
        )
    
    def _build_union_plan(
        self,
        union: sqlglot.exp.Union,
        node_id: str
    ) -> LogicalPlanNode:
        """Build plan for UNION."""
        left = self._build_plan_tree(union.this)
        right = self._build_plan_tree(union.expression)
        
        return LogicalPlanNode(
            node_type=PlanNodeType.UNION,
            node_id=node_id,
            left_child=left,
            right_child=right
        )
    
    def estimate_cardinality(
        self,
        plan_node: "LogicalPlanNode",
        tenant_id: str = "default"
    ) -> "CardinalityEstimate":
        """Estimate cardinality for a plan node (delegates to CardinalityEstimator)."""
        return self._cardinality_estimator.estimate(plan_node, tenant_id)

    def simulate_engine_cost(
        self,
        plan_node: LogicalPlanNode,
        engine: str,
        tenant_id: str = "default"
    ) -> float:
        """
        Simulate execution cost for a specific engine.
        
        Args:
            plan_node: Root of plan tree
            engine: Engine name (oracle, trino, spark)
            tenant_id: Tenant identifier
            
        Returns:
            Estimated cost in milliseconds
        """
        if engine not in self.engine_costs:
            logger.warning(f"Unknown engine: {engine}, using trino costs")
            engine = "trino"
        
        cost_model = self.engine_costs[engine]
        cardinality = self.estimate_cardinality(plan_node, tenant_id)
        
        # Calculate cost recursively
        total_cost = self._calculate_node_cost(
            plan_node, cost_model, cardinality.estimated_rows
        )
        
        # Add overhead
        total_cost += cost_model.startup_overhead_ms
        
        return total_cost
    
    def _calculate_node_cost(
        self,
        node: LogicalPlanNode,
        cost_model: EngineCostModel,
        input_rows: int
    ) -> float:
        """Calculate cost for a plan node."""
        if node is None:
            return 0.0
        
        cost = 0.0
        
        if node.node_type == PlanNodeType.SCAN:
            cost = input_rows * cost_model.base_scan_cost_per_row * cost_model.io_factor
        
        elif node.node_type == PlanNodeType.FILTER:
            cost = input_rows * cost_model.base_filter_cost_per_row * cost_model.cpu_factor
            cost += self._calculate_node_cost(node.left_child, cost_model, input_rows)
        
        elif node.node_type == PlanNodeType.JOIN:
            left_rows = input_rows  # Simplified
            right_rows = input_rows
            cost = (left_rows + right_rows) * cost_model.base_join_cost_per_row * cost_model.cpu_factor
            cost += self._calculate_node_cost(node.left_child, cost_model, left_rows)
            cost += self._calculate_node_cost(node.right_child, cost_model, right_rows)
        
        elif node.node_type == PlanNodeType.AGGREGATE:
            cost = input_rows * cost_model.base_aggregate_cost_per_row * cost_model.cpu_factor
            cost += self._calculate_node_cost(node.left_child, cost_model, input_rows)
        
        elif node.node_type == PlanNodeType.SORT:
            cost = input_rows * cost_model.base_sort_cost_per_row * cost_model.cpu_factor
            cost += self._calculate_node_cost(node.left_child, cost_model, input_rows)
        
        elif node.node_type == PlanNodeType.LIMIT:
            cost += self._calculate_node_cost(node.left_child, cost_model, input_rows)
        
        else:
            cost += self._calculate_node_cost(node.left_child, cost_model, input_rows)
        
        return cost
    
    def get_plan_summary(self, plan_node: LogicalPlanNode) -> Dict[str, Any]:
        """Get human-readable plan summary."""
        def _summarize(node: LogicalPlanNode, depth: int = 0) -> Dict[str, Any]:
            if node is None:
                return {}
            
            summary = {
                "type": node.node_type.value,
                "id": node.node_id,
                "estimated_rows": node.estimated_rows,
                "estimated_cost": round(node.estimated_cost, 2)
            }
            
            if node.table_name:
                summary["table"] = node.table_name
            
            if node.join_type:
                summary["join_type"] = node.join_type.value
            
            if node.columns:
                summary["columns"] = node.columns[:5]  # Limit columns shown
            
            if node.left_child:
                summary["left"] = _summarize(node.left_child, depth + 1)
            
            if node.right_child:
                summary["right"] = _summarize(node.right_child, depth + 1)
            
            return summary
        
        return _summarize(plan_node)
