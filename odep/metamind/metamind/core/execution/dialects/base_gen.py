"""Base dialect SQL generator — shared utilities for all dialect generators (F17)."""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from metamind.core.logical.nodes import (
    AggregateNode,
    FilterNode,
    JoinNode,
    LogicalNode,
    ProjectNode,
    ScanNode,
    SortNode,
    LimitNode,
)

logger = logging.getLogger(__name__)


def _try_transpile(sql: str, read_dialect: str, write_dialect: str) -> str:
    """Attempt sqlglot transpile, falling back to the original SQL."""
    try:
        import sqlglot  # type: ignore[import]
        result = sqlglot.transpile(sql, read=read_dialect, write=write_dialect, pretty=True)
        return result[0] if result else sql
    except Exception as exc:
        logger.debug("sqlglot transpile failed (%s→%s): %s", read_dialect, write_dialect, exc)
        return sql


def node_to_sql(node: LogicalNode, dialect: str = "postgres") -> str:
    """Convert a LogicalNode tree to SQL for the given dialect.

    Performs a recursive depth-first traversal of the logical plan tree
    and emits SQL fragments for each node type.

    Args:
        node: Root of the logical plan tree.
        dialect: Target SQL dialect.

    Returns:
        A SQL string representing the node tree.
    """
    if isinstance(node, ScanNode):
        table = node.table_name
        if node.alias:
            table = f"{table} AS {node.alias}"
        if node.sample_pct and node.sample_pct < 100:
            table = f"{table} TABLESAMPLE SYSTEM ({node.sample_pct})"
        return f"SELECT * FROM {table}"

    if isinstance(node, FilterNode):
        child_sql = node_to_sql(node.child, dialect)
        predicates = _build_predicate_clause(node)
        return f"SELECT * FROM ({child_sql}) AS _filter WHERE {predicates}"

    if isinstance(node, ProjectNode):
        child_sql = node_to_sql(node.child, dialect)
        cols = ", ".join(node.columns) if node.columns else "*"
        return f"SELECT {cols} FROM ({child_sql}) AS _project"

    if isinstance(node, AggregateNode):
        child_sql = node_to_sql(node.child, dialect)
        agg_exprs = ", ".join(
            f"{e.func.value.upper()}({e.column}) AS {e.alias or e.column}"
            for e in node.aggregates
        )
        group_by = ""
        if node.group_by:
            group_by = " GROUP BY " + ", ".join(node.group_by)
        proj = ", ".join(node.group_by + [agg_exprs]) if node.group_by else agg_exprs
        return f"SELECT {proj} FROM ({child_sql}) AS _agg{group_by}"

    if isinstance(node, JoinNode):
        left_sql = node_to_sql(node.left, dialect)
        right_sql = node_to_sql(node.right, dialect)
        jtype = node.join_type.value.upper()
        cond = node.condition or "TRUE"
        return (
            f"SELECT * FROM ({left_sql}) AS _left "
            f"{jtype} JOIN ({right_sql}) AS _right ON {cond}"
        )

    if isinstance(node, SortNode):
        child_sql = node_to_sql(node.child, dialect)
        order_parts = [
            f"{k.column} {k.direction.value.upper()}" for k in node.sort_keys
        ]
        order_by = ", ".join(order_parts)
        return f"SELECT * FROM ({child_sql}) AS _sort ORDER BY {order_by}"

    if isinstance(node, LimitNode):
        child_sql = node_to_sql(node.child, dialect)
        offset_clause = f" OFFSET {node.offset}" if node.offset else ""
        return f"SELECT * FROM ({child_sql}) AS _limit LIMIT {node.limit}{offset_clause}"

    # Fallback for unknown node types
    return "SELECT 1 -- unsupported node: " + type(node).__name__


def _build_predicate_clause(node: FilterNode) -> str:
    """Render filter node predicates as a SQL WHERE clause fragment."""
    if not node.predicates:
        return "TRUE"
    parts: list[str] = []
    for p in node.predicates:
        col = p.qualified_name
        op = p.operator
        val = p.value
        if op.upper() == "IN":
            vals_str = ", ".join(repr(v) for v in (val if isinstance(val, list) else [val]))
            parts.append(f"{col} IN ({vals_str})")
        elif op.upper() == "IS NULL":
            parts.append(f"{col} IS NULL")
        elif op.upper() == "IS NOT NULL":
            parts.append(f"{col} IS NOT NULL")
        elif op.upper() == "BETWEEN":
            lo, hi = val if isinstance(val, (list, tuple)) and len(val) == 2 else (val, val)
            parts.append(f"{col} BETWEEN {repr(lo)} AND {repr(hi)}")
        elif op.upper() == "LIKE":
            parts.append(f"{col} LIKE {repr(val)}")
        else:
            parts.append(f"{col} {op} {repr(val)}")
    return " AND ".join(parts)
