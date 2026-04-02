"""
Trino / Presto SQL Dialect Generator

File: metamind/execution/dialects/trino_gen.py
Role: Senior Database Engineer
Addresses: Gap identified in comparative audit — Trino engine had no dialect translator.

Converts MetaMind logical plan nodes into Trino-compatible SQL,
including Iceberg-specific syntax, Lambda expressions, and
Trino's native approximate aggregation functions.

Trino dialect differences vs ANSI SQL:
- LIMIT uses standard LIMIT clause (not FETCH FIRST)
- UNNEST for array expansion
- APPROX_DISTINCT instead of COUNT(DISTINCT)
- ROW_NUMBER / RANK window functions
- Lambda predicates: filter(array, x -> condition)
- Date arithmetic: date_add / date_diff
- Iceberg time-travel: FOR TIMESTAMP AS OF / FOR VERSION AS OF
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class TrinoJoinType(Enum):
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"
    CROSS = "CROSS JOIN"


@dataclass
class TrinoDialectOptions:
    """Tuning knobs for Trino SQL generation."""
    use_approx_distinct: bool = True      # Use APPROX_DISTINCT for COUNT(DISTINCT)
    approx_distinct_accuracy: float = 0.023
    enable_iceberg_time_travel: bool = False
    time_travel_timestamp: Optional[str] = None
    partition_pruning_hints: bool = True
    max_pushdown_predicates: int = 50


@dataclass
class GeneratedSQL:
    """Output of the Trino SQL generator."""
    sql: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    estimated_scan_rows: int = 0
    uses_approx: bool = False
    partition_filters: List[str] = field(default_factory=list)
    hints: List[str] = field(default_factory=list)


class TrinoSQLGenerator:
    """
    Translates MetaMind logical plan nodes into Trino-compatible SQL.

    Handles Trino-specific syntax for:
    - Iceberg tables (time-travel, partition pruning)
    - Approximate aggregations (APPROX_DISTINCT, APPROX_PERCENTILE)
    - Array/map operations
    - Cross-engine federated sub-selects
    """

    # Maps MetaMind operator tokens → Trino operator strings
    _OP_MAP: Dict[str, str] = {
        "=": "=", "!=": "<>", "<>": "<>",
        "<": "<", ">": ">", "<=": "<=", ">=": ">=",
        "like": "LIKE", "in": "IN", "not in": "NOT IN",
        "is null": "IS NULL", "is not null": "IS NOT NULL",
        "between": "BETWEEN",
    }

    # Trino aggregate function mappings
    _AGG_MAP: Dict[str, str] = {
        "count": "COUNT",
        "sum": "SUM",
        "avg": "AVG",
        "min": "MIN",
        "max": "MAX",
        "stddev": "STDDEV",
        "variance": "VARIANCE",
        "count_distinct": "APPROX_DISTINCT",  # replaced below if disabled
        "approx_percentile": "APPROX_PERCENTILE",
    }

    def __init__(self, options: Optional[TrinoDialectOptions] = None) -> None:
        self.options = options or TrinoDialectOptions()
        if not self.options.use_approx_distinct:
            self._AGG_MAP["count_distinct"] = "COUNT(DISTINCT {})"

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def generate(self, plan: Dict[str, Any], tenant_id: str) -> GeneratedSQL:
        """
        Generate Trino SQL from a MetaMind logical plan dict.

        Args:
            plan:      MetaMind plan node (nested dict from logical planner)
            tenant_id: Tenant for schema qualification

        Returns:
            GeneratedSQL with the final SQL and metadata
        """
        result = GeneratedSQL(sql="")
        sql = self._render_node(plan, result, tenant_id)
        result.sql = sql
        return result

    def generate_from_sql(self, original_sql: str) -> GeneratedSQL:
        """
        Pass-through with Trino-specific rewrites for raw SQL strings.
        Applies dialect corrections without a full plan round-trip.
        """
        sql = original_sql
        sql = self._rewrite_limit(sql)
        sql = self._rewrite_date_functions(sql)
        sql = self._inject_iceberg_time_travel(sql)
        return GeneratedSQL(sql=sql)

    # ------------------------------------------------------------------
    # Node rendering
    # ------------------------------------------------------------------

    def _render_node(
        self,
        node: Dict[str, Any],
        result: GeneratedSQL,
        tenant_id: str,
    ) -> str:
        node_type = node.get("type", "unknown").upper()
        dispatch = {
            "SCAN": self._render_scan,
            "FILTER": self._render_filter,
            "PROJECT": self._render_project,
            "JOIN": self._render_join,
            "AGGREGATE": self._render_aggregate,
            "SORT": self._render_sort,
            "LIMIT": self._render_limit,
            "UNION": self._render_union,
            "FEDERATEDSCAN": self._render_federated_scan,
        }
        renderer = dispatch.get(node_type)
        if renderer is None:
            logger.warning("Unknown plan node type: %s — falling back to raw SQL", node_type)
            return node.get("raw_sql", "-- unknown node")
        return renderer(node, result, tenant_id)

    def _render_scan(
        self,
        node: Dict[str, Any],
        result: GeneratedSQL,
        tenant_id: str,
    ) -> str:
        table = node.get("table", "unknown_table")
        alias = node.get("alias", "")
        schema = node.get("schema", "iceberg")
        columns = node.get("columns", ["*"])

        col_list = ", ".join(columns) if columns else "*"
        qualified = f"{schema}.{table}"

        # Iceberg time-travel
        time_travel = ""
        if self.options.enable_iceberg_time_travel and self.options.time_travel_timestamp:
            time_travel = f" FOR TIMESTAMP AS OF TIMESTAMP '{self.options.time_travel_timestamp}'"
            result.hints.append(f"iceberg_time_travel:{self.options.time_travel_timestamp}")

        base = f"SELECT {col_list}\nFROM {qualified}{time_travel}"
        if alias:
            base = f"SELECT {col_list}\nFROM {qualified}{time_travel} AS {alias}"

        # Partition pruning comment hint
        partition_keys = node.get("partition_keys", [])
        if partition_keys and self.options.partition_pruning_hints:
            hint = ", ".join(partition_keys)
            base = f"-- /*+ partition_filter({hint}) */\n{base}"

        result.estimated_scan_rows = node.get("estimated_rows", 0)
        return base

    def _render_filter(
        self,
        node: Dict[str, Any],
        result: GeneratedSQL,
        tenant_id: str,
    ) -> str:
        child_sql = self._render_node(node["child"], result, tenant_id)
        predicates = node.get("predicates", [])
        where_clause = self._render_predicates(predicates, result)
        if not where_clause:
            return child_sql
        return f"SELECT *\nFROM (\n  {self._indent(child_sql)}\n) AS _filter\nWHERE {where_clause}"

    def _render_project(
        self,
        node: Dict[str, Any],
        result: GeneratedSQL,
        tenant_id: str,
    ) -> str:
        child_sql = self._render_node(node["child"], result, tenant_id)
        expressions = node.get("expressions", ["*"])
        select_list = ", ".join(expressions)
        return f"SELECT {select_list}\nFROM (\n  {self._indent(child_sql)}\n) AS _project"

    def _render_join(
        self,
        node: Dict[str, Any],
        result: GeneratedSQL,
        tenant_id: str,
    ) -> str:
        left_sql = self._render_node(node["left"], result, tenant_id)
        right_sql = self._render_node(node["right"], result, tenant_id)
        join_type = TrinoJoinType[node.get("join_type", "INNER").upper()].value
        condition = node.get("condition", "TRUE")
        left_alias = node.get("left_alias", "_left")
        right_alias = node.get("right_alias", "_right")

        return (
            f"SELECT *\n"
            f"FROM (\n  {self._indent(left_sql)}\n) AS {left_alias}\n"
            f"{join_type} (\n  {self._indent(right_sql)}\n) AS {right_alias}\n"
            f"ON {condition}"
        )

    def _render_aggregate(
        self,
        node: Dict[str, Any],
        result: GeneratedSQL,
        tenant_id: str,
    ) -> str:
        child_sql = self._render_node(node["child"], result, tenant_id)
        group_by = node.get("group_by", [])
        aggregates = node.get("aggregates", [])

        select_parts: List[str] = list(group_by)
        for agg in aggregates:
            select_parts.append(self._render_aggregate_expr(agg, result))

        select_list = ", ".join(select_parts) if select_parts else "1"
        group_clause = ""
        if group_by:
            group_clause = f"\nGROUP BY {', '.join(group_by)}"

        having = node.get("having")
        having_clause = f"\nHAVING {having}" if having else ""

        return (
            f"SELECT {select_list}\n"
            f"FROM (\n  {self._indent(child_sql)}\n) AS _agg"
            f"{group_clause}{having_clause}"
        )

    def _render_sort(
        self,
        node: Dict[str, Any],
        result: GeneratedSQL,
        tenant_id: str,
    ) -> str:
        child_sql = self._render_node(node["child"], result, tenant_id)
        order_by = node.get("order_by", [])
        if not order_by:
            return child_sql
        order_clause = ", ".join(
            f"{col} {direction.upper()}"
            for col, direction in (
                (o["column"], o.get("direction", "ASC")) for o in order_by
            )
        )
        return f"SELECT *\nFROM (\n  {self._indent(child_sql)}\n) AS _sort\nORDER BY {order_clause}"

    def _render_limit(
        self,
        node: Dict[str, Any],
        result: GeneratedSQL,
        tenant_id: str,
    ) -> str:
        child_sql = self._render_node(node["child"], result, tenant_id)
        limit = node.get("limit", 1000)
        offset = node.get("offset", 0)
        offset_clause = f"\nOFFSET {offset}" if offset else ""
        return (
            f"SELECT *\nFROM (\n  {self._indent(child_sql)}\n) AS _limit"
            f"\nLIMIT {limit}{offset_clause}"
        )

    def _render_union(
        self,
        node: Dict[str, Any],
        result: GeneratedSQL,
        tenant_id: str,
    ) -> str:
        parts = [self._render_node(child, result, tenant_id) for child in node.get("children", [])]
        keyword = "UNION ALL" if node.get("all", True) else "UNION"
        separator = f"\n{keyword}\n"
        return separator.join(f"(\n  {self._indent(p)}\n)" for p in parts)

    def _render_federated_scan(
        self,
        node: Dict[str, Any],
        result: GeneratedSQL,
        tenant_id: str,
    ) -> str:
        """Render a federated scan using Trino's cross-catalog query syntax."""
        catalog = node.get("catalog", "iceberg")
        schema = node.get("schema", "default")
        table = node.get("table", "unknown")
        columns = node.get("columns", ["*"])
        col_list = ", ".join(columns)
        qualified = f'"{catalog}"."{schema}"."{table}"'
        result.hints.append(f"federated_source:{catalog}")
        return f"SELECT {col_list}\nFROM {qualified}"

    # ------------------------------------------------------------------
    # Aggregate expression rendering
    # ------------------------------------------------------------------

    def _render_aggregate_expr(self, agg: Dict[str, Any], result: GeneratedSQL) -> str:
        func = agg.get("function", "count").lower()
        column = agg.get("column", "*")
        alias = agg.get("alias", "")
        distinct = agg.get("distinct", False)
        percentile = agg.get("percentile")

        if func == "count" and distinct and self.options.use_approx_distinct:
            accuracy = self.options.approx_distinct_accuracy
            expr = f"APPROX_DISTINCT({column}, {accuracy})"
            result.uses_approx = True
        elif func == "approx_percentile" and percentile is not None:
            expr = f"APPROX_PERCENTILE({column}, {percentile})"
            result.uses_approx = True
        elif func == "count" and distinct:
            expr = f"COUNT(DISTINCT {column})"
        else:
            trino_func = self._AGG_MAP.get(func, func.upper())
            expr = f"{trino_func}({column})"

        return f"{expr} AS {alias}" if alias else expr

    # ------------------------------------------------------------------
    # Predicate rendering
    # ------------------------------------------------------------------

    def _render_predicates(
        self,
        predicates: List[Dict[str, Any]],
        result: GeneratedSQL,
    ) -> str:
        if not predicates:
            return ""
        parts = [self._render_predicate(p, result) for p in predicates]
        return " AND ".join(f"({p})" for p in parts if p)

    def _render_predicate(self, pred: Dict[str, Any], result: GeneratedSQL) -> str:
        col = pred.get("column", "1")
        op = self._OP_MAP.get(pred.get("operator", "=").lower(), "=")
        val = pred.get("value")

        if op in ("IS NULL", "IS NOT NULL"):
            return f"{col} {op}"
        if op == "IN" and isinstance(val, list):
            placeholders = ", ".join(self._quote(v) for v in val)
            return f"{col} IN ({placeholders})"
        if op == "NOT IN" and isinstance(val, list):
            placeholders = ", ".join(self._quote(v) for v in val)
            return f"{col} NOT IN ({placeholders})"
        if op == "BETWEEN" and isinstance(val, (list, tuple)) and len(val) == 2:
            return f"{col} BETWEEN {self._quote(val[0])} AND {self._quote(val[1])}"
        return f"{col} {op} {self._quote(val)}"

    # ------------------------------------------------------------------
    # Trino-specific SQL rewrites (for raw SQL pass-through)
    # ------------------------------------------------------------------

    def _rewrite_limit(self, sql: str) -> str:
        """Ensure FETCH FIRST … ROWS ONLY → LIMIT … (Trino prefers LIMIT)."""
        import re
        return re.sub(
            r"FETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY",
            r"LIMIT \1",
            sql,
            flags=re.IGNORECASE,
        )

    def _rewrite_date_functions(self, sql: str) -> str:
        """Rewrite DATEADD → date_add, DATEDIFF → date_diff."""
        import re
        sql = re.sub(r"\bDATEADD\s*\(", "date_add(", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bDATEDIFF\s*\(", "date_diff(", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bGETDATE\s*\(\s*\)", "NOW()", sql, flags=re.IGNORECASE)
        return sql

    def _inject_iceberg_time_travel(self, sql: str) -> str:
        """Append FOR TIMESTAMP AS OF clause if time-travel is enabled."""
        if not (self.options.enable_iceberg_time_travel and self.options.time_travel_timestamp):
            return sql
        ts = self.options.time_travel_timestamp
        import re
        return re.sub(
            r"\bFROM\s+(\w+\.\w+)\b(?!\s+FOR\s+TIMESTAMP)",
            rf"FROM \1 FOR TIMESTAMP AS OF TIMESTAMP '{ts}'",
            sql,
            flags=re.IGNORECASE,
        )

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _indent(sql: str, spaces: int = 2) -> str:
        pad = " " * spaces
        return "\n".join(pad + line for line in sql.splitlines())

    @staticmethod
    def _quote(val: Any) -> str:
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return "TRUE" if val else "FALSE"
        if isinstance(val, (int, float)):
            return str(val)
        # Escape single quotes
        escaped = str(val).replace("'", "''")
        return f"'{escaped}'"
