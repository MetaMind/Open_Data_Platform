"""MetaMind logical plan builder — converts sqlglot AST to LogicalNode tree."""
from __future__ import annotations

import logging
from typing import Optional

import sqlglot
import sqlglot.expressions as exp

from metamind.core.logical.nodes import (
    AggFunc,
    AggregateExpr,
    AggregateNode,
    FilterNode,
    JoinNode,
    JoinType,
    LimitNode,
    LogicalNode,
    Predicate,
    ProjectNode,
    ScanNode,
    SortDirection,
    SortKey,
    SortNode,
)

logger = logging.getLogger(__name__)


class LogicalPlanBuilder:
    """Builds a logical plan tree from a parsed sqlglot expression.

    Usage::

        builder = LogicalPlanBuilder(dialect="postgres")
        root = builder.build(sql)
    """

    def __init__(self, dialect: str = "postgres") -> None:
        """Initialize with target SQL dialect."""
        self._dialect = dialect

    def build(self, sql: str) -> LogicalNode:
        """Parse SQL string and build logical plan tree."""
        try:
            ast = sqlglot.parse_one(sql, dialect=self._dialect)
        except sqlglot.errors.ParseError as exc:
            raise ValueError(f"SQL parse error: {exc}") from exc

        return self._build_node(ast)

    def _build_node(self, expr: exp.Expression) -> LogicalNode:
        """Recursively build logical plan node from sqlglot expression."""
        if isinstance(expr, exp.Select):
            return self._build_select(expr)
        if isinstance(expr, exp.Union):
            # Treat UNION as a sub-query for now
            return self._build_select(expr.left)
        raise ValueError(f"Unsupported root expression type: {type(expr).__name__}")

    def _build_select(self, select: exp.Select) -> LogicalNode:
        """Build plan nodes from a SELECT statement."""
        # 1. FROM / JOIN → produces join tree or scan
        root: LogicalNode = self._build_from(select)

        # 2. WHERE predicates → push over from node
        where = select.args.get("where")
        if where:
            preds = self._extract_predicates(where)
            if preds:
                filter_node = FilterNode(predicates=preds)
                filter_node.children = [root]
                root = filter_node

        # 3. GROUP BY + aggregates
        group = select.args.get("group")
        agg_exprs = self._extract_aggregates(select)
        if group or agg_exprs:
            group_cols = self._extract_group_cols(group)
            having_preds: list[Predicate] = []
            having = select.args.get("having")
            if having:
                having_preds = self._extract_predicates(having)

            agg_node = AggregateNode(
                group_by=group_cols,
                aggregates=agg_exprs,
                having=having_preds,
            )
            agg_node.children = [root]
            root = agg_node

        # 4. ORDER BY
        order = select.args.get("order")
        if order:
            sort_keys = self._extract_sort_keys(order)
            sort_node = SortNode(sort_keys=sort_keys)
            sort_node.children = [root]
            root = sort_node

        # 5. LIMIT / OFFSET
        limit = select.args.get("limit")
        offset = select.args.get("offset")
        if limit:
            lval = int(limit.this.name) if limit.this else 100
            oval = int(offset.this.name) if offset and offset.this else 0
            limit_node = LimitNode(limit=lval, offset=oval)
            limit_node.children = [root]
            root = limit_node

        # 6. Projection
        select_exprs = select.args.get("expressions", [])
        columns = self._extract_columns(select_exprs)
        if columns and columns != ["*"]:
            proj = ProjectNode(columns=columns)
            proj.children = [root]
            root = proj

        return root

    def _build_from(self, select: exp.Select) -> LogicalNode:
        """Build scan/join tree from FROM clause."""
        # sqlglot uses "from_" key in newer versions.
        from_clause = select.args.get("from") or select.args.get("from_")
        joins = select.args.get("joins", [])

        if from_clause is None:
            raise ValueError("SELECT has no FROM clause")

        # Primary table
        primary_table = from_clause.this
        root: LogicalNode = self._make_scan(primary_table)

        # Process each JOIN
        for join_expr in joins:
            join_type = self._get_join_type(join_expr)
            right_scan = self._make_scan(join_expr.this)
            conditions = self._extract_join_conditions(join_expr)

            join_node = JoinNode(
                join_type=join_type,
                conditions=conditions,
            )
            join_node.children = [root, right_scan]
            root = join_node

        return root

    def _make_scan(self, table_expr: exp.Expression) -> ScanNode:
        """Create a ScanNode from a table expression."""
        if isinstance(table_expr, exp.Table):
            name = table_expr.name
            schema = table_expr.db or "public"
            alias = table_expr.alias or None
            return ScanNode(table_name=name, schema_name=schema, alias=alias)
        if isinstance(table_expr, exp.Subquery):
            # Subquery as scan — build inner plan and wrap
            inner = self._build_node(table_expr.this)
            alias = table_expr.alias or "subq"
            scan = ScanNode(table_name=f"__subquery_{alias}__", alias=alias)
            scan.children = [inner]
            return scan
        # Fallback
        return ScanNode(table_name=str(table_expr))

    def _get_join_type(self, join: exp.Join) -> JoinType:
        """Map sqlglot join kind to JoinType enum."""
        kind = join.args.get("kind", "").upper()
        mapping = {
            "LEFT": JoinType.LEFT,
            "RIGHT": JoinType.RIGHT,
            "FULL": JoinType.FULL,
            "CROSS": JoinType.CROSS,
            "SEMI": JoinType.SEMI,
            "ANTI": JoinType.ANTI,
        }
        return mapping.get(kind, JoinType.INNER)

    def _extract_join_conditions(self, join: exp.Join) -> list[Predicate]:
        """Extract join ON conditions as predicates."""
        on_clause = join.args.get("on")
        if on_clause is None:
            return []
        return self._extract_predicates(on_clause)

    def _extract_predicates(self, expr: exp.Expression) -> list[Predicate]:
        """Recursively extract predicates from a WHERE or HAVING clause."""
        preds: list[Predicate] = []
        if isinstance(expr, (exp.And, exp.Or)):
            preds.extend(self._extract_predicates(expr.left))
            preds.extend(self._extract_predicates(expr.right))
        elif isinstance(expr, exp.EQ):
            preds.append(self._make_pred(expr, "="))
        elif isinstance(expr, exp.NEQ):
            preds.append(self._make_pred(expr, "!="))
        elif isinstance(expr, exp.LT):
            preds.append(self._make_pred(expr, "<"))
        elif isinstance(expr, exp.GT):
            preds.append(self._make_pred(expr, ">"))
        elif isinstance(expr, exp.LTE):
            preds.append(self._make_pred(expr, "<="))
        elif isinstance(expr, exp.GTE):
            preds.append(self._make_pred(expr, ">="))
        elif isinstance(expr, exp.In):
            col = self._expr_to_str(expr.this)
            vals = [self._expr_to_str(v) for v in expr.expressions]
            preds.append(Predicate(column=col, operator="IN", value=vals))
        elif isinstance(expr, exp.Like):
            col = self._expr_to_str(expr.this)
            preds.append(Predicate(column=col, operator="LIKE", value=str(expr.expression)))
        elif isinstance(expr, exp.Is):
            col = self._expr_to_str(expr.this)
            preds.append(Predicate(column=col, operator="IS NULL", value=None))
        return preds

    def _make_pred(self, expr: exp.Expression, op: str) -> Predicate:
        """Create a Predicate from a binary comparison expression."""
        left = self._expr_to_str(expr.left)
        right = self._expr_to_str(expr.right)
        # Determine which side is the column
        return Predicate(column=left, operator=op, value=right)

    def _expr_to_str(self, expr: exp.Expression) -> str:
        """Convert expression to string representation."""
        if isinstance(expr, exp.Column):
            if expr.table:
                return f"{expr.table}.{expr.name}"
            return expr.name
        if isinstance(expr, exp.Literal):
            return expr.this
        return str(expr)

    def _extract_aggregates(self, select: exp.Select) -> list[AggregateExpr]:
        """Extract aggregate functions from SELECT list."""
        aggs: list[AggregateExpr] = []
        for expr in select.args.get("expressions", []):
            alias = expr.alias if hasattr(expr, "alias") else ""
            inner = expr.this if hasattr(expr, "this") else expr
            agg_map = {
                exp.Count: AggFunc.COUNT, exp.Sum: AggFunc.SUM,
                exp.Avg: AggFunc.AVG, exp.Min: AggFunc.MIN, exp.Max: AggFunc.MAX,
            }
            for agg_type, agg_func in agg_map.items():
                if isinstance(inner, agg_type):
                    col = self._expr_to_str(inner.this) if inner.this else None
                    aggs.append(AggregateExpr(
                        func=agg_func, column=col,
                        alias=alias or agg_func.value,
                    ))
        return aggs

    def _extract_group_cols(self, group: Optional[exp.Group]) -> list[str]:
        """Extract GROUP BY column list."""
        if group is None:
            return []
        return [self._expr_to_str(e) for e in group.expressions]

    def _extract_sort_keys(self, order: exp.Order) -> list[SortKey]:
        """Extract ORDER BY sort keys."""
        keys = []
        for expr in order.expressions:
            col = self._expr_to_str(expr.this)
            direction = SortDirection.DESC if expr.args.get("desc") else SortDirection.ASC
            keys.append(SortKey(column=col, direction=direction))
        return keys

    def _extract_columns(self, exprs: list[exp.Expression]) -> list[str]:
        """Extract SELECT column names."""
        cols = []
        for e in exprs:
            if isinstance(e, exp.Star):
                return ["*"]
            alias = e.alias if hasattr(e, "alias") and e.alias else None
            if alias:
                cols.append(alias)
            else:
                cols.append(self._expr_to_str(e))
        return cols
