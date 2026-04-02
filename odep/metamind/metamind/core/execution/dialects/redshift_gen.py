"""Redshift dialect SQL generator (F17)."""
from __future__ import annotations

import logging
import re

from metamind.core.logical.nodes import LogicalNode
from metamind.core.execution.dialects.base_gen import node_to_sql, _try_transpile

logger = logging.getLogger(__name__)

# Redshift-specific window function adjustments
_FILTER_IN_WINDOW_RE = re.compile(
    r"(\w+\([^)]*\))\s+FILTER\s*\(WHERE\s+([^)]+)\)\s+OVER\s*\(",
    re.IGNORECASE,
)


class RedshiftGenerator:
    """Generates Amazon Redshift SQL from a logical plan or SQL string.

    Handles Redshift quirks: distribution keys, sort keys, window function
    syntax differences, and COPY/UNLOAD operations.
    """

    def generate(self, node: LogicalNode, sql_hint: str = "") -> str:
        """Generate Redshift SQL from a logical plan node or hint string.

        Args:
            node: Root of the logical plan tree.
            sql_hint: Optional raw SQL (Postgres dialect) to transpile.

        Returns:
            Redshift-compatible SQL string.
        """
        if sql_hint:
            sql = _try_transpile(sql_hint, read="postgres", write="redshift")
        else:
            base = node_to_sql(node, dialect="redshift")
            sql = _try_transpile(base, read="postgres", write="redshift")
        return self.translate_window_functions(sql)

    def add_distkey_hint(self, sql: str, table: str) -> str:
        """Annotate the SQL with a comment indicating distribution key usage.

        Redshift uses DISTKEY at DDL time, not query time. This method
        adds an advisory comment that DBAs can use to guide CTAS or
        table redesign.

        Args:
            sql: Input SQL query.
            table: Table name whose DISTKEY should be leveraged.

        Returns:
            SQL with informational DISTKEY comment prepended.
        """
        return f"-- Advisory: {table} uses DISTKEY for co-located joins\n{sql}"

    def add_sortkey_hint(self, sql: str, table: str) -> str:
        """Annotate the SQL with a comment about sort key range scans.

        Similar to distkey hints — sort keys are configured at DDL time.
        This adds an advisory comment for operators.

        Args:
            sql: Input SQL query.
            table: Table name benefiting from sort key range scan.

        Returns:
            SQL with informational SORTKEY comment prepended.
        """
        return f"-- Advisory: leverage SORTKEY on {table} for range predicates\n{sql}"

    def translate_window_functions(self, sql: str) -> str:
        """Rewrite SQL window functions for Redshift compatibility.

        Redshift does not support the ``FILTER (WHERE ...)`` clause in
        window functions. This method transforms them to a ``CASE WHEN``
        equivalent.

        Args:
            sql: Input SQL string.

        Returns:
            SQL with window FILTER clauses removed / rewritten.
        """

        def replace_filter_window(m: re.Match) -> str:  # type: ignore[type-arg]
            agg_fn = m.group(1)
            condition = m.group(2).strip()
            # Transform agg(col) FILTER (WHERE cond) OVER (...) →
            # agg(CASE WHEN cond THEN col END) OVER (...)
            inner_re = re.compile(r"(\w+)\s*\(([^)]*)\)")
            def add_case(inner: re.Match) -> str:  # type: ignore[type-arg]
                fn = inner.group(1)
                arg = inner.group(2).strip()
                if arg and arg != "*":
                    return f"{fn}(CASE WHEN {condition} THEN {arg} END)"
                return f"{fn}({arg})"
            rewritten_agg = inner_re.sub(add_case, agg_fn, count=1)
            return rewritten_agg + " OVER ("

        sql = _FILTER_IN_WINDOW_RE.sub(replace_filter_window, sql)
        # Redshift does not support NULLS FIRST / NULLS LAST in some contexts
        sql = re.sub(
            r"\s+NULLS\s+(FIRST|LAST)",
            "",
            sql,
            flags=re.IGNORECASE,
        )
        return sql

    def add_query_group(self, sql: str, queue_name: str) -> str:
        """Assign the query to a WLM query group for workload management.

        Args:
            sql: Input SQL query.
            queue_name: Redshift WLM queue/group name.

        Returns:
            SQL preceded by SET query_group statement.
        """
        return f"SET query_group TO '{queue_name}';\n{sql}"
