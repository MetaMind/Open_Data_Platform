"""Snowflake dialect SQL generator (F17)."""
from __future__ import annotations

import logging
import re

from metamind.core.logical.nodes import LogicalNode
from metamind.core.execution.dialects.base_gen import node_to_sql, _try_transpile

logger = logging.getLogger(__name__)

# Patterns for Snowflake-specific transformations
_DATE_TRUNC_PG_RE = re.compile(
    r"DATE_TRUNC\s*\(\s*'(\w+)'\s*,\s*([^)]+)\)", re.IGNORECASE
)
_TRUNC_SF_RE = re.compile(
    r"TRUNC\s*\(\s*([^,]+),\s*'(\w+)'\s*\)", re.IGNORECASE
)


class SnowflakeGenerator:
    """Generates Snowflake-dialect SQL from a logical plan or SQL string.

    Handles Snowflake quirks: DATE_TRUNC argument order, result cache,
    warehouse hints, and array function compatibility.
    """

    def generate(self, node: LogicalNode, sql_hint: str = "") -> str:
        """Generate Snowflake SQL from a logical plan node or hint string.

        Args:
            node: Root of the logical plan tree.
            sql_hint: Optional raw SQL to transpile to Snowflake dialect.

        Returns:
            Snowflake-compatible SQL string.
        """
        if sql_hint:
            sql = _try_transpile(sql_hint, read="postgres", write="snowflake")
        else:
            base = node_to_sql(node, dialect="snowflake")
            sql = _try_transpile(base, read="postgres", write="snowflake")
        return self.translate_date_functions(sql)

    def add_result_cache_bypass(self, sql: str) -> str:
        """Prepend an ALTER SESSION to disable Snowflake result caching.

        Useful for benchmarking to avoid cached result retrieval.

        Args:
            sql: SQL query to run without result cache.

        Returns:
            SQL with cache-disabling session preamble.
        """
        return "ALTER SESSION SET USE_CACHED_RESULT = FALSE;\n" + sql

    def add_warehouse_hint(self, sql: str, warehouse: str) -> str:
        """Prepend a USE WAREHOUSE statement to select the compute warehouse.

        Args:
            sql: SQL query.
            warehouse: Snowflake warehouse identifier.

        Returns:
            SQL preceded by USE WAREHOUSE statement.
        """
        return f"USE WAREHOUSE {warehouse};\n{sql}"

    def translate_array_functions(self, sql: str) -> str:
        """Ensure array aggregate functions are Snowflake-compatible.

        ``ARRAY_AGG`` is natively supported in Snowflake SQL, so this
        method currently validates usage and is a no-op for standard cases.

        Args:
            sql: Input SQL string.

        Returns:
            SQL with array functions verified / normalised.
        """
        # Snowflake uses ARRAY_AGG natively, same as SQL standard
        # Replace any non-standard variants if present
        sql = re.sub(r"\bARRAY_COLLECT\s*\(", "ARRAY_AGG(", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bLIST_AGG\s*\(", "LISTAGG(", sql, flags=re.IGNORECASE)
        return sql

    def translate_date_functions(self, sql: str) -> str:
        """Rewrite PostgreSQL-style DATE_TRUNC to Snowflake argument order.

        PostgreSQL: ``DATE_TRUNC('month', col)``
        Snowflake:  ``DATE_TRUNC('month', col)``  ← same! But validate.

        Also handles ``TRUNC(col, 'MM')`` → ``DATE_TRUNC('month', col)``.

        Args:
            sql: Input SQL string.

        Returns:
            SQL with date functions normalised for Snowflake.
        """
        # Snowflake DATE_TRUNC signature matches PostgreSQL, no change needed
        # Translate Oracle-style TRUNC to Snowflake DATE_TRUNC
        month_map = {
            "MM": "month", "MONTH": "month", "YY": "year", "YYYY": "year",
            "DD": "day", "DDD": "day", "HH": "hour", "HH24": "hour",
            "MI": "minute", "SS": "second", "Q": "quarter", "W": "week",
        }

        def replace_trunc(m: re.Match) -> str:  # type: ignore[type-arg]
            col = m.group(1).strip()
            fmt = m.group(2).strip().upper()
            unit = month_map.get(fmt, fmt.lower())
            return f"DATE_TRUNC('{unit}', {col})"

        sql = _TRUNC_SF_RE.sub(replace_trunc, sql)
        return sql

    def qualify_table_names(
        self, sql: str, database: str, schema: str
    ) -> str:
        """Qualify bare table names with database.schema prefix.

        Simple heuristic: find FROM/JOIN token followed by a bare identifier
        (no dots) and prepend database.schema.

        Args:
            sql: Input SQL string.
            database: Snowflake database name.
            schema: Snowflake schema name.

        Returns:
            SQL with three-part qualified table identifiers.
        """
        prefix = f"{database}.{schema}."
        pattern = re.compile(
            r"((?:FROM|JOIN)\s+)([a-zA-Z_][a-zA-Z0-9_]*)(\b(?!\.))",
            re.IGNORECASE,
        )

        def qualify(m: re.Match) -> str:  # type: ignore[type-arg]
            keyword = m.group(1)
            table = m.group(2)
            # Skip if already qualified or is a keyword
            reserved = {"select", "where", "group", "order", "having", "limit", "with"}
            if table.lower() in reserved:
                return m.group(0)
            return f"{keyword}{prefix}{table}"

        return pattern.sub(qualify, sql)
