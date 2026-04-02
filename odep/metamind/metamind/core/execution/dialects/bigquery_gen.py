"""BigQuery dialect SQL generator (F17)."""
from __future__ import annotations

import logging
import re
from datetime import date
from typing import Optional, Tuple

from metamind.core.logical.nodes import LogicalNode
from metamind.core.execution.dialects.base_gen import node_to_sql, _try_transpile

logger = logging.getLogger(__name__)

# DATE_TRUNC in BigQuery uses reversed argument order compared to PostgreSQL
_PG_DATE_TRUNC_RE = re.compile(
    r"DATE_TRUNC\s*\(\s*'(\w+)'\s*,\s*([^)]+)\)", re.IGNORECASE
)
# TIMESTAMP_TRUNC same shape
_PG_TIMESTAMP_TRUNC_RE = re.compile(
    r"TIMESTAMP_TRUNC\s*\(\s*'(\w+)'\s*,\s*([^)]+)\)", re.IGNORECASE
)
# Identifier quoting: backtick for BQ
_DOUBLE_QUOTE_IDENT_RE = re.compile(r'"([a-zA-Z_][a-zA-Z0-9_]*)"')


class BigQueryGenerator:
    """Generates BigQuery Standard SQL from a logical plan or SQL string.

    Handles BQ-specific quirks:
    - ``DATE_TRUNC(col, MONTH)`` argument order (BQ) vs ``DATE_TRUNC('month', col)`` (PG)
    - Three-part qualified names: ``project.dataset.table``
    - Backtick identifier quoting
    - Partition filter injection
    """

    def generate(self, node: LogicalNode, sql_hint: str = "") -> str:
        """Generate BigQuery SQL from a logical plan node.

        Args:
            node: Root of the logical plan tree.
            sql_hint: Optional raw SQL string (Postgres dialect) to transpile.

        Returns:
            BigQuery-compatible SQL string.
        """
        if sql_hint:
            sql = _try_transpile(sql_hint, read="postgres", write="bigquery")
        else:
            base = node_to_sql(node, dialect="bigquery")
            sql = _try_transpile(base, read="postgres", write="bigquery")
        return self.translate_date_functions(sql)

    def qualify_table_names(self, sql: str, project: str, dataset: str) -> str:
        """Qualify bare table names with ``project.dataset.table`` form.

        Args:
            sql: Input SQL string.
            project: GCP project ID.
            dataset: BigQuery dataset name.

        Returns:
            SQL with fully qualified three-part table identifiers.
        """
        prefix = f"`{project}.{dataset}."
        pattern = re.compile(
            r"((?:FROM|JOIN)\s+)([a-zA-Z_][a-zA-Z0-9_]*)(\b(?!\.))",
            re.IGNORECASE,
        )

        def qualify(m: re.Match) -> str:  # type: ignore[type-arg]
            keyword = m.group(1)
            table = m.group(2)
            reserved = {"select", "where", "group", "order", "having", "limit", "with"}
            if table.lower() in reserved:
                return m.group(0)
            return f"{keyword}{prefix}{table}`"

        return pattern.sub(qualify, sql)

    def translate_date_functions(self, sql: str) -> str:
        """Convert PostgreSQL-style DATE_TRUNC to BigQuery argument order.

        PostgreSQL: ``DATE_TRUNC('month', col)``
        BigQuery:   ``DATE_TRUNC(col, MONTH)``

        Args:
            sql: Input SQL string.

        Returns:
            SQL with date truncation calls in BigQuery form.
        """

        def pg_to_bq(m: re.Match) -> str:  # type: ignore[type-arg]
            unit = m.group(1).upper()
            col = m.group(2).strip()
            return f"DATE_TRUNC({col}, {unit})"

        def pg_ts_to_bq(m: re.Match) -> str:  # type: ignore[type-arg]
            unit = m.group(1).upper()
            col = m.group(2).strip()
            return f"TIMESTAMP_TRUNC({col}, {unit})"

        sql = _PG_DATE_TRUNC_RE.sub(pg_to_bq, sql)
        sql = _PG_TIMESTAMP_TRUNC_RE.sub(pg_ts_to_bq, sql)
        # NOW() → CURRENT_TIMESTAMP() in BigQuery
        sql = re.sub(r"\bNOW\(\)", "CURRENT_TIMESTAMP()", sql, flags=re.IGNORECASE)
        # Convert double-quote identifiers to backtick
        sql = _DOUBLE_QUOTE_IDENT_RE.sub(r"`\1`", sql)
        return sql

    def add_partition_filter(
        self,
        sql: str,
        partition_col: str,
        date_range: Tuple[str, str],
    ) -> str:
        """Inject a partition column filter to avoid full-table scans.

        Prepends a ``WHERE`` (or ``AND``) clause constraining the partition
        column to a date range, which allows BigQuery to prune partitions.

        Args:
            sql: Input SQL query.
            partition_col: Column used for partitioning (e.g. ``"event_date"``).
            date_range: Tuple of (start_date, end_date) as ISO-8601 strings.

        Returns:
            SQL with partition filter injected.
        """
        start, end = date_range
        filter_clause = (
            f"{partition_col} >= DATE '{start}' AND {partition_col} <= DATE '{end}'"
        )
        # Try to append to an existing WHERE clause
        where_re = re.compile(r"\bWHERE\b", re.IGNORECASE)
        if where_re.search(sql):
            sql = where_re.sub(f"WHERE {filter_clause} AND (", sql, count=1)
            sql = sql.rstrip("; \n") + ")"
        else:
            # No WHERE yet — wrap as subquery with filter
            sql = (
                f"SELECT * FROM ({sql}) AS _partitioned "
                f"WHERE {filter_clause}"
            )
        return sql

    def use_legacy_sql(self, sql: str) -> str:
        """Wrap SQL in a BigQuery legacy SQL job config comment marker.

        Note: MetaMind defaults to Standard SQL; this is for migration only.

        Args:
            sql: SQL string.

        Returns:
            SQL prefixed with legacy SQL marker comment.
        """
        return f"-- @bigquery_legacy_sql=false\n{sql}"
