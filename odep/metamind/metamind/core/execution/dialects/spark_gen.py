"""Spark SQL dialect generator (F17)."""
from __future__ import annotations

import logging
import re

from metamind.core.logical.nodes import LogicalNode
from metamind.core.execution.dialects.base_gen import node_to_sql, _try_transpile

logger = logging.getLogger(__name__)

# Spark date function normalisation
_DATE_TRUNC_PG_RE = re.compile(
    r"DATE_TRUNC\s*\(\s*'(\w+)'\s*,\s*([^)]+)\)", re.IGNORECASE
)


class SparkSQLGenerator:
    """Generates Spark SQL from a logical plan or SQL string.

    Handles Spark-specific optimizer hints: BROADCAST, REPARTITION, SKEWJOIN,
    and translates date function syntax differences.
    """

    def generate(self, node: LogicalNode, sql_hint: str = "") -> str:
        """Generate Spark SQL from a logical plan node.

        Args:
            node: Root of the logical plan tree.
            sql_hint: Optional raw SQL (Postgres dialect) to transpile.

        Returns:
            Spark SQL-compatible string.
        """
        if sql_hint:
            sql = _try_transpile(sql_hint, read="postgres", write="spark")
        else:
            base = node_to_sql(node, dialect="spark")
            sql = _try_transpile(base, read="postgres", write="spark")
        return self.translate_date_functions(sql)

    def add_broadcast_hint(self, sql: str, table: str) -> str:
        """Inject a Spark BROADCAST hint to force broadcast join.

        The hint is inserted inside the first SELECT clause.

        Args:
            sql: Input SQL query.
            table: Table alias to broadcast.

        Returns:
            SQL with ``/*+ BROADCAST(table) */`` hint injected.
        """
        hint = f"/*+ BROADCAST({table}) */"
        return self._inject_hint(sql, hint)

    def add_repartition_hint(self, sql: str, col: str, n: int) -> str:
        """Inject a Spark REPARTITION hint.

        Args:
            sql: Input SQL query.
            col: Partitioning column.
            n: Target number of partitions.

        Returns:
            SQL with ``/*+ REPARTITION(n, col) */`` hint injected.
        """
        hint = f"/*+ REPARTITION({n}, {col}) */"
        return self._inject_hint(sql, hint)

    def add_skew_hint(self, sql: str, table: str, col: str) -> str:
        """Inject a Spark SKEWJOIN hint for skewed data partitions.

        Args:
            sql: Input SQL query.
            table: Table alias with skewed column.
            col: Skewed join column.

        Returns:
            SQL with ``/*+ SKEWJOIN(table) */`` hint injected.
        """
        hint = f"/*+ SKEWJOIN({table}) */"
        return self._inject_hint(sql, hint)

    def add_coalesce_hint(self, sql: str, n: int) -> str:
        """Inject a COALESCE hint to reduce partition count after a wide shuffle.

        Args:
            sql: Input SQL query.
            n: Target coalesced partition count.

        Returns:
            SQL with ``/*+ COALESCE(n) */`` hint injected.
        """
        hint = f"/*+ COALESCE({n}) */"
        return self._inject_hint(sql, hint)

    def translate_date_functions(self, sql: str) -> str:
        """Translate PostgreSQL DATE_TRUNC to Spark DATE_TRUNC format.

        Both use ``DATE_TRUNC('unit', col)`` — same signature — but Spark
        only accepts specific unit strings. This method normalises units.

        Args:
            sql: Input SQL string.

        Returns:
            SQL with date functions normalised for Spark.
        """
        # Spark DATE_TRUNC mirrors PostgreSQL; ensure unit is lowercase
        def normalise_unit(m: re.Match) -> str:  # type: ignore[type-arg]
            unit = m.group(1).lower()
            col = m.group(2).strip()
            return f"DATE_TRUNC('{unit}', {col})"

        sql = _DATE_TRUNC_PG_RE.sub(normalise_unit, sql)

        # NOW() → CURRENT_TIMESTAMP() in Spark SQL
        sql = re.sub(r"\bNOW\(\)", "CURRENT_TIMESTAMP()", sql, flags=re.IGNORECASE)

        # DATE_FORMAT(col, 'fmt') is valid in Spark; no change needed
        return sql

    def set_shuffle_partitions(self, sql: str, n: int = 200) -> str:
        """Prepend a SET to configure Spark shuffle partitions.

        Args:
            sql: Input SQL query.
            n: Number of shuffle partitions.

        Returns:
            SQL preceded by SET spark.sql.shuffle.partitions.
        """
        return f"SET spark.sql.shuffle.partitions={n};\n{sql}"

    # ── Internal helpers ──────────────────────────────────────

    @staticmethod
    def _inject_hint(sql: str, hint: str) -> str:
        """Inject a hint comment immediately after the first SELECT keyword."""
        select_re = re.compile(r"\bSELECT\b", re.IGNORECASE)
        match = select_re.search(sql)
        if match:
            pos = match.end()
            return sql[:pos] + " " + hint + sql[pos:]
        # Fallback: prepend as comment
        return f"-- Hint: {hint}\n{sql}"
