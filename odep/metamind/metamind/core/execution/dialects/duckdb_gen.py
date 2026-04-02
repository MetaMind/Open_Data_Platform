"""DuckDB dialect SQL generator (F17)."""
from __future__ import annotations

import logging
import re

from metamind.core.logical.nodes import LogicalNode
from metamind.core.execution.dialects.base_gen import node_to_sql, _try_transpile

logger = logging.getLogger(__name__)


class DuckDBGenerator:
    """Generates DuckDB SQL from a logical plan or SQL string.

    Handles DuckDB-specific features: parallel execution pragmas, array
    functions, TABLESAMPLE USING SAMPLE, and ASOF/POSITIONAL joins.
    """

    def generate(self, node: LogicalNode, sql_hint: str = "") -> str:
        """Generate DuckDB SQL from a logical plan node or hint string.

        Args:
            node: Root of the logical plan tree.
            sql_hint: Optional raw SQL (Postgres dialect) to transpile.

        Returns:
            DuckDB-compatible SQL string.
        """
        if sql_hint:
            sql = _try_transpile(sql_hint, read="postgres", write="duckdb")
        else:
            base = node_to_sql(node, dialect="duckdb")
            sql = _try_transpile(base, read="postgres", write="duckdb")
        return self.translate_array_functions(sql)

    def add_parallel_hint(self, sql: str, threads: int = 4) -> str:
        """Prepend a PRAGMA to configure DuckDB parallel thread count.

        Args:
            sql: Input SQL query.
            threads: Number of threads for parallel query execution.

        Returns:
            SQL preceded by PRAGMA threads statement.
        """
        return f"PRAGMA threads={threads};\n{sql}"

    def translate_array_functions(self, sql: str) -> str:
        """Normalise array aggregate function names for DuckDB compatibility.

        DuckDB uses ``list()`` or ``array_agg()`` interchangeably, but some
        PostgreSQL variants are not recognised.

        Args:
            sql: Input SQL string.

        Returns:
            SQL with array function names normalised for DuckDB.
        """
        # DuckDB supports ARRAY_AGG natively
        # Rewrite any non-standard variants
        sql = re.sub(r"\bARRAY_COLLECT\s*\(", "ARRAY_AGG(", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bLIST_AGG\s*\(", "LIST(", sql, flags=re.IGNORECASE)
        # DuckDB uses list_sort() not array_sort()
        sql = re.sub(r"\bARRAY_SORT\s*\(", "list_sort(", sql, flags=re.IGNORECASE)
        return sql

    def add_sample_clause(self, sql: str, pct: float = 10.0) -> str:
        """Append a TABLESAMPLE USING SAMPLE clause for approximate queries.

        DuckDB's USING SAMPLE syntax is appended to the FROM clause.
        This wraps the entire query as a subquery with a SAMPLE clause.

        Args:
            sql: Input SQL query.
            pct: Sample percentage (0-100).

        Returns:
            SQL wrapped to use DuckDB USING SAMPLE N PERCENT syntax.
        """
        pct = max(0.001, min(100.0, pct))
        # Wrap as a subquery and apply DuckDB SAMPLE syntax
        return (
            f"SELECT * FROM ({sql}) AS _sampled "
            f"USING SAMPLE {pct} PERCENT (bernoulli)"
        )

    def add_memory_limit(self, sql: str, memory_gb: float = 4.0) -> str:
        """Prepend a PRAGMA to cap DuckDB memory usage.

        Args:
            sql: Input SQL query.
            memory_gb: Memory limit in gigabytes.

        Returns:
            SQL preceded by memory limit PRAGMA.
        """
        gb_str = f"{memory_gb}GB"
        return f"PRAGMA memory_limit='{gb_str}';\n{sql}"

    def add_read_parquet_scan(self, sql: str, path: str) -> str:
        """Replace a virtual table reference with a read_parquet() call.

        Args:
            sql: SQL query referencing a table placeholder.
            path: Path to the Parquet file or glob pattern.

        Returns:
            SQL with the first bare table name replaced by read_parquet().
        """
        pattern = re.compile(
            r"((?:FROM|JOIN)\s+)([a-zA-Z_][a-zA-Z0-9_]*)(\b(?!\.))",
            re.IGNORECASE,
            )
        def replace_first(m: re.Match) -> str:  # type: ignore[type-arg]
            return f"{m.group(1)}read_parquet('{path}')"

        return pattern.sub(replace_first, sql, count=1)
