"""F17 Dialect-aware SQL generator - routes plans to backend-specific generators.

Wires the existing dialect generators (Postgres, Spark, Snowflake, BigQuery,
DuckDB, Redshift) into a unified interface for the query engine.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from metamind.core.logical.nodes import LogicalNode
from metamind.core.execution.dialects.base_gen import node_to_sql, _try_transpile

logger = logging.getLogger(__name__)

_DIALECT_GENERATORS: dict[str, Any] = {}


def _get_dialect_generator(dialect: str) -> Any:
    """Lazy-load and cache dialect generator instances."""
    if dialect in _DIALECT_GENERATORS:
        return _DIALECT_GENERATORS[dialect]

    generator = None
    try:
        if dialect in ("postgres", "postgresql"):
            from metamind.core.execution.dialects.postgres_gen import PostgreSQLGenerator
            generator = PostgreSQLGenerator()
        elif dialect == "spark":
            from metamind.core.execution.dialects.spark_gen import SparkSQLGenerator
            generator = SparkSQLGenerator()
        elif dialect == "snowflake":
            from metamind.core.execution.dialects.snowflake_gen import SnowflakeSQLGenerator
            generator = SnowflakeSQLGenerator()
        elif dialect == "bigquery":
            from metamind.core.execution.dialects.bigquery_gen import BigQuerySQLGenerator
            generator = BigQuerySQLGenerator()
        elif dialect == "duckdb":
            from metamind.core.execution.dialects.duckdb_gen import DuckDBSQLGenerator
            generator = DuckDBSQLGenerator()
        elif dialect == "redshift":
            from metamind.core.execution.dialects.redshift_gen import RedshiftSQLGenerator
            generator = RedshiftSQLGenerator()
    except ImportError as exc:
        logger.warning("Could not load dialect generator for %s: %s", dialect, exc)

    _DIALECT_GENERATORS[dialect] = generator
    return generator


_DIALECT_ALIASES: dict[str, str] = {
    "postgresql": "postgres", "pg": "postgres",
    "sparksql": "spark", "databricks": "spark",
    "sf": "snowflake",
    "bq": "bigquery", "google_bigquery": "bigquery",
    "duck": "duckdb",
    "rs": "redshift", "aws_redshift": "redshift",
    "mysql": "postgres", "flink": "spark",
}


def resolve_dialect(dialect: str) -> str:
    """Resolve a dialect alias to its canonical name."""
    return _DIALECT_ALIASES.get(dialect.lower().strip(), dialect.lower().strip())


class SQLGenerator:
    """Unified SQL generator that delegates to dialect-specific generators.

    Entry point for the query engine to produce SQL. Resolves dialect,
    delegates to native generator, falls back to base + sqlglot transpile.
    """

    def __init__(self, dialect: str = "postgres") -> None:
        """Initialize generator for a target dialect."""
        self._dialect = resolve_dialect(dialect)
        self._generator = _get_dialect_generator(self._dialect)
        logger.debug("SQLGenerator: dialect=%s gen=%s", self._dialect,
                     type(self._generator).__name__ if self._generator else "base")

    @property
    def dialect(self) -> str:
        """Return the resolved target dialect."""
        return self._dialect

    @property
    def has_native_generator(self) -> bool:
        """Return True if a native dialect generator is available."""
        return self._generator is not None

    def generate(self, node: LogicalNode, hints: Optional[dict[str, Any]] = None) -> str:
        """Generate SQL from a logical plan node using the target dialect.

        Uses native dialect generator if available, otherwise base + transpile.
        """
        if self._generator is not None:
            sql = self._generator.generate(node)
        else:
            base_sql = node_to_sql(node, dialect=self._dialect)
            sql = _try_transpile(base_sql, read="postgres", write=self._dialect)

        if hints:
            sql = self._apply_hints(sql, hints)
        return sql

    def generate_from_sql(self, sql: str, source_dialect: str = "postgres") -> str:
        """Transpile existing SQL from source to target dialect."""
        source = resolve_dialect(source_dialect)
        if source == self._dialect:
            return sql

        if self._generator is not None:
            try:
                result = self._generator.generate(None, sql_hint=sql)
                if result and result != sql:
                    return result
            except (TypeError, AttributeError):
                logger.error("Unhandled exception in sql_generator.py: %s", exc)

        return _try_transpile(sql, read=source, write=self._dialect)

    def _apply_hints(self, sql: str, hints: dict[str, Any]) -> str:
        """Apply backend-specific execution hints to generated SQL."""
        if self._generator is None:
            return sql

        if self._dialect == "postgres":
            if "index_hint" in hints:
                tbl, idx = hints["index_hint"]
                sql = self._generator.add_index_hint(sql, tbl, idx)
            if "parallel_workers" in hints:
                sql = self._generator.add_parallel_hint(sql, hints["parallel_workers"])
            if "work_mem_mb" in hints:
                sql = self._generator.set_work_mem(sql, hints["work_mem_mb"])
        elif self._dialect == "spark":
            if "broadcast_hint" in hints and hasattr(self._generator, "add_broadcast_hint"):
                sql = self._generator.add_broadcast_hint(sql, hints["broadcast_hint"])
        return sql

    def explain_sql(self, node: LogicalNode) -> str:
        """Generate EXPLAIN-wrapped version of the plan SQL."""
        base_sql = self.generate(node)
        prefixes = {
            "postgres": "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)",
            "spark": "EXPLAIN EXTENDED",
            "snowflake": "EXPLAIN USING JSON",
            "bigquery": "-- BigQuery: use query plan in console",
            "duckdb": "EXPLAIN ANALYZE",
            "redshift": "EXPLAIN",
        }
        return f"{prefixes.get(self._dialect, 'EXPLAIN')}\n{base_sql}"

    def supported_dialects(self) -> list[str]:
        """Return list of dialects with native generator support."""
        return ["postgres", "spark", "snowflake", "bigquery", "duckdb", "redshift"]

    def __repr__(self) -> str:
        """Repr."""
        mode = "native" if self.has_native_generator else "transpile"
        return f"SQLGenerator(dialect={self._dialect}, mode={mode})"
