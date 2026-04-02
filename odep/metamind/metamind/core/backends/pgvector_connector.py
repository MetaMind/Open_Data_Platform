"""PGVector backend connector — extends PostgreSQL with vector search (F13, F19)."""
from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Optional

from metamind.core.backends.connector import (
    ConnectorCapabilities,
    ConnectorExecutionError,
    ConnectionConfig,
    QueryResult,
)
from metamind.core.backends.postgres_connector import PostgresConnector

logger = logging.getLogger(__name__)

_CAPABILITIES = ConnectorCapabilities(
    supports_aggregation=True,
    supports_window_functions=True,
    supports_cte=True,
    supports_lateral=True,
    supports_unnest=True,
    supports_hash_join=True,
    supports_merge_join=True,
    supports_json_ops=True,
    supports_vector_search=True,
    supports_materialized_views=True,
    is_distributed=False,
    dialect="postgres",
    max_concurrent_queries=200,
    cost_per_gb_scan=0.0,
)

# Metric → operator mapping
_METRIC_OPS: dict[str, str] = {
    "cosine": "<=>",
    "l2": "<->",
    "inner_product": "<#>",
}

# Supported index types
_INDEX_TYPES = {"ivfflat", "hnsw"}


class PGVectorConnector(PostgresConnector):
    """PostgreSQL connector extended with pgvector support.

    Provides vector similarity search via pgvector operators and helper
    methods for creating IVFFlat / HNSW indexes.

    All standard PostgreSQL operations work unchanged; vector-specific
    methods are additions on top.
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """Initialise, delegating to PostgresConnector."""
        super().__init__(config)

    @property
    def capabilities(self) -> ConnectorCapabilities:
        """Return PGVector capabilities (includes vector_search=True)."""
        return _CAPABILITIES

    def connect(self) -> None:
        """Establish pool and ensure pgvector extension is installed."""
        super().connect()
        try:
            self.execute("CREATE EXTENSION IF NOT EXISTS vector")
            logger.info("pgvector extension enabled: %s", self._config.backend_id)
        except Exception as exc:
            logger.warning("Could not create pgvector extension: %s", exc)

    # ── Vector search ────────────────────────────────────────

    def execute_vector_search(
        self,
        table: str,
        embedding_col: str,
        query_vec: list[float],
        top_k: int = 10,
        metric: str = "cosine",
        extra_cols: Optional[list[str]] = None,
        schema: str = "public",
    ) -> QueryResult:
        """Perform a vector similarity search using pgvector operators.

        Args:
            table: Target table name.
            embedding_col: Column holding the ``vector`` type.
            query_vec: Query embedding as a Python list of floats.
            top_k: Number of nearest neighbours to return.
            metric: Distance metric — ``"cosine"``, ``"l2"``, or ``"inner_product"``.
            extra_cols: Additional columns to project (default: all except embedding).
            schema: Schema name (default ``"public"``).

        Returns:
            QueryResult with rows sorted by similarity ascending.
        """
        operator = _METRIC_OPS.get(metric, "<=>")
        select_cols = "*"
        if extra_cols:
            select_cols = ", ".join(extra_cols)
        vec_literal = "[" + ",".join(str(v) for v in query_vec) + "]"
        sql = (
            f"SELECT {select_cols}, "
            f"{embedding_col} {operator} '{vec_literal}'::vector AS _distance "
            f"FROM {schema}.{table} "
            f"ORDER BY {embedding_col} {operator} '{vec_literal}'::vector "
            f"LIMIT {top_k}"
        )
        return self.execute(sql)

    def create_vector_index(
        self,
        table: str,
        column: str,
        index_type: str = "ivfflat",
        schema: str = "public",
        lists: int = 100,
        m: int = 16,
        ef_construction: int = 64,
    ) -> None:
        """Create an IVFFlat or HNSW index on a vector column.

        Args:
            table: Table name.
            column: Vector column name.
            index_type: ``"ivfflat"`` (default) or ``"hnsw"``.
            schema: Schema name.
            lists: IVFFlat probe lists (only for IVFFlat).
            m: HNSW max connections per layer (only for HNSW).
            ef_construction: HNSW ef_construction parameter (only for HNSW).
        """
        index_type = index_type.lower()
        if index_type not in _INDEX_TYPES:
            raise ValueError(f"Unsupported index type '{index_type}'. Use one of {_INDEX_TYPES}.")

        idx_name = f"idx_{table}_{column}_{index_type}"
        qualified = f"{schema}.{table}"

        if index_type == "ivfflat":
            sql = (
                f"CREATE INDEX IF NOT EXISTS {idx_name} ON {qualified} "
                f"USING ivfflat ({column} vector_cosine_ops) "
                f"WITH (lists = {lists})"
            )
        else:  # hnsw
            sql = (
                f"CREATE INDEX IF NOT EXISTS {idx_name} ON {qualified} "
                f"USING hnsw ({column} vector_cosine_ops) "
                f"WITH (m = {m}, ef_construction = {ef_construction})"
            )

        self.execute(sql)
        logger.info(
            "Created %s index '%s' on %s.%s", index_type, idx_name, qualified, column
        )

    def get_table_stats(self, schema: str, table: str) -> dict[str, Any]:
        """Extend base stats with vector index presence info."""
        base_stats = super().get_table_stats(schema, table)

        index_sql = """
            SELECT i.relname AS index_name, am.amname AS index_type
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            WHERE n.nspname = %(schema)s AND t.relname = %(table)s
            AND am.amname IN ('ivfflat', 'hnsw')
        """
        try:
            idx_res = self.execute(index_sql, {"schema": schema, "table": table})
            base_stats["has_vector_index"] = len(idx_res.rows) > 0
            base_stats["vector_indexes"] = idx_res.rows
        except Exception as exc:
            logger.warning("Could not fetch vector index info: %s", exc)
            base_stats["has_vector_index"] = False
            base_stats["vector_indexes"] = []

        return base_stats
