"""Vector similarity search engine with index planning and multi-backend routing.

Provides native ANN search across pgvector, Lance, and DuckDB VSS backends
with cost-based index selection and query planning.

Feature: F19_vector_search
"""
from __future__ import annotations

import json
import logging
import math
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from metamind.core.catalog.metadata import MetadataCatalog
from metamind.core.execution.backends import BackendRegistry
from metamind.core.types import Predicate

logger = logging.getLogger(__name__)

# ──────────────────────────────── Data Classes ─────────────────────────────────


@dataclass
class VectorSearchRequest:
    """Request for a vector similarity search."""

    table: str
    embedding_column: str
    query_vector: list[float]
    top_k: int
    distance_metric: str  # "cosine", "l2", "inner_product"
    filter_predicates: list[Predicate] = field(default_factory=list)
    tenant_id: str = "default"
    backend_preference: Optional[str] = None  # "pgvector", "lance", "duckdb"


@dataclass
class VectorSearchResult:
    """Result of a vector similarity search."""

    rows: list[dict[str, object]]
    distances: list[float]
    row_count: int
    duration_ms: float
    index_used: Optional[str]  # IVFFlat, HNSW, None (exact scan)
    backend_used: str


@dataclass
class VectorIndex:
    """Metadata for a vector index."""

    index_name: str
    index_type: str  # "IVFFlat", "HNSW"
    column: str
    dimensions: int
    distance_metric: str
    build_params: dict[str, object] = field(default_factory=dict)
    table: str = ""
    tenant_id: str = "default"


# ──────────────────────────────── Cost Model ───────────────────────────────────


class VectorCostModel:
    """Estimates cost of different ANN strategies based on data characteristics."""

    EXACT_CPU_FACTOR: float = 1.0
    IVF_OVERHEAD: float = 1.2
    HNSW_LOG_FACTOR: float = 2.0

    def estimate_ann_cost(
        self,
        rows: int,
        dimensions: int,
        top_k: int,
        index_type: Optional[str] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> float:
        """Estimate the cost of a vector search operation.

        Returns a unitless cost score: lower is better.
        - Exact scan: O(rows × dimensions)
        - IVFFlat: O(nlist × dims + (rows/nlist) × dims × n_probe)
        - HNSW: O(log(rows) × ef_search × dimensions)
        """
        params = params or {}

        if index_type is None or index_type == "exact":
            return rows * dimensions * self.EXACT_CPU_FACTOR

        if index_type == "IVFFlat":
            nlist = params.get("nlist", int(math.sqrt(rows)))
            n_probe = params.get("n_probe", max(1, nlist // 10))
            scan_cost = nlist * dimensions
            probe_cost = (rows / max(nlist, 1)) * dimensions * n_probe
            return (scan_cost + probe_cost) * self.IVF_OVERHEAD

        if index_type == "HNSW":
            ef_search = params.get("ef_search", max(top_k, 64))
            return math.log2(max(rows, 2)) * ef_search * dimensions * self.HNSW_LOG_FACTOR

        return rows * dimensions

    def recommend_index(
        self,
        rows: int,
        dimensions: int,
        expected_qps: float = 10.0,
        memory_budget_mb: float = 1024.0,
    ) -> dict[str, Any]:
        """Recommend an index type based on data characteristics.

        Rules:
        - rows < 100K AND dimensions < 512 → exact scan (no index needed)
        - rows < 10M → IVFFlat (lower build cost, good recall)
        - rows ≥ 10M OR high QPS → HNSW (best query latency)
        """
        estimated_data_mb = (rows * dimensions * 4) / (1024 * 1024)

        if rows < 100_000 and dimensions < 512:
            return {
                "index_type": "exact",
                "reason": "Small dataset with low dimensionality; exact scan is efficient",
                "estimated_recall": 1.0,
                "estimated_build_time_s": 0,
                "estimated_memory_mb": estimated_data_mb,
            }

        if rows < 10_000_000 and expected_qps < 100:
            nlist = min(int(math.sqrt(rows)), 4096)
            return {
                "index_type": "IVFFlat",
                "reason": "Medium dataset; IVFFlat offers good recall with moderate build cost",
                "build_params": {"nlist": nlist},
                "estimated_recall": 0.95,
                "estimated_build_time_s": rows * dimensions * 4e-9,
                "estimated_memory_mb": estimated_data_mb * 1.1,
            }

        m = 16 if dimensions <= 768 else 32
        ef_construction = 200
        return {
            "index_type": "HNSW",
            "reason": "Large dataset or high QPS; HNSW provides best query latency",
            "build_params": {"m": m, "ef_construction": ef_construction},
            "estimated_recall": 0.99,
            "estimated_build_time_s": rows * dimensions * 8e-9,
            "estimated_memory_mb": estimated_data_mb * 1.5,
        }


# ────────────────────────────── Search Planner ─────────────────────────────────


class VectorSearchPlanner:
    """Plans vector search execution across available backends."""

    PGVECTOR_OPS: dict[str, str] = {
        "cosine": "<=>",
        "l2": "<->",
        "inner_product": "<#>",
    }

    def __init__(
        self,
        catalog: MetadataCatalog,
        cost_model: VectorCostModel,
        backend_registry: BackendRegistry,
    ) -> None:
        self.catalog = catalog
        self.cost_model = cost_model
        self.backend_registry = backend_registry

    def plan(self, request: VectorSearchRequest) -> dict[str, Any]:
        """Plan a vector search execution strategy.

        Checks for existing vector index, selects backend, estimates cost.
        """
        vi = self.catalog.get_vector_index(
            request.tenant_id, request.table, request.embedding_column
        )

        index_type: Optional[str] = None
        index_params: dict[str, Any] = {}
        if vi is not None:
            index_type = vi.get("index_type", "exact")
            index_params = vi.get("build_params", {})

        table_meta = self.catalog.get_table(request.tenant_id, request.table)
        row_count = table_meta.row_count if table_meta else 100_000
        dimensions = len(request.query_vector)

        cost = self.cost_model.estimate_ann_cost(
            rows=row_count,
            dimensions=dimensions,
            top_k=request.top_k,
            index_type=index_type,
            params=index_params,
        )

        backend = self._select_backend(request)
        strategy = index_type if index_type and index_type != "exact" else "exact_scan"

        sql = self._build_sql(request, backend)

        return {
            "backend": backend,
            "strategy": strategy,
            "index_type": index_type,
            "estimated_cost": cost,
            "row_count": row_count,
            "dimensions": dimensions,
            "sql": sql,
        }

    def execute(self, request: VectorSearchRequest) -> VectorSearchResult:
        """Execute a vector search by routing to the appropriate backend."""
        start = time.monotonic()
        plan = self.plan(request)
        backend_name = plan["backend"]
        strategy = plan["strategy"]

        connector = self.backend_registry.get(backend_name)
        if connector is not None:
            try:
                rows_raw = connector.execute_sql(plan["sql"])
                rows = rows_raw if rows_raw else []
                distances = [float(r.get("__distance", 0.0)) for r in rows]
            except Exception as exc:
                logger.warning("Backend %s execution failed: %s", backend_name, exc)
                rows = []
                distances = []
        else:
            logger.info(
                "No live backend '%s'; returning empty result for plan-only mode",
                backend_name,
            )
            rows = []
            distances = []

        elapsed = (time.monotonic() - start) * 1000.0
        index_used = strategy if strategy != "exact_scan" else None

        return VectorSearchResult(
            rows=rows,
            distances=distances,
            row_count=len(rows),
            duration_ms=round(elapsed, 2),
            index_used=index_used,
            backend_used=backend_name,
        )

    def _select_backend(self, request: VectorSearchRequest) -> str:
        """Select the best backend for the request."""
        if request.backend_preference:
            if self.backend_registry.available(request.backend_preference):
                return request.backend_preference

        if self.backend_registry.has_capability("pgvector", "vector_search"):
            return "pgvector"
        if self.backend_registry.available("lance"):
            return "lance"
        if self.backend_registry.available("duckdb"):
            return "duckdb"
        return "duckdb"

    def _build_sql(self, request: VectorSearchRequest, backend: str) -> str:
        """Build the SQL/query for the selected backend."""
        if backend == "pgvector" or backend == "postgres":
            return self._build_pgvector_sql(request, request.distance_metric)
        elif backend == "lance":
            return self._build_lance_query(request)
        else:
            return self._build_duckdb_sql(request)

    def _build_pgvector_sql(self, request: VectorSearchRequest, metric: str) -> str:
        """Generate pgvector operator syntax."""
        op = self.PGVECTOR_OPS.get(metric, "<=>")
        vec_literal = f"'[{','.join(str(v) for v in request.query_vector)}]'"

        where_parts: list[str] = [f"tenant_id = '{request.tenant_id}'"]
        for pred in request.filter_predicates:
            col = f"{pred.table}.{pred.column}" if pred.table else pred.column
            if pred.operator.upper() == "IN":
                vals = ", ".join(f"'{v}'" for v in pred.value)
                where_parts.append(f"{col} IN ({vals})")
            elif pred.operator.upper() == "IS NULL":
                where_parts.append(f"{col} IS NULL")
            else:
                val = f"'{pred.value}'" if isinstance(pred.value, str) else str(pred.value)
                where_parts.append(f"{col} {pred.operator} {val}")

        where_clause = " AND ".join(where_parts)

        return (
            f"SELECT *, {request.embedding_column} {op} {vec_literal} AS __distance "
            f"FROM {request.table} "
            f"WHERE {where_clause} "
            f"ORDER BY {request.embedding_column} {op} {vec_literal} "
            f"LIMIT {request.top_k}"
        )

    def _build_lance_query(self, request: VectorSearchRequest) -> str:
        """Build Lance DSL format query."""
        vec_str = json.dumps(request.query_vector)
        filters: list[str] = [f"tenant_id = '{request.tenant_id}'"]
        for pred in request.filter_predicates:
            col = pred.column
            val = f"'{pred.value}'" if isinstance(pred.value, str) else str(pred.value)
            filters.append(f"{col} {pred.operator} {val}")

        filter_clause = " AND ".join(filters)
        return (
            f"SELECT * FROM {request.table} "
            f"WHERE {filter_clause} "
            f"SEARCH({request.embedding_column}, {vec_str}, "
            f"metric='{request.distance_metric}', k={request.top_k})"
        )

    def _build_duckdb_sql(self, request: VectorSearchRequest) -> str:
        """Build DuckDB VSS query using array_cosine_distance."""
        vec_literal = f"[{','.join(str(v) for v in request.query_vector)}]"
        metric_fn = {
            "cosine": "array_cosine_distance",
            "l2": "array_distance",
            "inner_product": "list_inner_product",
        }.get(request.distance_metric, "array_cosine_distance")

        where_parts: list[str] = [f"tenant_id = '{request.tenant_id}'"]
        for pred in request.filter_predicates:
            col = pred.column
            val = f"'{pred.value}'" if isinstance(pred.value, str) else str(pred.value)
            where_parts.append(f"{col} {pred.operator} {val}")

        where_clause = " AND ".join(where_parts)
        dist_expr = f"{metric_fn}({request.embedding_column}, {vec_literal})"
        if request.distance_metric == "inner_product":
            dist_expr = f"-({dist_expr})"

        return (
            f"SELECT *, {dist_expr} AS __distance "
            f"FROM {request.table} "
            f"WHERE {where_clause} "
            f"ORDER BY __distance ASC "
            f"LIMIT {request.top_k}"
        )


# ────────────────────────────── Index Manager ──────────────────────────────────


class VectorIndexManager:
    """Creates and manages vector indexes in MetaMind's metadata catalog."""

    def __init__(
        self,
        catalog: MetadataCatalog,
        backend_registry: BackendRegistry,
    ) -> None:
        self.catalog = catalog
        self.backend_registry = backend_registry

    def create_index(
        self,
        tenant_id: str,
        table: str,
        column: str,
        index_type: str = "HNSW",
        metric: str = "cosine",
        params: Optional[dict[str, Any]] = None,
        backend: str = "pgvector",
    ) -> VectorIndex:
        """Create a vector index on a backend and register in catalog."""
        params = params or {}
        index_name = f"vi_{table}_{column}_{index_type.lower()}_{uuid.uuid4().hex[:8]}"
        dimensions = params.pop("dimensions", 768)

        index = VectorIndex(
            index_name=index_name,
            index_type=index_type,
            column=column,
            dimensions=dimensions,
            distance_metric=metric,
            build_params=params,
            table=table,
            tenant_id=tenant_id,
        )

        connector = self.backend_registry.get(backend)
        if connector is not None:
            create_sql = self._build_create_index_sql(index, backend)
            try:
                connector.execute_sql(create_sql)
            except Exception as exc:
                logger.warning("Backend index creation skipped: %s", exc)

        key = f"{table}.{column}"
        self.catalog.register_vector_index(
            tenant_id,
            key,
            {
                "index_name": index_name,
                "index_type": index_type,
                "column": column,
                "table": table,
                "dimensions": dimensions,
                "distance_metric": metric,
                "build_params": params,
                "backend": backend,
            },
        )

        logger.info(
            "Created vector index %s on %s.%s (type=%s, metric=%s)",
            index_name, table, column, index_type, metric,
        )
        return index

    def drop_index(self, tenant_id: str, index_name: str) -> None:
        """Drop a vector index by name."""
        for vi_meta in self.catalog.list_vector_indexes(tenant_id):
            if vi_meta.get("index_name") == index_name:
                key = f"{vi_meta['table']}.{vi_meta['column']}"
                self.catalog.remove_vector_index(tenant_id, key)
                logger.info("Dropped vector index %s", index_name)
                return
        logger.warning("Vector index %s not found for tenant %s", index_name, tenant_id)

    def list_indexes(self, tenant_id: str) -> list[VectorIndex]:
        """List all vector indexes for a tenant."""
        result: list[VectorIndex] = []
        for meta in self.catalog.list_vector_indexes(tenant_id):
            result.append(
                VectorIndex(
                    index_name=meta.get("index_name", ""),
                    index_type=meta.get("index_type", "HNSW"),
                    column=meta.get("column", ""),
                    dimensions=meta.get("dimensions", 0),
                    distance_metric=meta.get("distance_metric", "cosine"),
                    build_params=meta.get("build_params", {}),
                    table=meta.get("table", ""),
                    tenant_id=tenant_id,
                )
            )
        return result

    def get_index_for_column(
        self, tenant_id: str, table: str, column: str
    ) -> Optional[VectorIndex]:
        """Get the vector index for a specific column, if one exists."""
        meta = self.catalog.get_vector_index(tenant_id, table, column)
        if meta is None:
            return None
        return VectorIndex(
            index_name=meta.get("index_name", ""),
            index_type=meta.get("index_type", "HNSW"),
            column=meta.get("column", column),
            dimensions=meta.get("dimensions", 0),
            distance_metric=meta.get("distance_metric", "cosine"),
            build_params=meta.get("build_params", {}),
            table=table,
            tenant_id=tenant_id,
        )

    def _build_create_index_sql(self, index: VectorIndex, backend: str) -> str:
        """Build the DDL for creating a vector index."""
        if backend in ("pgvector", "postgres"):
            ops_class = {
                "cosine": "vector_cosine_ops",
                "l2": "vector_l2_ops",
                "inner_product": "vector_ip_ops",
            }.get(index.distance_metric, "vector_cosine_ops")

            if index.index_type == "HNSW":
                m = index.build_params.get("m", 16)
                ef = index.build_params.get("ef_construction", 64)
                return (
                    f"CREATE INDEX {index.index_name} ON {index.table} "
                    f"USING hnsw ({index.column} {ops_class}) "
                    f"WITH (m = {m}, ef_construction = {ef})"
                )
            else:
                nlist = index.build_params.get("nlist", 100)
                return (
                    f"CREATE INDEX {index.index_name} ON {index.table} "
                    f"USING ivfflat ({index.column} {ops_class}) "
                    f"WITH (lists = {nlist})"
                )
        return f"-- Index creation not implemented for backend: {backend}"
