"""
GPU Engine — cuDF-Accelerated Query Execution with PyArrow CPU Fallback

File: metamind/execution/gpu_engine.py
Role: Infrastructure Engineer
Dependencies: cudf (optional), pyarrow, metamind.config.settings

Executes aggregation-heavy queries on GPU via RAPIDS cuDF.  When cuDF is
unavailable (non-GPU nodes, development machines), every operation falls
back transparently to equivalent PyArrow / pandas CPU operations so that
the rest of the platform is unaffected.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# cuDF import guard
# ---------------------------------------------------------------------------
try:
    import cudf            # type: ignore[import]
    import cupy as cp      # type: ignore[import]
    GPU_AVAILABLE = True
    logger.info("GPUEngine: cuDF available — GPU acceleration enabled")
except ImportError:
    cudf = None            # type: ignore[assignment]
    cp = None              # type: ignore[assignment]
    GPU_AVAILABLE = False
    logger.info("GPUEngine: cuDF not installed — CPU fallback active")


# ---------------------------------------------------------------------------
# GPUEngine
# ---------------------------------------------------------------------------

class GPUEngine:
    """
    Executes in-memory data operations using cuDF (GPU) or PyArrow (CPU).

    Caller is responsible for fetching source data into a PyArrow Table
    before calling execute().  The engine performs the compute-intensive
    step (filter / aggregate / join) and returns a PyArrow Table for
    downstream stitching.
    """

    def __init__(self, settings: Any) -> None:
        """
        Parameters
        ----------
        settings : AppSettings
            Must expose ``settings.gpu.min_gpu_rows`` (default 100_000).
        """
        self._settings = settings
        self._min_rows: int = getattr(
            getattr(settings, "gpu", None), "min_gpu_rows", 100_000
        )

    # ------------------------------------------------------------------
    # Public execution entry point
    # ------------------------------------------------------------------

    async def execute(
        self,
        data: pa.Table,
        operation: str,
        op_spec: Dict[str, Any],
        tenant_id: str,
    ) -> pa.Table:
        """
        Apply *operation* to *data* and return the result as a PyArrow Table.

        Parameters
        ----------
        data       : PyArrow Table — source data
        operation  : "filter" | "aggregate" | "join"
        op_spec    : operation-specific parameters (see _execute_* methods)
        tenant_id  : logged for observability

        Falls back to CPU if GPU is unavailable or row count is below threshold.
        """
        n_rows = len(data)
        use_gpu = self.is_available and n_rows >= self._min_rows

        t0 = time.monotonic()
        try:
            if use_gpu:
                result = self._execute_on_gpu(data, operation, op_spec)
            else:
                if use_gpu is False and self.is_available:
                    logger.info(
                        "GPUEngine: skipping GPU for tenant=%s rows=%d < min=%d",
                        tenant_id,
                        n_rows,
                        self._min_rows,
                    )
                result = self._execute_on_cpu(data, operation, op_spec)
        except Exception as exc:
            logger.error(
                "GPUEngine.execute failed op=%s tenant=%s: %s — falling back to CPU",
                operation,
                tenant_id,
                exc,
            )
            result = self._execute_on_cpu(data, operation, op_spec)

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.info(
            "GPUEngine.execute op=%s tenant=%s rows_in=%d rows_out=%d ms=%d gpu=%s",
            operation,
            tenant_id,
            n_rows,
            len(result),
            elapsed_ms,
            use_gpu,
        )
        return result

    # ------------------------------------------------------------------
    # GPU execution path
    # ------------------------------------------------------------------

    def _execute_on_gpu(
        self, data: pa.Table, operation: str, op_spec: Dict[str, Any]
    ) -> pa.Table:
        df = self._to_cudf(data)
        if operation == "filter":
            df = self._execute_filter(df, op_spec.get("predicates", []), gpu=True)
        elif operation == "aggregate":
            df = self._execute_aggregation(df, op_spec, gpu=True)
        elif operation == "join":
            right_arrow = op_spec.get("right_table")
            if right_arrow is not None:
                right_df = self._to_cudf(right_arrow)
                df = self._execute_join(
                    df,
                    right_df,
                    op_spec.get("key", ""),
                    op_spec.get("join_type", "inner"),
                    gpu=True,
                )
        else:
            logger.warning("GPUEngine: unknown operation '%s', returning input", operation)
        return self._from_cudf(df)

    def _execute_on_cpu(
        self, data: pa.Table, operation: str, op_spec: Dict[str, Any]
    ) -> pa.Table:
        if operation == "filter":
            return self._execute_filter(data, op_spec.get("predicates", []), gpu=False)
        elif operation == "aggregate":
            return self._execute_aggregation(data, op_spec, gpu=False)
        elif operation == "join":
            right = op_spec.get("right_table")
            if right is not None:
                return self._execute_join(
                    data,
                    right,
                    op_spec.get("key", ""),
                    op_spec.get("join_type", "inner"),
                    gpu=False,
                )
        return data

    # ------------------------------------------------------------------
    # cuDF ↔ PyArrow conversion
    # ------------------------------------------------------------------

    def _to_cudf(self, arrow_table: pa.Table) -> Any:
        """Convert PyArrow Table → cuDF DataFrame."""
        return cudf.DataFrame.from_arrow(arrow_table)

    def _from_cudf(self, cudf_df: Any) -> pa.Table:
        """Convert cuDF DataFrame → PyArrow Table."""
        return cudf_df.to_arrow()

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def _execute_aggregation(
        self,
        df: Any,
        agg_spec: Dict[str, Any],
        gpu: bool = False,
    ) -> Any:
        """
        GROUP BY + aggregates.

        agg_spec example::
            {
                "group_by": ["region", "product"],
                "aggregates": [
                    {"column": "revenue", "func": "sum", "alias": "total_revenue"},
                    {"column": "orders",  "func": "count", "alias": "order_count"},
                ]
            }
        """
        group_cols: List[str] = agg_spec.get("group_by", [])
        aggregates: List[Dict[str, Any]] = agg_spec.get("aggregates", [])

        if not aggregates:
            return df

        _FUNC_MAP = {"sum": "sum", "count": "count", "mean": "mean",
                     "min": "min", "max": "max", "avg": "mean"}

        if gpu:
            agg_dict: Dict[str, Any] = {}
            for agg in aggregates:
                col = agg.get("column", "")
                func = _FUNC_MAP.get(agg.get("func", "sum"), "sum")
                if col and col in df.columns:
                    agg_dict[col] = func
            if group_cols:
                result = df.groupby(group_cols).agg(agg_dict).reset_index()
            else:
                result = df.agg(agg_dict)
            return result
        else:
            # PyArrow CPU path — use pandas via .to_pandas()
            pdf = df.to_pandas() if hasattr(df, "to_pandas") else df
            import pandas as pd

            agg_dict_pd: Dict[str, Any] = {}
            for agg in aggregates:
                col = agg.get("column", "")
                func = _FUNC_MAP.get(agg.get("func", "sum"), "sum")
                alias = agg.get("alias", col)
                if col in pdf.columns:
                    agg_dict_pd[alias] = pd.NamedAgg(column=col, aggfunc=func)
            if group_cols:
                result_pd = pdf.groupby(group_cols).agg(**agg_dict_pd).reset_index()
            else:
                result_pd = pdf.agg({a.get("column", ""): a.get("func", "sum")
                                     for a in aggregates}).to_frame().T
            return pa.Table.from_pandas(result_pd)

    # ------------------------------------------------------------------
    # Filter
    # ------------------------------------------------------------------

    def _execute_filter(
        self,
        df: Any,
        predicates: List[Dict[str, Any]],
        gpu: bool = False,
    ) -> Any:
        """
        Apply a list of predicate dicts.

        Each predicate: ``{"column": "age", "op": ">=", "value": 18}``
        Supported ops: ``=, !=, >, >=, <, <=, in, not_in, is_null, is_not_null``
        """
        if not predicates:
            return df

        _OP_MAP = {
            "=": "==", "eq": "==", "ne": "!=",
            "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
        }

        if gpu:
            mask = None
            for pred in predicates:
                col = pred.get("column", "")
                op_raw = pred.get("op", "=")
                op = _OP_MAP.get(op_raw, op_raw)
                val = pred.get("value")
                if col not in df.columns:
                    continue
                try:
                    if op == "==":
                        m = df[col] == val
                    elif op == "!=":
                        m = df[col] != val
                    elif op == ">":
                        m = df[col] > val
                    elif op == ">=":
                        m = df[col] >= val
                    elif op == "<":
                        m = df[col] < val
                    elif op == "<=":
                        m = df[col] <= val
                    elif op in ("in", "not_in"):
                        m = df[col].isin(val)
                        if op == "not_in":
                            m = ~m
                    elif op == "is_null":
                        m = df[col].isnull()
                    elif op == "is_not_null":
                        m = df[col].notnull()
                    else:
                        continue
                    mask = m if mask is None else (mask & m)
                except Exception as exc:
                    logger.error("GPUEngine._execute_filter predicate error col=%s: %s", col, exc)
            return df[mask] if mask is not None else df
        else:
            # PyArrow CPU path
            if isinstance(df, pa.Table):
                combined_mask = None
                for pred in predicates:
                    col = pred.get("column", "")
                    op_raw = pred.get("op", "=")
                    op = _OP_MAP.get(op_raw, op_raw)
                    val = pred.get("value")
                    if col not in df.schema.names:
                        continue
                    col_arr = df.column(col)
                    try:
                        if op == "==":
                            m = pc.equal(col_arr, val)
                        elif op == "!=":
                            m = pc.not_equal(col_arr, val)
                        elif op == ">":
                            m = pc.greater(col_arr, val)
                        elif op == ">=":
                            m = pc.greater_equal(col_arr, val)
                        elif op == "<":
                            m = pc.less(col_arr, val)
                        elif op == "<=":
                            m = pc.less_equal(col_arr, val)
                        else:
                            continue
                        combined_mask = m if combined_mask is None else pc.and_(combined_mask, m)
                    except Exception as exc:
                        logger.error("GPUEngine CPU filter error col=%s: %s", col, exc)
                if combined_mask is not None:
                    return df.filter(combined_mask)
            return df

    # ------------------------------------------------------------------
    # Join
    # ------------------------------------------------------------------

    def _execute_join(
        self,
        left: Any,
        right: Any,
        key: str,
        join_type: str = "inner",
        gpu: bool = False,
    ) -> Any:
        """Perform a keyed merge/join on *key*."""
        _JOIN_TYPES = {"inner": "inner", "left": "leftouter", "right": "rightouter",
                       "full": "fullouter", "outer": "fullouter"}
        how = _JOIN_TYPES.get(join_type, "inner")
        if gpu:
            try:
                return left.merge(right, on=key, how=how)
            except Exception as exc:
                logger.error("GPUEngine GPU join failed key=%s: %s", key, exc)
                return left
        else:
            import pandas as pd
            ldf = left.to_pandas() if isinstance(left, pa.Table) else left
            rdf = right.to_pandas() if isinstance(right, pa.Table) else right
            merged = ldf.merge(rdf, on=key, how=join_type)
            return pa.Table.from_pandas(merged)

    # ------------------------------------------------------------------
    # Health / availability
    # ------------------------------------------------------------------

    def health_check(self) -> Dict[str, Any]:
        """Return GPU device info; falls back to CPU-only info."""
        info: Dict[str, Any] = {
            "gpu_available": self.is_available,
            "min_gpu_rows": self._min_rows,
        }
        if GPU_AVAILABLE:
            try:
                props = cp.cuda.runtime.getDeviceProperties(0)
                info["device_name"] = props["name"].decode()
                mem_info = cp.cuda.runtime.memGetInfo()
                info["memory_free_mb"] = int(mem_info[0] / 1024 / 1024)
                info["memory_total_mb"] = int(mem_info[1] / 1024 / 1024)
                import cudf
                info["cudf_version"] = cudf.__version__
            except Exception as exc:
                logger.error("GPUEngine.health_check device query failed: %s", exc)
        return info

    @property
    def is_available(self) -> bool:
        """True if cuDF is importable and a CUDA device is detected."""
        if not GPU_AVAILABLE:
            return False
        try:
            return cp.cuda.runtime.getDeviceCount() > 0
        except Exception:
            return False
