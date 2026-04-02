"""
GPU Router — Dispatch Decision Logic for GPU-Accelerated Query Execution

File: metamind/core/gpu_router.py
Role: Infrastructure Engineer
Dependencies: metamind.execution.gpu_engine, pyarrow

Decides whether a query should be routed to the GPUEngine based on
row count estimates, operation type, and GPU availability.  Integrates
with QueryRouter as an optional pre-execution step.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

import pyarrow as pa

from metamind.execution.gpu_engine import GPUEngine

logger = logging.getLogger(__name__)

# Operations that benefit from GPU acceleration
_GPU_ELIGIBLE_OPS = {"aggregate", "filter", "group_by", "scan"}

# Operations that are NOT worth sending to GPU (join-heavy tends to be
# memory-bound across large tables with many columns)
_GPU_INELIGIBLE_OPS = {"join_heavy", "nested_loop_join"}


class GPURouter:
    """
    Stateless routing helper that wraps GPUEngine with a should_use_gpu
    decision function and a route_with_gpu_fallback execution wrapper.

    Integrates into QueryRouter by being passed as an optional constructor
    argument; QueryRouter calls ``route_with_gpu_fallback`` when the
    routing decision target is ``"gpu"``.
    """

    def __init__(self, gpu_engine: GPUEngine, settings: Any) -> None:
        self._gpu_engine = gpu_engine
        self._settings = settings
        self._min_rows: int = getattr(
            getattr(settings, "gpu", None), "min_gpu_rows", 100_000
        )
        self._enabled: bool = bool(
            getattr(getattr(settings, "gpu", None), "enabled", False)
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def should_use_gpu(
        self,
        features: Dict[str, Any],
        engine_health: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Return True when all of the following conditions are met:

        1. GPU processing is enabled in settings.
        2. GPUEngine reports a CUDA device is available.
        3. Estimated row count ≥ min_gpu_rows.
        4. Primary operation is aggregation or filter (not join-heavy).

        Parameters
        ----------
        features     : query feature dict (from PlanFeatureExtractor or
                       QueryCostModel feature extraction)
        engine_health: optional dict from EngineHealthRegistry
        """
        if not self._enabled:
            return False

        if not self._gpu_engine.is_available:
            logger.debug("GPURouter: GPU not available")
            return False

        row_estimate = float(features.get("estimated_output_rows", 0))
        if row_estimate < self._min_rows:
            logger.debug(
                "GPURouter: row estimate %d < min %d — skipping GPU",
                row_estimate,
                self._min_rows,
            )
            return False

        op_type = str(features.get("operation_type", "aggregate")).lower()
        if op_type in _GPU_INELIGIBLE_OPS:
            logger.debug("GPURouter: op_type '%s' not GPU-eligible", op_type)
            return False

        # Reject join-heavy queries (many joins = high memory bandwidth cost)
        num_joins = int(features.get("num_joins", 0))
        if num_joins > 3:
            logger.debug(
                "GPURouter: num_joins=%d > 3 — rejecting GPU routing", num_joins
            )
            return False

        return True

    async def route_with_gpu_fallback(
        self,
        data: pa.Table,
        operation: str,
        op_spec: Dict[str, Any],
        tenant_id: str,
    ) -> Tuple[pa.Table, str]:
        """
        Attempt GPU execution; fall back to CPU on failure.

        Returns
        -------
        (result_table, engine_used)
            engine_used is ``"gpu"`` on success or ``"cpu_fallback"`` on failure.
        """
        try:
            result = await self._gpu_engine.execute(
                data, operation, op_spec, tenant_id
            )
            return result, "gpu"
        except Exception as exc:
            logger.error(
                "GPURouter: GPU execution failed tenant=%s op=%s: %s — using CPU",
                tenant_id,
                operation,
                exc,
            )
            result = self._gpu_engine._execute_on_cpu(data, operation, op_spec)
            if not isinstance(result, pa.Table):
                result = pa.table({})
            return result, "cpu_fallback"

    def get_status(self) -> Dict[str, Any]:
        """Return GPU routing status for health endpoints."""
        return {
            "enabled": self._enabled,
            "gpu_available": self._gpu_engine.is_available,
            "min_gpu_rows": self._min_rows,
            "health": self._gpu_engine.health_check(),
        }
