"""AdapterFactory — runtime resolver for ODEP adapter implementations."""

from __future__ import annotations

import importlib
from typing import Any

from odep.interfaces import ExecutionEngine, MetadataService, Orchestrator
from odep.exceptions import AdapterNotFoundError, ProtocolViolationError

# ---------------------------------------------------------------------------
# Registry: layer → engine_name → dotted import path
# ---------------------------------------------------------------------------

ADAPTER_REGISTRY: dict[str, dict[str, str]] = {
    "metadata": {
        "openmeta": "odep.adapters.openmeta.adapter.OpenMetaAdapter",
    },
    "orchestration": {
        "airflow": "odep.adapters.airflow.adapter.AirflowAdapter",
        "dagster": "odep.adapters.dagster.adapter.DagsterAdapter",
        "prefect": "odep.adapters.prefect.adapter.PrefectAdapter",
        "temporal": "odep.adapters.temporal.adapter.TemporalAdapter",
    },
    "execution": {
        "spark": "odep.adapters.spark.adapter.SparkAdapter",
        "flink": "odep.adapters.flink.adapter.FlinkAdapter",
        "dbt": "odep.adapters.dbt.adapter.DbtAdapter",
        "duckdb": "odep.adapters.duckdb.adapter.DuckDbAdapter",
        "trino": "odep.adapters.trino.adapter.TrinoAdapter",
        "clickhouse": "odep.adapters.clickhouse.adapter.ClickHouseAdapter",
        "metamind": "odep.adapters.metamind.adapter.MetaMindAdapter",
    },
}

# ---------------------------------------------------------------------------
# Protocol map: layer → Protocol class
# ---------------------------------------------------------------------------

PROTOCOL_MAP: dict[str, type] = {
    "metadata": MetadataService,
    "orchestration": Orchestrator,
    "execution": ExecutionEngine,
}


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _load_adapter_class(dotted_path: str) -> type:
    """Lazily import and return the class at *dotted_path*."""
    module_path, class_name = dotted_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def _resolve(layer: str, engine: str, config: Any) -> Any:
    registry = ADAPTER_REGISTRY[layer]
    if engine not in registry:
        raise AdapterNotFoundError(layer, engine, list(registry.keys()))
    adapter_class = _load_adapter_class(registry[engine])
    adapter = adapter_class(config)
    if not isinstance(adapter, PROTOCOL_MAP[layer]):
        raise ProtocolViolationError(adapter_class, PROTOCOL_MAP[layer])
    return adapter


# ---------------------------------------------------------------------------
# Public resolver functions
# ---------------------------------------------------------------------------

def get_metadata_adapter(engine: str, config: Any) -> MetadataService:
    """Return a MetadataService adapter for the given engine name."""
    return _resolve("metadata", engine, config)


def get_orchestrator_adapter(engine: str, config: Any) -> Orchestrator:
    """Return an Orchestrator adapter for the given engine name."""
    return _resolve("orchestration", engine, config)


def get_execution_adapter(engine: str, config: Any) -> ExecutionEngine:
    """Return an ExecutionEngine adapter for the given engine name."""
    return _resolve("execution", engine, config)


__all__ = [
    "ADAPTER_REGISTRY",
    "PROTOCOL_MAP",
    "get_metadata_adapter",
    "get_orchestrator_adapter",
    "get_execution_adapter",
]
