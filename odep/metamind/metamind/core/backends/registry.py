"""Backend connector registry — manages all registered execution engines."""
from __future__ import annotations

import logging
from typing import Optional, Type

from metamind.core.backends.connector import BackendConnector, ConnectionConfig

logger = logging.getLogger(__name__)


class BackendRegistry:
    """Registry for all backend connectors.

    Supports:
    - Static registration via register_class()
    - Dynamic instantiation per tenant config
    - Health monitoring across all registered backends

    To add a new engine, implement BackendConnector and call register_class().
    """

    _connector_classes: dict[str, Type[BackendConnector]] = {}

    def __init__(self) -> None:
        """Initialize with empty instance registry."""
        self._instances: dict[str, BackendConnector] = {}

    @classmethod
    def register_class(
        cls, backend_type: str, connector_class: Type[BackendConnector]
    ) -> None:
        """Register a connector class for a backend type.

        Args:
            backend_type: String identifier (e.g., 'postgres', 'spark')
            connector_class: BackendConnector subclass to use
        """
        cls._connector_classes[backend_type] = connector_class
        logger.info("Registered connector class for backend: %s", backend_type)

    def get_or_create(
        self,
        backend_id: str,
        config: ConnectionConfig,
    ) -> BackendConnector:
        """Get existing or create new connector for backend_id.

        Args:
            backend_id: Unique instance ID (e.g., 'prod-postgres-1')
            config: Connection configuration

        Returns:
            Connected BackendConnector instance.

        Raises:
            ValueError: If backend_type not registered.
        """
        if backend_id in self._instances:
            conn = self._instances[backend_id]
            if conn.is_connected:
                return conn
            conn.connect()
            return conn

        backend_type = config.extra_params.get("backend_type", backend_id.split("-")[0])
        connector_cls = self._connector_classes.get(str(backend_type))

        if connector_cls is None:
            # Try to auto-detect from known backends
            connector_cls = self._auto_detect_class(str(backend_type))

        if connector_cls is None:
            raise ValueError(
                f"No connector registered for backend type '{backend_type}'. "
                f"Registered: {list(self._connector_classes.keys())}"
            )

        instance = connector_cls(config)
        instance.connect()
        self._instances[backend_id] = instance
        logger.info("Created connector: %s (%s)", backend_id, backend_type)
        return instance

    def get(self, backend_id: str) -> Optional[BackendConnector]:
        """Get an existing connector by ID without creating a new one."""
        return self._instances.get(backend_id)

    def list_backends(self) -> list[str]:
        """Return list of registered backend instance IDs."""
        return list(self._instances.keys())

    def health_check_all(self) -> dict[str, dict[str, str]]:
        """Run health checks on all registered connectors."""
        return {bid: conn.health_check() for bid, conn in self._instances.items()}

    def disconnect_all(self) -> None:
        """Disconnect all connectors and clear registry."""
        for conn in self._instances.values():
            try:
                conn.disconnect()
            except Exception as exc:
                logger.warning("Error disconnecting %s: %s", conn.backend_id, exc)
        self._instances.clear()
        logger.info("All backend connectors disconnected")

    def _auto_detect_class(
        self, backend_type: str
    ) -> Optional[Type[BackendConnector]]:
        """Auto-load connector class by convention-based import."""
        type_map = {
            "postgres": "metamind.core.backends.postgres_connector.PostgresConnector",
            "postgresql": "metamind.core.backends.postgres_connector.PostgresConnector",
            "duckdb": "metamind.core.backends.duckdb_connector.DuckDBConnector",
            "mysql": "metamind.core.backends.mysql_connector.MySQLConnector",
            "spark": "metamind.core.backends.spark_connector.SparkConnector",
            "snowflake": "metamind.core.backends.snowflake_connector.SnowflakeConnector",
            "bigquery": "metamind.core.backends.bigquery_connector.BigQueryConnector",
            "redshift": "metamind.core.backends.redshift_connector.RedshiftConnector",
            "flink": "metamind.core.backends.flink_connector.FlinkConnector",
            "pgvector": "metamind.core.backends.pgvector_connector.PGVectorConnector",
            "lance": "metamind.core.backends.lance_connector.LanceConnector",
        }

        module_path = type_map.get(backend_type)
        if module_path is None:
            return None

        try:
            module_name, class_name = module_path.rsplit(".", 1)
            import importlib
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            # Cache for future lookups
            self._connector_classes[backend_type] = cls
            return cls
        except (ImportError, AttributeError) as exc:
            logger.warning("Could not auto-load connector for %s: %s", backend_type, exc)
            return None


# ── Global default registry ───────────────────────────────────
_default_registry: Optional[BackendRegistry] = None


def get_registry() -> BackendRegistry:
    """Get the global default backend registry."""
    global _default_registry
    if _default_registry is None:
        _default_registry = BackendRegistry()
        _bootstrap_defaults(_default_registry)
    return _default_registry


def _bootstrap_defaults(registry: BackendRegistry) -> None:
    """Register built-in connectors into the registry."""
    try:
        from metamind.core.backends.postgres_connector import PostgresConnector
        registry.register_class("postgres", PostgresConnector)
        registry.register_class("postgresql", PostgresConnector)
    except ImportError:
        logger.error("Unhandled exception in registry.py: %s", exc)

    try:
        from metamind.core.backends.duckdb_connector import DuckDBConnector
        registry.register_class("duckdb", DuckDBConnector)
    except ImportError:
        logger.error("Unhandled exception in registry.py: %s", exc)
