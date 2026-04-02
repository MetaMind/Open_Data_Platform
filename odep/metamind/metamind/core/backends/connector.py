"""Abstract backend connector interface — pluggable engine framework (F13)."""
from __future__ import annotations

import abc
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class ConnectorCapabilities:
    """Declares what a backend engine supports."""

    # Query capabilities
    supports_aggregation: bool = True
    supports_window_functions: bool = True
    supports_cte: bool = True
    supports_lateral: bool = False
    supports_unnest: bool = False
    max_query_size_mb: int = 16

    # Join capabilities
    supports_hash_join: bool = True
    supports_merge_join: bool = True
    supports_broadcast_join: bool = False

    # Data capabilities
    supports_vector_search: bool = False
    supports_full_text_search: bool = False
    supports_json_ops: bool = False

    # Execution model
    is_distributed: bool = False
    is_serverless: bool = False
    supports_async: bool = False
    max_concurrent_queries: int = 100

    # Cost properties
    cost_per_gb_scan: float = 0.0
    cost_per_query: float = 0.0

    # Dialect
    dialect: str = "postgres"
    sql_version: str = "SQL:2011"

    # DDL support
    supports_create_table: bool = True
    supports_materialized_views: bool = False


@dataclass
class QueryResult:
    """Standardized query result across all backends."""

    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    duration_ms: float
    bytes_scanned: int = 0
    cache_hit: bool = False
    backend: str = ""
    query_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        """Check if result set is empty."""
        return self.row_count == 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize result to JSON-compatible dict."""
        return {
            "columns": self.columns,
            "rows": self.rows,
            "row_count": self.row_count,
            "duration_ms": self.duration_ms,
            "bytes_scanned": self.bytes_scanned,
            "cache_hit": self.cache_hit,
            "backend": self.backend,
            "query_id": self.query_id,
        }


@dataclass
class ConnectionConfig:
    """Connection configuration for a backend connector."""

    backend_id: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    connection_string: Optional[str] = None
    pool_size: int = 10
    connect_timeout: int = 30
    query_timeout: int = 300
    extra_params: dict[str, Any] = field(default_factory=dict)


class BackendConnector(abc.ABC):
    """Abstract base class for all execution backend connectors.

    All connectors must be thread-safe and support async execution.
    To add a new engine: subclass BackendConnector and implement all abstract methods.
    Register via BackendRegistry.
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """Initialize connector with connection config."""
        self._config = config
        self._connected = False

    @property
    def backend_id(self) -> str:
        """Return unique backend identifier."""
        return self._config.backend_id

    @property
    def is_connected(self) -> bool:
        """Return True if connector has an active connection."""
        return self._connected

    @property
    @abc.abstractmethod
    def capabilities(self) -> ConnectorCapabilities:
        """Return connector capabilities declaration."""
        ...

    @abc.abstractmethod
    def connect(self) -> None:
        """Establish connection to the backend engine."""
        ...

    @abc.abstractmethod
    def disconnect(self) -> None:
        """Close connection and release resources."""
        ...

    @abc.abstractmethod
    def execute(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> QueryResult:
        """Execute SQL query and return standardized result.

        Args:
            sql: SQL string to execute (dialect-specific)
            params: Optional bind parameters
            timeout_seconds: Query timeout override

        Returns:
            QueryResult with rows, columns, and metadata.

        Raises:
            ConnectorExecutionError: If query fails.
            ConnectorTimeoutError: If query times out.
        """
        ...

    @abc.abstractmethod
    def explain(self, sql: str) -> dict[str, Any]:
        """Execute EXPLAIN and return parsed plan as dict."""
        ...

    @abc.abstractmethod
    def get_table_stats(
        self, schema: str, table: str
    ) -> dict[str, Any]:
        """Fetch native table statistics (row count, size, column stats)."""
        ...

    def test_connection(self) -> bool:
        """Test if connection is alive. Default: execute SELECT 1."""
        try:
            result = self.execute("SELECT 1 AS alive")
            return result.row_count > 0
        except Exception as exc:
            logger.warning("Connection test failed for %s: %s", self.backend_id, exc)
            return False

    def health_check(self) -> dict[str, str]:
        """Return health status dict."""
        alive = self.test_connection()
        return {
            "backend": self.backend_id,
            "status": "healthy" if alive else "unhealthy",
            "connected": str(self._connected),
        }

    def __repr__(self) -> str:
        """Repr."""
        return f"{self.__class__.__name__}(id={self.backend_id}, connected={self._connected})"


# ── Custom Exceptions ─────────────────────────────────────────

class ConnectorError(Exception):
    """Base exception for connector errors."""

    def __init__(self, message: str, backend_id: str = "", sql: str = "") -> None:
        """Initialize with message and optional context."""
        super().__init__(message)
        self.backend_id = backend_id
        self.sql = sql


class ConnectorExecutionError(ConnectorError):
    """Raised when query execution fails."""


class ConnectorConnectionError(ConnectorError):
    """Raised when connection cannot be established."""


class ConnectorTimeoutError(ConnectorError):
    """Raised when query exceeds time limit."""
