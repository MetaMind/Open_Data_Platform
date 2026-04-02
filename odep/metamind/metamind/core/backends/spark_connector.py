"""Apache Spark backend connector — uses PySpark with lazy imports (F13)."""
from __future__ import annotations

import io
import logging
import time
import uuid
from contextlib import redirect_stdout
from typing import Any, Optional

from metamind.core.backends.connector import (
    BackendConnector,
    ConnectorCapabilities,
    ConnectorConnectionError,
    ConnectorExecutionError,
    ConnectorTimeoutError,
    ConnectionConfig,
    QueryResult,
)

logger = logging.getLogger(__name__)

_CAPABILITIES = ConnectorCapabilities(
    supports_aggregation=True,
    supports_window_functions=True,
    supports_cte=True,
    supports_lateral=True,
    supports_unnest=True,
    supports_hash_join=True,
    supports_merge_join=True,
    supports_broadcast_join=True,
    supports_json_ops=True,
    is_distributed=True,
    dialect="spark",
    max_concurrent_queries=200,
    cost_per_gb_scan=0.0,
)


class SparkConnector(BackendConnector):
    """Spark SQL connector via PySpark (lazy-imported).

    Supports both local SparkSession and remote Spark Connect (Spark 3.4+).
    Configuration keys in extra_params:
        - ``spark_master``: e.g. ``"local[*]"`` or ``"spark://host:7077"``
        - ``spark_remote``: e.g. ``"sc://host:15002"`` for Spark Connect
        - ``app_name``: application name (default ``"MetaMind"``)
        - ``spark_conf``: dict of additional Spark config key-value pairs
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """Initialise with connection config."""
        super().__init__(config)
        self._spark: Optional[Any] = None

    @property
    def capabilities(self) -> ConnectorCapabilities:
        """Return Spark capabilities."""
        return _CAPABILITIES

    def connect(self) -> None:
        """Create or attach to a SparkSession."""
        try:
            params = self._config.extra_params
            remote_url: Optional[str] = params.get("spark_remote")
            master: str = params.get("spark_master", "local[*]")
            app_name: str = params.get("app_name", "MetaMind")
            spark_conf: dict[str, str] = params.get("spark_conf", {})

            if remote_url:
                # Spark Connect (3.4+)
                from pyspark.sql import SparkSession  # type: ignore[import]
                builder = SparkSession.builder.remote(remote_url)
            else:
                from pyspark.sql import SparkSession  # type: ignore[import]
                builder = SparkSession.builder.master(master).appName(app_name)

            for key, val in spark_conf.items():
                builder = builder.config(key, val)

            self._spark = builder.getOrCreate()
            self._connected = True
            logger.info("Spark session established: %s", self._config.backend_id)
        except ImportError as exc:
            raise ConnectorConnectionError(
                "pyspark is not installed. Install with: pip install pyspark",
                backend_id=self._config.backend_id,
            ) from exc
        except Exception as exc:
            raise ConnectorConnectionError(
                f"Failed to start Spark session: {exc}",
                backend_id=self._config.backend_id,
            ) from exc

    def disconnect(self) -> None:
        """Stop the SparkSession if we own it."""
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception as exc:
                logger.warning("Error stopping Spark session: %s", exc)
        self._connected = False
        self._spark = None
        logger.info("Spark connector disconnected: %s", self._config.backend_id)

    def execute(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> QueryResult:
        """Execute a Spark SQL statement and return QueryResult.

        Note: params substitution is done via Python string formatting using
        the params dict, since Spark SQL does not support JDBC-style ``?``
        placeholders in all contexts.
        """
        if self._spark is None:
            raise ConnectorExecutionError("Spark not connected", backend_id=self._config.backend_id)

        # Basic parameter substitution (named params only)
        if params:
            sql = sql % params

        query_id = str(uuid.uuid4())
        start = time.monotonic()
        try:
            df = self._spark.sql(sql)
            pdf = df.toPandas()
            columns = list(pdf.columns)
            rows = pdf.to_dict(orient="records")
            row_count = len(rows)
        except Exception as exc:
            err_str = str(exc).lower()
            if "timeout" in err_str or "timed out" in err_str:
                raise ConnectorTimeoutError(
                    f"Spark query timed out", backend_id=self._config.backend_id, sql=sql
                ) from exc
            raise ConnectorExecutionError(
                f"Spark SQL failed: {exc}", backend_id=self._config.backend_id, sql=sql
            ) from exc

        duration_ms = (time.monotonic() - start) * 1000
        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=row_count,
            duration_ms=duration_ms,
            backend=self._config.backend_id,
            query_id=query_id,
        )

    def explain(self, sql: str) -> dict[str, Any]:
        """Return Spark physical plan via df.explain(extended=True)."""
        if self._spark is None:
            return {"error": "not connected"}
        try:
            df = self._spark.sql(sql)
            buf = io.StringIO()
            with redirect_stdout(buf):
                df.explain(extended=True)
            plan_text = buf.getvalue()
            return {"plan_type": "spark_physical", "plan": plan_text}
        except Exception as exc:
            logger.warning("Spark explain failed: %s", exc)
            return {"error": str(exc)}

    def get_table_stats(self, schema: str, table: str) -> dict[str, Any]:
        """Compute table statistics using ANALYZE TABLE."""
        if self._spark is None:
            return {}
        qualified = f"{schema}.{table}" if schema else table
        try:
            self._spark.sql(
                f"ANALYZE TABLE {qualified} COMPUTE STATISTICS FOR ALL COLUMNS"
            )
            stats_df = self._spark.sql(f"DESCRIBE EXTENDED {qualified}")
            rows = stats_df.toPandas().to_dict(orient="records")
            # Extract row count from DESCRIBE EXTENDED output
            row_count = 0
            for r in rows:
                if str(r.get("col_name", "")).lower() == "statistics":
                    info = str(r.get("data_type", ""))
                    for part in info.split(","):
                        if "rows" in part.lower():
                            try:
                                row_count = int(part.split()[0].replace(",", ""))
                            except (ValueError, IndexError):
                                logger.error("Unhandled exception in spark_connector.py: %s", exc)
            return {
                "row_count": row_count,
                "qualified_name": qualified,
                "columns": rows,
            }
        except Exception as exc:
            logger.warning("Spark get_table_stats failed for %s.%s: %s", schema, table, exc)
            return {}
