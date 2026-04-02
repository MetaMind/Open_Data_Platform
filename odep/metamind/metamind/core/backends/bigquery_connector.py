"""Google BigQuery backend connector — production implementation (F13)."""
from __future__ import annotations

import logging
import time
import uuid
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
    supports_merge_join=False,
    supports_json_ops=True,
    supports_materialized_views=True,
    is_distributed=True,
    is_serverless=True,
    dialect="bigquery",
    max_concurrent_queries=100,
    cost_per_gb_scan=5.0,
)

_TB_BYTES = 1_099_511_627_776  # bytes per TiB


class BigQueryConnector(BackendConnector):
    """BigQuery connector using google-cloud-bigquery.

    Extra params in ConnectionConfig.extra_params:
        ``project``      — GCP project ID (required)
        ``dataset``      — default dataset
        ``credentials``  — path to service account JSON, or a
                           google.oauth2.credentials.Credentials object
        ``location``     — BigQuery job location (e.g. "US")
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """Initialise with connection config."""
        super().__init__(config)
        self._client: Optional[Any] = None
        self._project: str = ""
        self._dataset: str = ""
        self._location: str = "US"

    @property
    def capabilities(self) -> ConnectorCapabilities:
        """Return BigQuery capabilities."""
        return _CAPABILITIES

    def connect(self) -> None:
        """Create a BigQuery client."""
        try:
            from google.cloud import bigquery  # type: ignore[import]
            from google.oauth2 import service_account  # type: ignore[import]

            extra = self._config.extra_params
            self._project = extra.get("project") or self._config.database or ""
            self._dataset = extra.get("dataset") or self._config.schema or ""
            self._location = extra.get("location", "US")

            creds_src = extra.get("credentials")
            if isinstance(creds_src, str):
                creds = service_account.Credentials.from_service_account_file(creds_src)
                self._client = bigquery.Client(project=self._project, credentials=creds)
            elif creds_src is not None:
                self._client = bigquery.Client(project=self._project, credentials=creds_src)
            else:
                # Use application default credentials
                self._client = bigquery.Client(project=self._project)

            self._connected = True
            logger.info("BigQuery client created: project=%s", self._project)
        except ImportError as exc:
            raise ConnectorConnectionError(
                "google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery",
                backend_id=self._config.backend_id,
            ) from exc
        except Exception as exc:
            raise ConnectorConnectionError(
                f"BigQuery connect failed: {exc}",
                backend_id=self._config.backend_id,
            ) from exc

    def disconnect(self) -> None:
        """Close the BigQuery client."""
        if self._client is not None:
            try:
                self._client.close()
            except Exception as exc:
                logger.warning("Error closing BigQuery client: %s", exc)
        self._connected = False
        self._client = None
        logger.info("BigQuery connector disconnected: %s", self._config.backend_id)

    def execute(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> QueryResult:
        """Execute a BigQuery SQL query and return QueryResult."""
        if self._client is None:
            raise ConnectorExecutionError("Not connected", backend_id=self._config.backend_id)

        query_id = str(uuid.uuid4())
        start = time.monotonic()
        timeout = float(timeout_seconds or self._config.query_timeout)

        try:
            from google.cloud import bigquery  # type: ignore[import]

            job_config = bigquery.QueryJobConfig()
            if params:
                job_config.query_parameters = self._build_query_params(params, bigquery)

            job = self._client.query(sql, job_config=job_config, location=self._location)
            rows_iter = job.result(timeout=timeout)

            columns: list[str] = [f.name for f in rows_iter.schema] if rows_iter.schema else []
            rows: list[dict[str, Any]] = [dict(r) for r in rows_iter]
            row_count = len(rows)
        except Exception as exc:
            err_str = str(exc).lower()
            if "timeout" in err_str or "deadline" in err_str:
                raise ConnectorTimeoutError(
                    f"BigQuery query timed out: {exc}",
                    backend_id=self._config.backend_id,
                    sql=sql,
                ) from exc
            raise ConnectorExecutionError(
                f"BigQuery execution failed: {exc}",
                backend_id=self._config.backend_id,
                sql=sql,
            ) from exc

        duration_ms = (time.monotonic() - start) * 1000
        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=row_count,
            duration_ms=duration_ms,
            backend=self._config.backend_id,
            query_id=query_id,
            bytes_scanned=getattr(job, "total_bytes_processed", 0) or 0,
        )

    def explain(self, sql: str) -> dict[str, Any]:
        """Use a dry-run job to return bytes estimate and cost info (no data returned)."""
        if self._client is None:
            return {"error": "not connected"}
        try:
            from google.cloud import bigquery  # type: ignore[import]

            config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            job = self._client.query(sql, job_config=config, location=self._location)
            bytes_processed = job.total_bytes_processed or 0
            estimated_cost_usd = (bytes_processed / _TB_BYTES) * 5.0
            return {
                "plan_type": "bigquery_dry_run",
                "bytes_processed": bytes_processed,
                "estimated_cost_usd": round(estimated_cost_usd, 6),
                "cache_hit": job.cache_hit,
            }
        except Exception as exc:
            logger.warning("BigQuery explain failed: %s", exc)
            return {"error": str(exc)}

    def get_table_stats(self, schema: str, table: str) -> dict[str, Any]:
        """Fetch table metadata via the BigQuery client API."""
        if self._client is None:
            return {}
        try:
            dataset = schema or self._dataset
            table_ref = f"{self._project}.{dataset}.{table}"
            tbl = self._client.get_table(table_ref)
            columns = [
                {
                    "column_name": f.name,
                    "data_type": f.field_type,
                    "is_nullable": f.is_nullable,
                    "description": f.description,
                }
                for f in tbl.schema
            ]
            return {
                "row_count": tbl.num_rows or 0,
                "total_bytes": tbl.num_bytes or 0,
                "partition_field": tbl.time_partitioning.field if tbl.time_partitioning else None,
                "clustering_fields": tbl.clustering_fields or [],
                "created_at": str(tbl.created) if tbl.created else None,
                "modified_at": str(tbl.modified) if tbl.modified else None,
                "columns": columns,
            }
        except Exception as exc:
            logger.warning("BigQuery get_table_stats failed: %s", exc)
            return {}

    def _build_query_params(self, params: dict[str, Any], bigquery: Any) -> list[Any]:
        """Convert dict params to BigQuery QueryParameter objects."""
        result = []
        for name, value in params.items():
            if isinstance(value, str):
                result.append(bigquery.ScalarQueryParameter(name, "STRING", value))
            elif isinstance(value, bool):
                result.append(bigquery.ScalarQueryParameter(name, "BOOL", value))
            elif isinstance(value, int):
                result.append(bigquery.ScalarQueryParameter(name, "INT64", value))
            elif isinstance(value, float):
                result.append(bigquery.ScalarQueryParameter(name, "FLOAT64", value))
            else:
                result.append(bigquery.ScalarQueryParameter(name, "STRING", str(value)))
        return result
