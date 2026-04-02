"""Amazon Redshift backend connector — production implementation (F13)."""
from __future__ import annotations

import logging
import re
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
    supports_lateral=False,
    supports_unnest=False,
    supports_hash_join=True,
    supports_merge_join=True,
    supports_json_ops=False,
    supports_materialized_views=True,
    is_distributed=True,
    dialect="redshift",
    max_concurrent_queries=50,
    cost_per_gb_scan=1.0,
)


class RedshiftConnector(BackendConnector):
    """Redshift connector using the redshift_connector package.

    Falls back to psycopg2 if redshift_connector is not installed.
    Extra params:
        ``ssl``             — bool, default True
        ``sslmode``         — ``"require"`` / ``"verify-ca"`` etc.
        ``iam``             — bool, use IAM auth
        ``cluster_identifier`` — for IAM auth
        ``region``          — AWS region
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """Initialise with connection config."""
        super().__init__(config)
        self._conn: Optional[Any] = None
        self._driver: str = "unknown"

    @property
    def capabilities(self) -> ConnectorCapabilities:
        """Return Redshift capabilities."""
        return _CAPABILITIES

    def connect(self) -> None:
        """Open connection to Redshift."""
        c = self._config
        extra = c.extra_params
        try:
            import redshift_connector  # type: ignore[import]
            self._conn = redshift_connector.connect(
                host=c.host or "localhost",
                port=c.port or 5439,
                database=c.database or "dev",
                user=c.username or "",
                password=c.password or "",
                ssl=extra.get("ssl", True),
                iam=extra.get("iam", False),
                cluster_identifier=extra.get("cluster_identifier", ""),
                region=extra.get("region", ""),
                timeout=c.connect_timeout,
            )
            self._driver = "redshift_connector"
        except ImportError:
            logger.info("redshift_connector not found, trying psycopg2")
            try:
                import psycopg2  # type: ignore[import]
                import psycopg2.extras
                dsn = self._build_dsn()
                self._conn = psycopg2.connect(dsn, connect_timeout=c.connect_timeout)
                self._driver = "psycopg2"
            except ImportError as exc2:
                raise ConnectorConnectionError(
                    "Neither redshift_connector nor psycopg2 is installed.",
                    backend_id=c.backend_id,
                ) from exc2
            except Exception as exc2:
                raise ConnectorConnectionError(
                    f"Redshift psycopg2 connect failed: {exc2}", backend_id=c.backend_id
                ) from exc2
        except Exception as exc:
            raise ConnectorConnectionError(
                f"Redshift connect failed: {exc}", backend_id=c.backend_id
            ) from exc

        self._connected = True
        logger.info("Redshift connection established (%s): %s", self._driver, c.backend_id)

    def disconnect(self) -> None:
        """Close Redshift connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception as exc:
                logger.warning("Error closing Redshift connection: %s", exc)
        self._connected = False
        self._conn = None
        logger.info("Redshift connector disconnected: %s", self._config.backend_id)

    def execute(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> QueryResult:
        """Execute SQL on Redshift."""
        if self._conn is None:
            raise ConnectorExecutionError("Not connected", backend_id=self._config.backend_id)

        query_id = str(uuid.uuid4())
        start = time.monotonic()
        timeout = timeout_seconds or self._config.query_timeout

        try:
            cur = self._conn.cursor()
            if timeout:
                cur.execute(f"SET statement_timeout = {timeout * 1000}")
            cur.execute(sql, params or None)

            columns: list[str] = []
            rows: list[dict[str, Any]] = []
            row_count = 0

            if cur.description:
                columns = [d[0] for d in cur.description]
                raw = cur.fetchall()
                rows = [dict(zip(columns, r)) for r in raw]
                row_count = len(rows)
            elif cur.rowcount >= 0:
                row_count = cur.rowcount
            self._conn.commit()
            cur.close()
        except (ConnectorTimeoutError, ConnectorExecutionError):
            try:
                self._conn.rollback()
            except Exception:
                logger.error("Unhandled exception in redshift_connector.py: %s", exc)
            raise
        except Exception as exc:
            try:
                self._conn.rollback()
            except Exception:
                logger.error("Unhandled exception in redshift_connector.py: %s", exc)
            err_str = str(exc).lower()
            if "timeout" in err_str or "statement_timeout" in err_str:
                raise ConnectorTimeoutError(
                    f"Redshift query timed out: {exc}",
                    backend_id=self._config.backend_id,
                    sql=sql,
                ) from exc
            raise ConnectorExecutionError(
                f"Redshift execution failed: {exc}",
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
        )

    def explain(self, sql: str) -> dict[str, Any]:
        """Run EXPLAIN and parse the text plan into a dict."""
        explain_sql = f"EXPLAIN {sql}"
        result = self.execute(explain_sql)
        plan_lines = [list(r.values())[0] for r in result.rows if r]
        plan_text = "\n".join(str(line) for line in plan_lines)
        return {
            "plan_type": "redshift_explain",
            "plan": plan_text,
            "nodes": self._parse_plan_nodes(plan_text),
        }

    def get_table_stats(self, schema: str, table: str) -> dict[str, Any]:
        """Fetch table info from SVV_TABLE_INFO and column info from SVV_COLUMNS."""
        tbl_sql = """
            SELECT
                tbl_rows         AS row_count,
                size             AS size_mb,
                diststyle        AS dist_style,
                sortkey1         AS sort_key,
                skew_rows        AS skew_rows,
                unsorted         AS unsorted_pct
            FROM svv_table_info
            WHERE "schema" = %(schema)s AND "table" = %(table)s
        """
        col_sql = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM svv_columns
            WHERE table_schema = %(schema)s AND table_name = %(table)s
            ORDER BY ordinal_position
        """
        dist_sql = """
            SELECT
                SUM(num_values) AS total_values,
                MAX(num_values) AS max_skew
            FROM svv_diskusage
            WHERE name = %(table)s
        """
        t_res = self.execute(tbl_sql, {"schema": schema, "table": table})
        c_res = self.execute(col_sql, {"schema": schema, "table": table})
        d_res = self.execute(dist_sql, {"table": table})

        t_row = t_res.rows[0] if t_res.rows else {}
        d_row = d_res.rows[0] if d_res.rows else {}
        return {
            "row_count": int(t_row.get("row_count") or 0),
            "size_mb": int(t_row.get("size_mb") or 0),
            "dist_style": t_row.get("dist_style"),
            "sort_key": t_row.get("sort_key"),
            "skew_rows": float(t_row.get("skew_rows") or 0.0),
            "disk_values": int(d_row.get("total_values") or 0),
            "columns": c_res.rows,
        }

    def _build_dsn(self) -> str:
        """Build a Postgres-compatible DSN for Redshift via psycopg2."""
        c = self._config
        if c.connection_string:
            return c.connection_string
        return (
            f"host={c.host or 'localhost'} port={c.port or 5439} "
            f"dbname={c.database or 'dev'} user={c.username or ''} "
            f"password={c.password or ''} "
            f"connect_timeout={c.connect_timeout}"
        )

    @staticmethod
    def _parse_plan_nodes(plan_text: str) -> list[dict[str, Any]]:
        """Extract node types from a Redshift text EXPLAIN plan."""
        nodes: list[dict[str, Any]] = []
        pattern = re.compile(r"->?\s*([A-Za-z\s]+?)\s*\(cost=(\S+)\)")
        for match in pattern.finditer(plan_text):
            nodes.append({"node": match.group(1).strip(), "cost": match.group(2)})
        return nodes
