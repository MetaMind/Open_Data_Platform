"""Audit Log Exporter — streams mm_query_logs to Parquet on S3, GCS, or local disk.

Supports SOC2 / HIPAA audit archiving requirements.  All exports are tracked
in mm_audit_exports for auditability of the audit trail itself.
"""
from __future__ import annotations

import io
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class ExportResult:
    """Result of a completed audit export job."""

    file_path: str
    row_count: int
    duration_ms: float
    storage_backend: str


class AuditExporter:
    """Export audit logs from mm_query_logs to Parquet files on object storage.

    Args:
        db_engine: SQLAlchemy Engine connected to the MetaMind metadata DB.
        storage_backend: Destination type — 's3', 'gcs', or 'local'.
    """

    def __init__(
        self,
        db_engine: Engine,
        storage_backend: Literal["s3", "gcs", "local"] = "local",
    ) -> None:
        self._engine = db_engine
        self._backend = storage_backend

    async def export(
        self,
        tenant_id: str,
        start_date: datetime,
        end_date: datetime,
        dest_path: str,
    ) -> ExportResult:
        """Export query logs for tenant between start_date and end_date.

        Streams rows in chunks to a Parquet file and uploads to the configured
        backend.  Chunked reading prevents OOM on large tenants (fixes W-12).
        Records the export in mm_audit_exports.

        Args:
            tenant_id: Tenant to export logs for.
            start_date: Inclusive start of the time window.
            end_date: Inclusive end of the time window.
            dest_path: Destination path/key on the storage backend.

        Returns:
            ExportResult with file_path, row_count, and duration_ms.
        """
        t_start = time.monotonic()

        parquet_bytes, row_count = self._serialize_parquet_chunked(
            tenant_id, start_date, end_date
        )
        self._upload(parquet_bytes, dest_path)

        duration_ms = (time.monotonic() - t_start) * 1000
        self._record_export(tenant_id, dest_path, row_count)

        logger.info(
            "AuditExporter exported %d rows for tenant=%s to %s in %.0fms",
            row_count,
            tenant_id,
            dest_path,
            duration_ms,
        )
        return ExportResult(
            file_path=dest_path,
            row_count=row_count,
            duration_ms=duration_ms,
            storage_backend=self._backend,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    _CHUNK_SIZE = 10_000  # rows per batch — prevents OOM (fixes W-12)

    def _serialize_parquet_chunked(
        self, tenant_id: str, start_date: datetime, end_date: datetime
    ) -> tuple[bytes, int]:
        """Stream rows from mm_query_logs in chunks and write to Parquet.

        Uses SQLAlchemy server-side cursor (stream_results) to avoid loading
        the full result set into memory at once.

        Returns:
            (parquet_bytes, total_row_count)
        """
        query = text(
            "SELECT * FROM mm_query_logs "
            "WHERE tenant_id = :tid "
            "  AND submitted_at BETWEEN :s AND :e "
            "ORDER BY submitted_at"
        )
        params = {"tid": tenant_id, "s": start_date, "e": end_date}
        row_count = 0

        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            buf = io.BytesIO()
            writer: "pq.ParquetWriter | None" = None

            with self._engine.connect().execution_options(
                stream_results=True, yield_per=self._CHUNK_SIZE
            ) as conn:
                result = conn.execute(query, params)
                while True:
                    batch = result.fetchmany(self._CHUNK_SIZE)
                    if not batch:
                        break
                    rows = [dict(row._mapping) for row in batch]
                    row_count += len(rows)
                    table = pa.Table.from_pylist(rows)
                    if writer is None:
                        writer = pq.ParquetWriter(buf, table.schema)
                    writer.write_table(table)

            if writer is not None:
                writer.close()

            return buf.getvalue(), row_count

        except Exception as exc:
            logger.error("AuditExporter chunked Parquet export failed: %s", exc)
            # Fallback: attempt plain fetchall (smaller datasets)
            return self._serialize_parquet(
                self._fetch_rows(tenant_id, start_date, end_date)
            ), row_count

    def _fetch_rows(
        self, tenant_id: str, start_date: datetime, end_date: datetime
    ) -> list[dict]:
        """SELECT rows from mm_query_logs within the time window (small dataset fallback)."""
        query = text(
            "SELECT * FROM mm_query_logs "
            "WHERE tenant_id = :tid "
            "  AND submitted_at BETWEEN :s AND :e "
            "ORDER BY submitted_at"
        )
        try:
            with self._engine.connect() as conn:
                result = conn.execute(
                    query, {"tid": tenant_id, "s": start_date, "e": end_date}
                )
                return [dict(row._mapping) for row in result.fetchall()]
        except Exception as exc:
            logger.error("AuditExporter._fetch_rows failed: %s", exc)
            return []

    def _serialize_parquet(self, rows: list[dict]) -> bytes:
        """Serialize rows list to Parquet bytes via pyarrow."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            if not rows:
                schema = pa.schema(
                    [pa.field("tenant_id", pa.string()),
                     pa.field("submitted_at", pa.string())]
                )
                table = pa.table({}, schema=schema)
            else:
                table = pa.Table.from_pylist(rows)

            buf = io.BytesIO()
            pq.write_table(table, buf)
            return buf.getvalue()
        except Exception as exc:
            logger.error("AuditExporter Parquet serialization failed: %s", exc)
            # Fallback: CSV bytes
            if not rows:
                return b""
            headers = ",".join(rows[0].keys())
            lines = [headers] + [
                ",".join(str(v) for v in r.values()) for r in rows
            ]
            return "\n".join(lines).encode()

    def _upload(self, data: bytes, dest_path: str) -> None:
        """Write Parquet bytes to the configured storage backend."""
        if self._backend == "local":
            self._upload_local(data, dest_path)
        elif self._backend == "s3":
            self._upload_s3(data, dest_path)
        elif self._backend == "gcs":
            self._upload_gcs(data, dest_path)
        else:
            raise ValueError(f"Unknown storage backend: {self._backend}")

    def _upload_local(self, data: bytes, dest_path: str) -> None:
        import os
        os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
        with open(dest_path, "wb") as fh:
            fh.write(data)
        logger.debug("AuditExporter wrote %d bytes to %s", len(data), dest_path)

    def _upload_s3(self, data: bytes, dest_path: str) -> None:
        try:
            import boto3  # type: ignore[import]
            # dest_path format: s3://bucket/key
            parts = dest_path.replace("s3://", "").split("/", 1)
            bucket, key = parts[0], parts[1] if len(parts) > 1 else "export.parquet"
            s3 = boto3.client("s3")
            s3.put_object(Bucket=bucket, Key=key, Body=data)
            logger.debug("AuditExporter uploaded to S3 s3://%s/%s", bucket, key)
        except Exception as exc:
            logger.error("AuditExporter S3 upload failed: %s", exc)
            raise

    def _upload_gcs(self, data: bytes, dest_path: str) -> None:
        try:
            from google.cloud import storage as gcs  # type: ignore[import]
            parts = dest_path.replace("gs://", "").split("/", 1)
            bucket_name, blob_name = parts[0], parts[1] if len(parts) > 1 else "export.parquet"
            client = gcs.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.upload_from_string(data, content_type="application/octet-stream")
            logger.debug("AuditExporter uploaded to GCS gs://%s/%s", bucket_name, blob_name)
        except Exception as exc:
            logger.error("AuditExporter GCS upload failed: %s", exc)
            raise

    def _record_export(
        self, tenant_id: str, file_path: str, row_count: int
    ) -> None:
        """Insert a record into mm_audit_exports."""
        try:
            ins = text(
                "INSERT INTO mm_audit_exports "
                "(tenant_id, file_path, row_count, exported_at) "
                "VALUES (:tid, :fp, :rc, NOW())"
            )
            with self._engine.begin() as conn:
                conn.execute(
                    ins,
                    {"tid": tenant_id, "fp": file_path, "rc": row_count},
                )
        except Exception as exc:
            logger.error("AuditExporter._record_export failed: %s", exc)
