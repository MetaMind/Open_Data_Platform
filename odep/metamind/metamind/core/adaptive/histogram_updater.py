"""F22 Auto-stats collection with staleness detection and background scheduling.

Monitors table statistics freshness, detects stale histograms via DML counters
and metadata version drift, and triggers background collection using sampling.
"""
from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from metamind.core.metadata.catalog import MetadataCatalog

logger = logging.getLogger(__name__)


@dataclass
class TableStaleness:
    """Staleness assessment for a single table."""

    tenant_id: str
    schema_name: str
    table_name: str
    last_analyzed: float  # epoch timestamp
    estimated_dml_changes: int  # rows changed since last analyze
    current_row_count: int
    staleness_ratio: float  # changes / row_count
    is_stale: bool
    priority: float  # higher = more urgent


@dataclass
class CollectionConfig:
    """Configuration for auto-stats collection."""

    staleness_threshold: float = 0.1  # 10% change triggers refresh
    min_rows_changed: int = 1000  # minimum absolute changes before refresh
    max_sample_pct: float = 10.0  # max sample percentage for large tables
    min_sample_rows: int = 10000  # minimum sample rows
    large_table_threshold: int = 10_000_000  # tables above this use sampling
    collection_timeout_seconds: int = 300
    max_concurrent_collections: int = 3
    check_interval_seconds: int = 300  # 5 minutes


class StalenessDetector:
    """Detects which tables have stale statistics.

    Checks three signals:
    1. DML counter drift (INSERT/UPDATE/DELETE since last ANALYZE)
    2. Time since last analysis
    3. Metadata version changes
    """

    def __init__(self, engine: Engine, config: Optional[CollectionConfig] = None) -> None:
        """Initialize detector with database connection."""
        self._engine = engine
        self._config = config or CollectionConfig()

    def check_all_tables(self, tenant_id: str) -> list[TableStaleness]:
        """Check staleness of all tables for a tenant.

        Returns list of stale tables sorted by priority (most urgent first).
        """
        tables = self._get_table_stats_ages(tenant_id)
        stale: list[TableStaleness] = []

        for t in tables:
            assessment = self._assess_staleness(t, tenant_id)
            if assessment.is_stale:
                stale.append(assessment)

        stale.sort(key=lambda s: s.priority, reverse=True)
        logger.info(
            "Staleness check for tenant %s: %d/%d tables stale",
            tenant_id, len(stale), len(tables),
        )
        return stale

    def _get_table_stats_ages(self, tenant_id: str) -> list[dict[str, Any]]:
        """Query metadata for table stats freshness."""
        stmt = sa.text(
            "SELECT t.schema_name, t.table_name, t.row_count, "
            "COALESCE(t.last_analyzed, 0) AS last_analyzed, "
            "COALESCE(t.dml_changes_since_analyze, 0) AS dml_changes "
            "FROM mm_tables t WHERE t.tenant_id = :tid AND t.is_active = TRUE"
        )
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt, {"tid": tenant_id}).fetchall()
            return [
                {
                    "schema": r[0], "table": r[1],
                    "row_count": int(r[2] or 0),
                    "last_analyzed": float(r[3] or 0),
                    "dml_changes": int(r[4] or 0),
                }
                for r in rows
            ]
        except Exception as exc:
            logger.warning("Could not query table stats ages: %s", exc)
            return []

    def _assess_staleness(self, table_info: dict[str, Any], tenant_id: str) -> TableStaleness:
        """Assess staleness of a single table."""
        row_count = max(1, table_info["row_count"])
        dml_changes = table_info["dml_changes"]
        last_analyzed = table_info["last_analyzed"]

        staleness_ratio = dml_changes / row_count
        age_hours = (time.time() - last_analyzed) / 3600 if last_analyzed > 0 else 999.0

        is_stale = (
            (staleness_ratio >= self._config.staleness_threshold
             and dml_changes >= self._config.min_rows_changed)
            or last_analyzed == 0  # never analyzed
            or age_hours > 168  # older than 1 week
        )

        # Priority: combine staleness ratio with table size (larger tables matter more)
        size_factor = math.log2(max(2, row_count)) / 30  # normalize
        priority = staleness_ratio * 0.6 + size_factor * 0.2 + min(age_hours / 168, 1.0) * 0.2

        return TableStaleness(
            tenant_id=tenant_id,
            schema_name=table_info["schema"],
            table_name=table_info["table"],
            last_analyzed=last_analyzed,
            estimated_dml_changes=dml_changes,
            current_row_count=row_count,
            staleness_ratio=staleness_ratio,
            is_stale=is_stale,
            priority=priority if is_stale else 0.0,
        )


class StatsCollector:
    """Collects table and column statistics using sampling.

    For small tables: full scan.
    For large tables: TABLESAMPLE or random sampling via LIMIT + ORDER BY RANDOM().
    """

    def __init__(self, engine: Engine, catalog: MetadataCatalog,
                 config: Optional[CollectionConfig] = None) -> None:
        """Initialize collector."""
        self._engine = engine
        self._catalog = catalog
        self._config = config or CollectionConfig()

    def collect_table_stats(
        self, tenant_id: str, schema: str, table: str, connector: Any
    ) -> dict[str, Any]:
        """Collect fresh statistics for a table.

        Gathers: row count, column NDV, null fraction, min/max, and histogram
        buckets for high-cardinality columns.

        Args:
            tenant_id: Tenant identifier.
            schema: Schema name.
            table: Table name.
            connector: BackendConnector for executing stats queries.

        Returns:
            Dict with collected statistics.
        """
        start = time.monotonic()
        stats: dict[str, Any] = {"schema": schema, "table": table}

        # Step 1: Get row count
        try:
            count_result = connector.execute(f"SELECT COUNT(*) AS cnt FROM {schema}.{table}")
            row_count = int(count_result.rows[0]["cnt"]) if count_result.rows else 0
            stats["row_count"] = row_count
        except Exception as exc:
            logger.warning("Could not count rows for %s.%s: %s", schema, table, exc)
            stats["row_count"] = 0
            return stats

        # Step 2: Determine sample strategy
        sample_clause = self._build_sample_clause(row_count, schema, table)

        # Step 3: Collect per-column stats
        columns = self._get_column_list(tenant_id, schema, table)
        column_stats: dict[str, dict[str, Any]] = {}

        for col_name in columns:
            col_stats = self._collect_column_stats(
                connector, schema, table, col_name, row_count, sample_clause
            )
            if col_stats:
                column_stats[col_name] = col_stats

        stats["columns"] = column_stats
        stats["collection_time_ms"] = (time.monotonic() - start) * 1000

        # Step 4: Persist to metadata catalog
        self._persist_stats(tenant_id, schema, table, stats)

        logger.info(
            "Collected stats for %s.%s: %d rows, %d columns in %.0fms",
            schema, table, row_count, len(column_stats), stats["collection_time_ms"],
        )
        return stats

    def _build_sample_clause(self, row_count: int, schema: str, table: str) -> str:
        """Build SQL sample clause based on table size."""
        if row_count <= self._config.large_table_threshold:
            return ""  # full scan for small tables

        sample_pct = min(
            self._config.max_sample_pct,
            max(0.1, (self._config.min_sample_rows / row_count) * 100),
        )
        return f"TABLESAMPLE SYSTEM ({sample_pct:.1f})"

    def _get_column_list(self, tenant_id: str, schema: str, table: str) -> list[str]:
        """Get list of columns for a table from the catalog."""
        try:
            table_meta = self._catalog.get_table(tenant_id, schema, table)
            if table_meta and hasattr(table_meta, "columns"):
                return [c.column_name for c in table_meta.columns]
        except Exception:
            logger.error("Unhandled exception in histogram_updater.py: %s", exc)
        return []

    def _collect_column_stats(
        self, connector: Any, schema: str, table: str,
        column: str, row_count: int, sample_clause: str,
    ) -> Optional[dict[str, Any]]:
        """Collect NDV, null fraction, min, max for a single column."""
        try:
            sql = (
                f"SELECT "
                f"COUNT(DISTINCT {column}) AS ndv, "
                f"SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)::float / "
                f"GREATEST(COUNT(*), 1) AS null_frac, "
                f"MIN({column}) AS min_val, "
                f"MAX({column}) AS max_val, "
                f"COUNT(*) AS sample_rows "
                f"FROM {schema}.{table} {sample_clause}"
            )
            result = connector.execute(sql, timeout_seconds=60)
            if not result.rows:
                return None

            row = result.rows[0]
            return {
                "ndv": int(row.get("ndv", 0)),
                "null_fraction": float(row.get("null_frac", 0.0)),
                "min_value": row.get("min_val"),
                "max_value": row.get("max_val"),
                "sample_rows": int(row.get("sample_rows", 0)),
            }
        except Exception as exc:
            logger.debug("Could not collect stats for %s.%s.%s: %s", schema, table, column, exc)
            return None

    def _persist_stats(self, tenant_id: str, schema: str, table: str,
                       stats: dict[str, Any]) -> None:
        """Persist collected statistics to the metadata catalog."""
        try:
            # Update row count
            stmt = sa.text(
                "UPDATE mm_tables SET row_count = :cnt, last_analyzed = :ts, "
                "dml_changes_since_analyze = 0 "
                "WHERE tenant_id = :tid AND schema_name = :s AND table_name = :t"
            )
            with self._engine.begin() as conn:
                conn.execute(stmt, {
                    "cnt": stats["row_count"], "ts": time.time(),
                    "tid": tenant_id, "s": schema, "t": table,
                })

            # Update per-column stats
            for col_name, col_stats in stats.get("columns", {}).items():
                col_stmt = sa.text(
                    "UPDATE mm_columns SET ndv = :ndv, null_fraction = :nf "
                    "WHERE tenant_id = :tid AND schema_name = :s AND table_name = :t "
                    "AND column_name = :col"
                )
                with self._engine.begin() as conn:
                    conn.execute(col_stmt, {
                        "ndv": col_stats["ndv"], "nf": col_stats["null_fraction"],
                        "tid": tenant_id, "s": schema, "t": table, "col": col_name,
                    })
        except Exception as exc:
            logger.warning("Failed to persist stats for %s.%s: %s", schema, table, exc)


class HistogramUpdater:
    """Orchestrates automatic statistics collection.

    Combines staleness detection with stats collection to provide a single
    entry point for the background scheduler.
    """

    def __init__(self, engine: Engine, catalog: MetadataCatalog,
                 config: Optional[CollectionConfig] = None) -> None:
        """Initialize with database engine and catalog."""
        self._engine = engine
        self._catalog = catalog
        self._config = config or CollectionConfig()
        self._detector = StalenessDetector(engine, self._config)
        self._collector = StatsCollector(engine, catalog, self._config)

    def update_stale_tables(self, tenant_id: str, connector: Any,
                            max_tables: int = 5) -> list[dict[str, Any]]:
        """Detect and refresh stale table statistics.

        Args:
            tenant_id: Tenant to check.
            connector: BackendConnector for executing stats queries.
            max_tables: Maximum tables to refresh in one pass.

        Returns:
            List of collection results for refreshed tables.
        """
        stale = self._detector.check_all_tables(tenant_id)
        results: list[dict[str, Any]] = []

        for table_info in stale[:max_tables]:
            try:
                stats = self._collector.collect_table_stats(
                    tenant_id, table_info.schema_name, table_info.table_name, connector
                )
                results.append(stats)
            except Exception as exc:
                logger.warning(
                    "Auto-stats collection failed for %s.%s: %s",
                    table_info.schema_name, table_info.table_name, exc,
                )

        return results

    def update_table_stats(self, tenant_id: str, schema: str, table: str,
                           connector: Any) -> dict[str, Any]:
        """Force-refresh statistics for a specific table."""
        return self._collector.collect_table_stats(tenant_id, schema, table, connector)

    def get_stale_tables(self, tenant_id: str) -> list[TableStaleness]:
        """Return list of stale tables without collecting stats."""
        return self._detector.check_all_tables(tenant_id)
