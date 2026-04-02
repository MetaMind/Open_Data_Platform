"""Materialized View Auto-Refresh Scheduler.

Monitors query frequency for tables and automatically refreshes stale
materialized views whose base tables are heavily queried.

Runs as a persistent asyncio background task started from bootstrap.py.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_DEFAULT_POLL_SECONDS = 120
_QUERY_COUNT_THRESHOLD = 10   # queries/hour to trigger refresh consideration
_FRESHNESS_WINDOW_HOURS = 1


class MVAutoRefreshScheduler:
    """Background scheduler that keeps materialized views fresh.

    Args:
        db_engine: SQLAlchemy Engine for querying metadata and executing refreshes.
        query_engine: MetaMind QueryEngine (used to execute refresh SQL if needed).
        poll_interval_seconds: How often the tick loop runs (default 120 s).
    """

    def __init__(
        self,
        db_engine: Engine,
        query_engine: Any,
        poll_interval_seconds: int = _DEFAULT_POLL_SECONDS,
    ) -> None:
        self._engine = db_engine
        self._query_engine = query_engine
        self._poll_interval = poll_interval_seconds
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background scheduler loop."""
        if self._running:
            logger.warning("MVAutoRefreshScheduler already running")
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "MVAutoRefreshScheduler started (poll_interval=%ds)",
            self._poll_interval,
        )

    async def stop(self) -> None:
        """Stop the scheduler and await task completion."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                return
        logger.info("MVAutoRefreshScheduler stopped")

    # ------------------------------------------------------------------
    # Loop & tick
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            try:
                await self._tick()
            except Exception as exc:
                logger.error("MVAutoRefreshScheduler._tick error: %s", exc, exc_info=True)
            await asyncio.sleep(self._poll_interval)

    async def _tick(self) -> None:
        """Single scheduler tick: find stale MVs for hot tables and refresh them."""
        hot_tables = self._find_hot_tables()
        if not hot_tables:
            return

        stale_mvs = self._find_stale_mvs(hot_tables)
        if not stale_mvs:
            logger.debug("MVAutoRefreshScheduler: no stale MVs to refresh")
            return

        logger.info(
            "MVAutoRefreshScheduler: refreshing %d materialized views", len(stale_mvs)
        )
        for mv in stale_mvs:
            asyncio.create_task(self._refresh(mv["mv_id"]))

    async def _refresh(self, mv_id: str) -> None:
        """Refresh a single materialized view by mv_id."""
        try:
            defn = self._load_mv_definition(mv_id)
            if defn is None:
                logger.warning("MVAutoRefreshScheduler: mv_id=%s not found", mv_id)
                return

            mv_name: str = defn.get("mv_name", "")
            definition_sql: str = defn.get("definition_sql", "")

            logger.info("MVAutoRefreshScheduler: refreshing mv=%s", mv_name)
            self._execute_refresh(mv_id, mv_name, definition_sql)

        except Exception as exc:
            logger.error(
                "MVAutoRefreshScheduler._refresh failed mv_id=%s: %s", mv_id, exc
            )
            self._set_status(mv_id, "error")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_hot_tables(self) -> list[str]:
        """Return table names with high query frequency in the last hour."""
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT DISTINCT query_features->>'table_name' AS table_name "
                        "FROM mm_query_logs "
                        "WHERE submitted_at >= NOW() - INTERVAL '1 hour' "
                        "  AND query_features ? 'table_name' "
                        "GROUP BY query_features->>'table_name' "
                        f"HAVING COUNT(*) > {_QUERY_COUNT_THRESHOLD}"
                    )
                ).fetchall()
            return [r.table_name for r in rows if r.table_name]
        except Exception as exc:
            logger.error("_find_hot_tables failed: %s", exc)
            return []

    def _find_stale_mvs(self, hot_tables: list[str]) -> list[dict]:
        """Return stale materialized views whose base tables are in hot_tables."""
        if not hot_tables:
            return []
        try:
            placeholders = ",".join(f"'{t}'" for t in hot_tables)
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT mv_id, mv_name, definition_sql "
                        "FROM mm_materialized_views "
                        f"WHERE base_table = ANY(ARRAY[{placeholders}]::TEXT[]) "
                        "  AND (status = 'stale' OR status IS NULL "
                        "       OR last_refreshed_at < NOW() - INTERVAL '1 hour')"
                    )
                ).fetchall()
            return [dict(r._mapping) for r in rows]
        except Exception as exc:
            logger.error("_find_stale_mvs failed: %s", exc)
            return []

    def _load_mv_definition(self, mv_id: str) -> Optional[dict]:
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT mv_id, mv_name, definition_sql "
                        "FROM mm_materialized_views WHERE mv_id = :id"
                    ),
                    {"id": mv_id},
                ).fetchone()
            return dict(row._mapping) if row else None
        except Exception as exc:
            logger.error("_load_mv_definition failed mv_id=%s: %s", mv_id, exc)
            return None

    def _execute_refresh(
        self, mv_id: str, mv_name: str, definition_sql: str
    ) -> None:
        """Execute the refresh SQL and update status."""
        try:
            with self._engine.begin() as conn:
                if mv_name:
                    conn.execute(
                        text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv_name}")
                    )
                elif definition_sql:
                    conn.execute(text(definition_sql))
            self._set_status(mv_id, "fresh")
            logger.info("Refreshed MV mv_id=%s mv_name=%s", mv_id, mv_name)
        except Exception as exc:
            logger.error("_execute_refresh failed mv_id=%s: %s", mv_id, exc)
            self._set_status(mv_id, "error")

    def _set_status(self, mv_id: str, status: str) -> None:
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE mm_materialized_views "
                        "SET status = :s, last_refreshed_at = NOW() "
                        "WHERE mv_id = :id"
                    ),
                    {"s": status, "id": mv_id},
                )
        except Exception as exc:
            logger.error("_set_status failed mv_id=%s: %s", mv_id, exc)
