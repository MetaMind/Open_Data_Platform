"""Query audit logging — extracted from server.py to keep server.py ≤ 480 lines.

File: metamind/api/query_logger.py
Role: API Engineer
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


async def log_query(
    app_context: Any,
    query_id: str,
    tenant_id: str,
    user_id: str,
    sql: str,
    decision: Optional[Any],
    execution_time_ms: int,
    row_count: int,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """Persist query execution record to mm_query_logs."""
    if app_context is None:
        return

    try:
        from sqlalchemy import text

        with app_context.sync_db_engine.connect() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO mm_query_logs
                    (query_id, tenant_id, user_id, original_sql, rewritten_sql,
                     target_source, execution_strategy, total_time_ms, row_count,
                     status, error_message, cache_hit, submitted_at)
                    VALUES (:query_id, :tenant_id, :user_id, :sql, :rewritten,
                            :target, :strategy, :time_ms, :row_count,
                            :status, :error, :cache_hit, NOW())
                    """
                ),
                {
                    "query_id": query_id,
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "sql": sql,
                    "rewritten": decision.rewritten_sql if decision else None,
                    "target": decision.target_source if decision else None,
                    "strategy": decision.execution_strategy.value if decision else None,
                    "time_ms": execution_time_ms,
                    "row_count": row_count,
                    "status": status,
                    "error": error_message,
                    "cache_hit": (
                        decision.execution_strategy.value == "cached" if decision else False
                    ),
                },
            )
            conn.commit()
    except Exception as exc:
        logger.warning("Failed to log query %s: %s", query_id, exc)
