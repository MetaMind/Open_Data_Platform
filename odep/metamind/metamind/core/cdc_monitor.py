"""
CDC Monitor - Data Freshness Tracking

File: metamind/core/cdc_monitor.py
Role: Data Engineer
Phase: 1
Dependencies: SQLAlchemy

Tracks data freshness between Oracle (source) and S3 (replica).
Used by QueryRouter to make freshness-aware routing decisions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any

from sqlalchemy import Engine, text
from metamind.cdc.outbound_webhook import CDCWebhookDispatcher, CDCEvent  # Task 08

logger = logging.getLogger(__name__)


@dataclass
class CDCStatus:
    """CDC replication status for a table."""
    table_name: str
    source_id: str
    last_cdc_timestamp: Optional[datetime]
    last_s3_timestamp: Optional[datetime]
    lag_seconds: int
    messages_behind: int
    is_healthy: bool
    health_status: str = "unknown"


class CDCMonitor:
    """
    Tracks data freshness between Oracle (source) and S3 (replica).
    Used by QueryRouter to make freshness-aware routing decisions.
    """
    
    # Health thresholds (in seconds)
    HEALTHY_LAG_SECONDS = 300    # 5 minutes
    WARNING_LAG_SECONDS = 600    # 10 minutes
    CRITICAL_LAG_SECONDS = 1800  # 30 minutes
    
    def __init__(self, engine: Engine, kafka_admin: Optional[Any] = None):
        """
        Initialize CDC monitor.
        
        Args:
            engine: SQLAlchemy engine for metadata database
            kafka_admin: Optional Kafka admin client for direct lag check
        """
        self.engine = engine
        self.kafka = kafka_admin  # Optional: direct Kafka lag check
        # Task 08: outbound webhook dispatcher (wired by bootstrap)
        self._webhook_dispatcher: CDCWebhookDispatcher | None = None
        logger.debug("CDCMonitor initialized")
    
    def get_lag(self, table_name: str, source_type: str = "s3_iceberg") -> int:
        """
        Get replication lag in seconds.
        
        Args:
            table_name: Name of the table
            source_type: Type of replica source
            
        Returns:
            Lag in seconds (large number if no status found - assume stale)
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                SELECT lag_seconds, messages_behind, last_cdc_timestamp
                FROM mm_cdc_status
                WHERE table_name = :table
                AND source_id LIKE :source_pattern
                ORDER BY last_cdc_timestamp DESC NULLS LAST
                LIMIT 1
                """),
                {
                    "table": table_name,
                    "source_pattern": f"%{source_type}%"
                }
            ).fetchone()
            
            if result:
                lag = result.lag_seconds
                if lag is None:
                    # Calculate from timestamps
                    if result.last_cdc_timestamp:
                        lag = int(
                            (datetime.now() - result.last_cdc_timestamp).total_seconds()
                        )
                    else:
                        lag = 999999
                return max(0, lag)
            
            return 999999  # Unknown = assume very stale
    
    def get_status(self, table_name: str, source_id: str) -> Optional[CDCStatus]:
        """
        Get full CDC status for a table.
        
        Args:
            table_name: Name of the table
            source_id: Source identifier
            
        Returns:
            CDCStatus object or None if not found
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                SELECT table_name, source_id, last_cdc_timestamp,
                       last_s3_timestamp, lag_seconds, messages_behind,
                       health_status, is_healthy
                FROM mm_cdc_status
                WHERE table_name = :table AND source_id = :source
                """),
                {"table": table_name, "source": source_id}
            ).fetchone()
            
            if not result:
                return None
            
            lag = result.lag_seconds or 0
            is_healthy = result.is_healthy if result.is_healthy is not None else (
                lag < self.HEALTHY_LAG_SECONDS
            )
            
            return CDCStatus(
                table_name=result.table_name,
                source_id=result.source_id,
                last_cdc_timestamp=result.last_cdc_timestamp,
                last_s3_timestamp=result.last_s3_timestamp,
                lag_seconds=lag,
                messages_behind=result.messages_behind or 0,
                is_healthy=is_healthy,
                health_status=result.health_status or "unknown"
            )
    
    def is_healthy(self, table_name: str, max_lag_seconds: int = 300) -> bool:
        """
        Check if CDC is within acceptable lag.
        
        Args:
            table_name: Name of the table
            max_lag_seconds: Maximum acceptable lag
            
        Returns:
            True if CDC is healthy
        """
        lag = self.get_lag(table_name, "s3_iceberg")
        return lag <= max_lag_seconds
    
    def get_health_summary(self, tenant_id: str) -> Dict[str, Any]:
        """
        Get CDC health summary for all tables.
        
        Args:
            tenant_id: Tenant identifier
            
        Returns:
            Health summary dictionary
        """
        with self.engine.connect() as conn:
            results = conn.execute(
                text("""
                SELECT table_name, lag_seconds, messages_behind, health_status
                FROM mm_cdc_status
                WHERE source_id LIKE '%s3%'
                AND tenant_id = :tenant_id
                ORDER BY lag_seconds DESC NULLS LAST
                """),
                {"tenant_id": tenant_id}
            ).fetchall()
            
            total_tables = len(results)
            healthy_tables = sum(
                1 for r in results
                if r.lag_seconds and r.lag_seconds < self.HEALTHY_LAG_SECONDS
            )
            warning_tables = sum(
                1 for r in results
                if r.lag_seconds and self.HEALTHY_LAG_SECONDS <= r.lag_seconds < self.WARNING_LAG_SECONDS
            )
            critical_tables = total_tables - healthy_tables - warning_tables
            
            max_lag = max(
                (r.lag_seconds for r in results if r.lag_seconds),
                default=0
            )
            
            # Tables with most lag
            lagging_tables = [
                {"table": r.table_name, "lag_seconds": r.lag_seconds}
                for r in results[:5]
                if r.lag_seconds and r.lag_seconds > self.HEALTHY_LAG_SECONDS
            ]
            
            return {
                "total_tables": total_tables,
                "healthy": healthy_tables,
                "warning": warning_tables,
                "critical": critical_tables,
                "max_lag_seconds": max_lag,
                "lagging_tables": lagging_tables,
                "overall_status": (
                    "healthy" if critical_tables == 0
                    else "degraded" if critical_tables < total_tables / 2
                    else "unhealthy"
                )
            }
    
    def update_status(
        self,
        source_id: str,
        table_name: str,
        cdc_timestamp: Optional[datetime],
        s3_timestamp: Optional[datetime] = None,
        messages_behind: int = 0,
        tenant_id: str = "default"
    ) -> None:
        """
        Update CDC status (called by Spark streaming job).
        
        Args:
            source_id: Source identifier
            table_name: Table name
            cdc_timestamp: Last CDC timestamp
            s3_timestamp: Last S3 write timestamp
            messages_behind: Number of messages behind
            tenant_id: Tenant identifier
        """
        s3_ts = s3_timestamp or datetime.now()
        
        # Calculate lag
        lag = int((s3_ts - cdc_timestamp).total_seconds()) if cdc_timestamp else None
        
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                INSERT INTO mm_cdc_status
                (tenant_id, source_id, table_name, last_cdc_timestamp, last_s3_timestamp,
                 lag_seconds, messages_behind, updated_at)
                VALUES (:tenant_id, :source, :table, :cdc, :s3, :lag, :behind, NOW())
                ON CONFLICT (tenant_id, source_id, table_name)
                DO UPDATE SET
                    last_cdc_timestamp = EXCLUDED.last_cdc_timestamp,
                    last_s3_timestamp = EXCLUDED.last_s3_timestamp,
                    lag_seconds = EXCLUDED.lag_seconds,
                    messages_behind = EXCLUDED.messages_behind,
                    updated_at = NOW()
                """),
                {
                    "tenant_id": tenant_id,
                    "source": source_id,
                    "table": table_name,
                    "cdc": cdc_timestamp,
                    "s3": s3_ts,
                    "lag": lag,
                    "behind": messages_behind
                }
            )
            conn.commit()
            
            logger.debug(
                f"CDC status updated: {table_name} lag={lag}s, "
                f"behind={messages_behind} messages"
            )
    
    def get_tables_behind(
        self,
        tenant_id: str,
        threshold_seconds: int = 300
    ) -> List[Dict[str, Any]]:
        """
        Get list of tables exceeding lag threshold.
        
        Args:
            tenant_id: Tenant identifier
            threshold_seconds: Lag threshold
            
        Returns:
            List of tables with their lag
        """
        with self.engine.connect() as conn:
            results = conn.execute(
                text("""
                SELECT table_name, lag_seconds, messages_behind, health_status
                FROM mm_cdc_status
                WHERE source_id LIKE '%s3%'
                AND tenant_id = :tenant_id
                AND (lag_seconds > :threshold OR lag_seconds IS NULL)
                ORDER BY lag_seconds DESC NULLS LAST
                """),
                {
                    "tenant_id": tenant_id,
                    "threshold": threshold_seconds
                }
            ).fetchall()
            
            return [
                {
                    "table": r.table_name,
                    "lag_seconds": r.lag_seconds,
                    "messages_behind": r.messages_behind,
                    "health_status": r.health_status
                }
                for r in results
            ]
    
    def get_freshness_for_tables(
        self,
        tables: List[str],
        tenant_id: str = "default"
    ) -> Dict[str, int]:
        """
        Get freshness (lag) for multiple tables.
        
        Args:
            tables: List of table names
            tenant_id: Tenant identifier
            
        Returns:
            Dictionary mapping table name to lag seconds
        """
        if not tables:
            return {}
        
        with self.engine.connect() as conn:
            results = conn.execute(
                text("""
                SELECT table_name, lag_seconds
                FROM mm_cdc_status
                WHERE tenant_id = :tenant_id
                AND table_name = ANY(:tables)
                AND source_id LIKE '%s3%'
                """),
                {
                    "tenant_id": tenant_id,
                    "tables": tables
                }
            ).fetchall()
            
            return {
                r.table_name: r.lag_seconds or 999999
                for r in results
            }
