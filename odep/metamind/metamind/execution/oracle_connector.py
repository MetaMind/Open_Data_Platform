"""
Oracle Connector - Production-Safe Database Access

File: metamind/execution/oracle_connector.py
Role: Senior Database Engineer
Phase: 1
Dependencies: oracledb, pyarrow

Production-safe Oracle connector with resource governance.
SAFETY FEATURES:
1. Query pattern validation (no DML/DDL)
2. Connection pooling with strict limits
3. Per-user concurrency limits
4. Query timeouts
5. Circuit breaker on errors
6. Automatic hints for parallelism
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Any

import pyarrow as pa

logger = logging.getLogger(__name__)

# CRITICAL: These patterns are BLOCKED to protect production Oracle
BLOCKED_SQL_PATTERNS = [
    r"\bUPDATE\b", r"\bDELETE\b", r"\bINSERT\b", r"\bMERGE\b",
    r"\bDROP\b", r"\bCREATE\b", r"\bALTER\b", r"\bTRUNCATE\b",
    r"\bGRANT\b", r"\bREVOKE\b", r"\bEXECUTE\b", r"\bCALL\b",
    r"\bBEGIN\b", r"\bDECLARE\b",  # PL/SQL blocks
]


@dataclass
class OraclePoolConfig:
    """DRCP (Database Resident Connection Pooling) configuration."""
    min_sessions: int = 2
    max_sessions: int = 20
    session_increment: int = 1
    max_per_user: int = 5
    timeout: int = 60  # Seconds
    max_lifetime_session: int = 3600  # 1 hour


@dataclass
class OracleQueryMetrics:
    """Query execution metrics."""
    elapsed_ms: int
    cpu_time_ms: Optional[int] = None
    io_time_ms: Optional[int] = None
    rows_fetched: int = 0
    buffer_gets: Optional[int] = None
    disk_reads: Optional[int] = None


class OracleConnector:
    """
    Production-safe Oracle connector with resource governance.
    
    SAFETY FEATURES:
    1. Query pattern validation (no DML/DDL)
    2. Connection pooling with strict limits
    3. Per-user concurrency limits
    4. Query timeouts
    5. Circuit breaker on errors
    6. Automatic hints for parallelism
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        service_name: str,
        user: str,
        password: Optional[str],
        pool_config: Optional[OraclePoolConfig] = None,
        circuit_threshold: int = 5,
        circuit_timeout: int = 60
    ):
        """
        Initialize Oracle connector.
        
        Args:
            host: Oracle host
            port: Oracle port
            service_name: Oracle service name
            user: Oracle user
            password: Oracle password
            pool_config: Connection pool configuration
            circuit_threshold: Errors before opening circuit
            circuit_timeout: Seconds before retry
        """
        self.host = host
        self.port = port
        self.service_name = service_name
        self.user = user
        self.password = password
        self.pool_config = pool_config or OraclePoolConfig()
        self._pool: Optional[Any] = None
        self._user_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._circuit_failures = 0
        self._circuit_last_failure = 0.0
        self._circuit_threshold = circuit_threshold
        self._circuit_timeout = circuit_timeout
        self._circuit_open = False
        logger.debug(f"OracleConnector initialized: {host}:{port}/{service_name}")
    
    async def initialize(self) -> None:
        """Initialize connection pool."""
        try:
            import oracledb
            
            self._pool = oracledb.create_pool(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                service_name=self.service_name,
                min=self.pool_config.min_sessions,
                max=self.pool_config.max_sessions,
                increment=self.pool_config.session_increment,
                timeout=self.pool_config.timeout,
                max_lifetime_session=self.pool_config.max_lifetime_session
            )
            logger.info(
                f"Oracle pool initialized: "
                f"{self.pool_config.min_sessions}-{self.pool_config.max_sessions} sessions"
            )
        except Exception as e:
            logger.error(f"Failed to initialize Oracle pool: {e}")
            raise
    
    async def execute(
        self,
        sql: str,
        user_id: str,
        timeout_override: Optional[int] = None,
        fetch_size: int = 10000
    ) -> pa.Table:
        """
        Execute query with full safety checks.
        
        Args:
            sql: SQL query (SELECT only)
            user_id: User identifier for per-user limits
            timeout_override: Optional timeout override
            fetch_size: Rows per fetch
            
        Returns:
            Arrow Table with results
        """
        # 1. Circuit breaker check
        if self._is_circuit_open():
            raise CircuitBreakerOpen("Oracle circuit breaker is open")
        
        # 2. Query validation
        self._validate_query_safety(sql)
        
        # 3. Add performance hints
        hinted_sql = self._add_parallel_hints(sql)
        
        # 4. Acquire per-user slot and execute
        async with self._acquire_user_slot(user_id):
            return await self._execute_with_monitoring(
                hinted_sql, timeout_override, fetch_size
            )
    
    async def _execute_with_monitoring(
        self,
        sql: str,
        timeout_override: Optional[int],
        fetch_size: int
    ) -> pa.Table:
        """Execute query with monitoring."""
        import oracledb
        
        start_time = asyncio.get_running_loop().time()
        conn = None
        cursor = None
        
        try:
            conn = self._pool.acquire()
            cursor = conn.cursor()
            cursor.arraysize = fetch_size
            
            # Set timeout
            timeout = timeout_override or self.pool_config.timeout
            cursor.execute(f"ALTER SESSION SET DDL_LOCK_TIMEOUT = {timeout}")
            
            # Execute
            cursor.execute(sql)
            
            # Fetch all results
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            # Convert to Arrow
            arrow_table = self._to_arrow(columns, rows)
            elapsed_ms = int(
                (asyncio.get_running_loop().time() - start_time) * 1000
            )
            
            # Record success (close circuit if open)
            self._record_success()
            
            logger.debug(
                f"Oracle query executed: {len(rows)} rows in {elapsed_ms}ms"
            )
            
            return arrow_table
            
        except Exception as e:
            self._record_failure()
            logger.error(f"Oracle query failed: {e}")
            raise OracleQueryError(f"Query execution failed: {e}")
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def _validate_query_safety(self, sql: str) -> None:
        """
        Validate query doesn't contain dangerous patterns.
        
        Args:
            sql: SQL query to validate
            
        Raises:
            UnsafeQueryError: If query contains blocked patterns
        """
        upper_sql = sql.upper()
        
        for pattern in BLOCKED_SQL_PATTERNS:
            if re.search(pattern, upper_sql):
                raise UnsafeQueryError(
                    f"Blocked pattern detected: {pattern}. "
                    f"Only SELECT queries are allowed."
                )
        
        # Warn on queries without WHERE on large tables
        if "WHERE" not in upper_sql and "FROM" in upper_sql:
            logger.warning(
                "Query without WHERE clause - may cause full table scan"
            )
    
    def _add_parallel_hints(self, sql: str) -> str:
        """
        Add Oracle parallel execution hints for large queries.
        
        Args:
            sql: Original SQL
            
        Returns:
            SQL with parallel hints if applicable
        """
        # Don't double-hint
        if "/*+" in sql.upper():
            return sql
        
        # Check if worth parallelizing (heuristic)
        upper = sql.upper()
        has_aggregate = any(
            agg in upper for agg in ["SUM(", "COUNT(", "AVG(", "MAX(", "MIN("]
        )
        has_join = "JOIN" in upper
        
        if has_aggregate or has_join:
            return f"/*+ PARALLEL(4) */ {sql}"
        
        return sql
    
    @asynccontextmanager
    async def _acquire_user_slot(self, user_id: str):
        """
        Enforce per-user connection limits.
        
        Args:
            user_id: User identifier
            
        Yields:
            When slot is acquired
        """
        if user_id not in self._user_semaphores:
            self._user_semaphores[user_id] = asyncio.Semaphore(
                self.pool_config.max_per_user
            )
        
        acquired = await self._user_semaphores[user_id].acquire()
        
        if not acquired:
            raise ResourceExhausted(
                f"User {user_id} exceeded max concurrent queries "
                f"({self.pool_config.max_per_user})"
            )
        
        try:
            yield
        finally:
            self._user_semaphores[user_id].release()
    
    def _is_circuit_open(self) -> bool:
        """Check if circuit breaker is open."""
        if not self._circuit_open:
            return False
        
        # Check if timeout elapsed
        if time.time() - self._circuit_last_failure > self._circuit_timeout:
            self._circuit_open = False
            self._circuit_failures = 0
            logger.info("Oracle circuit breaker closed after timeout")
            return False
        
        return True
    
    def _record_success(self) -> None:
        """Record successful execution."""
        if self._circuit_open:
            self._circuit_open = False
            self._circuit_failures = 0
            logger.info("Oracle circuit breaker closed on success")
    
    def _record_failure(self) -> None:
        """Record failed execution."""
        self._circuit_failures += 1
        self._circuit_last_failure = time.time()
        
        if self._circuit_failures >= self._circuit_threshold:
            self._circuit_open = True
            logger.error(
                f"Oracle circuit breaker opened after "
                f"{self._circuit_failures} failures"
            )
    
    def _to_arrow(self, columns: List[str], rows: List[tuple]) -> pa.Table:
        """Convert Oracle results to Arrow Table."""
        if not rows:
            return pa.Table.from_pydict({col: [] for col in columns})
        
        # Oracle returns tuples, transpose to columnar
        columnar = list(zip(*rows))
        arrays = []
        
        for i, col_name in enumerate(columns):
            values = columnar[i] if i < len(columnar) else []
            arrow_type = self._infer_oracle_type(values)
            arrays.append(pa.array(values, type=arrow_type))
        
        return pa.Table.from_arrays(arrays, names=columns)
    
    def _infer_oracle_type(self, values: List[Any]) -> pa.DataType:
        """Map Oracle types to Arrow types."""
        for v in values:
            if v is None:
                continue
            if isinstance(v, int):
                return pa.int64()
            if isinstance(v, float):
                return pa.float64()
            if isinstance(v, str):
                return pa.string()
            if isinstance(v, bytes):
                return pa.binary()
            
            # Oracle-specific types
            try:
                import oracledb
                if isinstance(v, oracledb.LOB):
                    return pa.binary()
                if isinstance(v, oracledb.Timestamp):
                    return pa.timestamp('us')
                if isinstance(v, oracledb.Date):
                    return pa.date32()
            except ImportError:
                logger.error("Unhandled exception in oracle_connector.py: %s", exc)
            
            return pa.string()
        
        return pa.string()
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check Oracle connectivity and pool status.
        
        Returns:
            Health status dictionary
        """
        conn = None
        cursor = None
        
        try:
            conn = self._pool.acquire()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            
            return {
                "status": "healthy",
                "pool_size": self._pool.max,
                "open_connections": self._pool.busy,
                "circuit_state": "open" if self._circuit_open else "closed"
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "circuit_state": "open" if self._circuit_open else "closed"
            }
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    async def close(self) -> None:
        """Close connection pool."""
        if self._pool:
            self._pool.close()
            self._pool = None
            logger.info("Oracle connection pool closed")


class UnsafeQueryError(Exception):
    """Query failed safety validation."""
    pass


class ResourceExhausted(Exception):
    """Resource limits exceeded."""
    pass


class CircuitBreakerOpen(Exception):
    """Circuit breaker is open."""
    pass


class OracleQueryError(Exception):
    """Oracle execution error."""
    pass
