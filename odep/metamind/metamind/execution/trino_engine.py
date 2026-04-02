"""
Trino Engine - Async Query Execution

File: metamind/execution/trino_engine.py
Role: Senior Backend Engineer
Phase: 1
Dependencies: aiohttp, pyarrow

Async Trino query engine with Arrow result conversion.
Supports streaming, cancellation, and query monitoring.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, AsyncIterator

import aiohttp
import pyarrow as pa
from aiohttp import ClientTimeout

logger = logging.getLogger(__name__)


@dataclass
class TrinoResult:
    """Query execution result with Arrow format."""
    columns: List[str]
    data: pa.Table
    row_count: int
    execution_time_ms: int
    query_id: str
    query_metadata: Dict[str, Any]


@dataclass
class TrinoQueryInfo:
    """Query statistics from Trino."""
    state: str  # QUEUED, PLANNING, STARTING, RUNNING, FINISHED, FAILED
    elapsed_time_ms: int
    queued_time_ms: int
    cpu_time_ms: int
    processed_rows: int
    processed_bytes: int
    peak_memory_bytes: int


class TrinoEngine:
    """
    Async Trino query engine with Arrow result conversion.
    Supports streaming, cancellation, and query monitoring.
    """
    
    def __init__(
        self,
        coordinator_url: str,
        user: str,
        password: Optional[str] = None,
        catalog: str = "iceberg",
        schema: str = "default",
        max_concurrent: int = 100,
        query_timeout: int = 300
    ):
        """
        Initialize Trino engine.
        
        Args:
            coordinator_url: Trino coordinator URL
            user: Trino user
            password: Optional password
            catalog: Default catalog
            schema: Default schema
            max_concurrent: Max concurrent queries
            query_timeout: Query timeout in seconds
        """
        self.coordinator = coordinator_url.rstrip("/")
        self.user = user
        self.password = password
        self.catalog = catalog
        self.schema = schema
        self.max_concurrent = max_concurrent
        self.timeout = ClientTimeout(total=query_timeout)
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(max_concurrent)
        logger.debug(f"TrinoEngine initialized: {coordinator_url}")
    
    async def connect(self) -> None:
        """Initialize HTTP session."""
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers=self._default_headers()
            )
            logger.debug("Trino HTTP session created")
    
    def _default_headers(self) -> Dict[str, str]:
        """Build Trino HTTP headers."""
        headers = {
            "X-Trino-User": self.user,
            "X-Trino-Catalog": self.catalog,
            "X-Trino-Schema": self.schema,
            "Content-Type": "text/plain",
            "Accept": "application/json"
        }
        if self.password:
            import base64
            auth = base64.b64encode(
                f"{self.user}:{self.password}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {auth}"
        return headers
    
    async def execute(
        self,
        sql: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None
    ) -> TrinoResult:
        """
        Execute SQL and return full materialized result.
        
        Args:
            sql: SQL query
            catalog: Optional catalog override
            schema: Optional schema override
            
        Returns:
            TrinoResult with Arrow data
        """
        await self.connect()
        
        async with self._semaphore:
            start_time = asyncio.get_running_loop().time()
            
            # Submit query
            headers = self._default_headers()
            if catalog:
                headers["X-Trino-Catalog"] = catalog
            if schema:
                headers["X-Trino-Schema"] = schema
            
            async with self._session.post(
                f"{self.coordinator}/v1/statement",
                headers=headers,
                data=sql.encode("utf-8")
            ) as resp:
                result = await resp.json()
                next_uri = result.get("nextUri")
                query_id = result.get("id")
                
                if result.get("error"):
                    raise TrinoQueryError(result["error"])
                
                # Collect all data
                all_data: List[List[Any]] = []
                columns: Optional[List[str]] = None
                
                while next_uri:
                    async with self._session.get(next_uri, headers=headers) as poll:
                        poll_result = await poll.json()
                        
                        # Extract schema on first batch
                        if poll_result.get("columns") and not columns:
                            columns = [
                                col["name"] for col in poll_result["columns"]
                            ]
                        
                        # Extract data rows
                        if poll_result.get("data"):
                            all_data.extend(poll_result["data"])
                        
                        # Check for errors
                        if poll_result.get("error"):
                            raise TrinoQueryError(poll_result["error"])
                        
                        next_uri = poll_result.get("nextUri")
                        
                        # Log progress for long queries
                        if len(all_data) % 10000 == 0 and len(all_data) > 0:
                            logger.debug(
                                f"Trino query {query_id}: "
                                f"{len(all_data)} rows fetched"
                            )
                
                # Convert to Arrow
                arrow_table = self._to_arrow(columns or [], all_data)
                elapsed_ms = int(
                    (asyncio.get_running_loop().time() - start_time) * 1000
                )
                
                return TrinoResult(
                    columns=columns or [],
                    data=arrow_table,
                    row_count=len(all_data),
                    execution_time_ms=elapsed_ms,
                    query_id=query_id,
                    query_metadata={"sql": sql[:1000]}
                )
    
    async def execute_streaming(
        self,
        sql: str,
        batch_size: int = 10000
    ) -> AsyncIterator[pa.RecordBatch]:
        """
        Stream results in Arrow batches for large queries.
        
        Args:
            sql: SQL query
            batch_size: Batch size for streaming
            
        Yields:
            Arrow RecordBatches
        """
        await self.connect()
        
        async with self._semaphore:
            headers = self._default_headers()
            
            async with self._session.post(
                f"{self.coordinator}/v1/statement",
                headers=headers,
                data=sql.encode("utf-8")
            ) as resp:
                result = await resp.json()
                next_uri = result.get("nextUri")
                query_id = result.get("id")
                
                columns: Optional[List[str]] = None
                buffer: List[List[Any]] = []
                
                while next_uri:
                    async with self._session.get(next_uri, headers=headers) as poll:
                        poll_result = await poll.json()
                        
                        if poll_result.get("columns") and not columns:
                            columns = [
                                col["name"] for col in poll_result["columns"]
                            ]
                        
                        if poll_result.get("data"):
                            buffer.extend(poll_result["data"])
                            
                            # Yield batch when full
                            while len(buffer) >= batch_size:
                                batch = buffer[:batch_size]
                                buffer = buffer[batch_size:]
                                yield self._to_batch(columns or [], batch)
                        
                        if poll_result.get("error"):
                            raise TrinoQueryError(poll_result["error"])
                        
                        next_uri = poll_result.get("nextUri")
                
                # Yield remaining
                if buffer and columns:
                    yield self._to_batch(columns, buffer)
    
    def _to_arrow(
        self,
        columns: List[str],
        rows: List[List[Any]]
    ) -> pa.Table:
        """Convert JSON rows to Arrow Table with type inference."""
        if not rows:
            return pa.Table.from_pydict({col: [] for col in columns})
        
        # Transpose to columnar format
        columnar = list(zip(*rows))
        arrays = []
        
        for i, col_name in enumerate(columns):
            values = columnar[i] if i < len(columnar) else []
            arrow_type = self._infer_type(values)
            arrays.append(pa.array(values, type=arrow_type))
        
        return pa.Table.from_arrays(arrays, names=columns)
    
    def _to_batch(
        self,
        columns: List[str],
        rows: List[List[Any]]
    ) -> pa.RecordBatch:
        """Convert rows to Arrow RecordBatch."""
        columnar = list(zip(*rows))
        arrays = []
        
        for i, col_name in enumerate(columns):
            values = columnar[i] if i < len(columnar) else []
            arrow_type = self._infer_type(values)
            arrays.append(pa.array(values, type=arrow_type))
        
        return pa.RecordBatch.from_arrays(arrays, names=columns)
    
    def _infer_type(self, values: List[Any]) -> pa.DataType:
        """Infer Arrow type from values."""
        for v in values:
            if v is None:
                continue
            if isinstance(v, bool):
                return pa.bool_()
            if isinstance(v, int):
                return pa.int64()
            if isinstance(v, float):
                return pa.float64()
            return pa.string()
        return pa.string()
    
    async def get_query_info(self, query_id: str) -> TrinoQueryInfo:
        """
        Get query execution statistics.
        
        Args:
            query_id: Trino query ID
            
        Returns:
            TrinoQueryInfo with statistics
        """
        await self.connect()
        
        async with self._session.get(
            f"{self.coordinator}/v1/query/{query_id}"
        ) as resp:
            data = await resp.json()
            stats = data.get("queryStats", {})
            
            return TrinoQueryInfo(
                state=data.get("state", "UNKNOWN"),
                elapsed_time_ms=self._parse_duration(
                    stats.get("elapsedTime", "0ms")
                ),
                queued_time_ms=self._parse_duration(
                    stats.get("queuedTime", "0ms")
                ),
                cpu_time_ms=self._parse_duration(
                    stats.get("totalCpuTime", "0ms")
                ),
                processed_rows=stats.get("processedRows", 0),
                processed_bytes=stats.get("processedBytes", 0),
                peak_memory_bytes=stats.get("peakMemoryBytes", 0)
            )
    
    def _parse_duration(self, duration_str: str) -> int:
        """Parse Trino duration string to milliseconds."""
        match = re.match(r"([\d.]+)([a-z]+)", duration_str)
        if not match:
            return 0
        
        value, unit = float(match.group(1)), match.group(2)
        multipliers = {
            "ms": 1, "s": 1000, "m": 60000,
            "h": 3600000, "d": 86400000
        }
        return int(value * multipliers.get(unit, 1))
    
    async def cancel_query(self, query_id: str) -> None:
        """
        Cancel running query.
        
        Args:
            query_id: Trino query ID to cancel
        """
        await self.connect()
        
        try:
            await self._session.delete(
                f"{self.coordinator}/v1/query/{query_id}"
            )
            logger.info(f"Cancelled Trino query {query_id}")
        except Exception as e:
            logger.error(f"Failed to cancel query {query_id}: {e}")
    
    async def close(self) -> None:
        """Close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
            logger.debug("Trino HTTP session closed")


class TrinoQueryError(Exception):
    """Trino query execution error."""
    pass
