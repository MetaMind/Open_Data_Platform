"""
Spark Engine - Batch Query Execution

File: metamind/execution/spark_engine.py
Role: Big Data Engineer
Phase: 1
Dependencies: pyspark, pyarrow

Spark batch job execution engine for large-scale analytics.
Routes batch jobs (large aggregations, full table scans) to Spark.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Callable
from enum import Enum

import pyarrow as pa

logger = logging.getLogger(__name__)


class SparkJobStatus(Enum):
    """Spark job execution status."""
    PENDING = "pending"
    SUBMITTED = "submitted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class SparkJobConfig:
    """Spark job configuration."""
    app_name: str = "metamind-batch-query"
    master_url: str = "spark://spark-master:7077"
    executor_memory: str = "4g"
    executor_cores: int = 2
    num_executors: int = 3
    driver_memory: str = "2g"
    sql_shuffle_partitions: int = 200
    default_parallelism: int = 100


@dataclass
class SparkJobResult:
    """Spark job execution result."""
    job_id: str
    status: SparkJobStatus
    columns: List[str]
    data: pa.Table
    row_count: int
    execution_time_ms: int
    spark_metrics: Dict[str, Any]


@dataclass
class SparkJobInfo:
    """Spark job information."""
    job_id: str
    status: SparkJobStatus
    submission_time: Optional[str] = None
    completion_time: Optional[str] = None
    stages: List[Dict[str, Any]] = None
    tasks_total: int = 0
    tasks_completed: int = 0


class SparkEngine:
    """
    Spark batch job execution engine.
    
    Routes batch jobs (large aggregations, full table scans) to Spark.
    Supports:
    - Large-scale aggregations (>1M rows)
    - Full table scans
    - Complex multi-table joins
    - ETL-style transformations
    """
    
    # Thresholds for Spark routing
    BATCH_ROW_THRESHOLD = 1_000_000  # 1M rows
    BATCH_BYTES_THRESHOLD = 100 * 1024 * 1024  # 100MB
    COMPLEX_JOIN_THRESHOLD = 5  # 5+ joins
    
    def __init__(
        self,
        config: Optional[SparkJobConfig] = None,
        enable_hive_support: bool = True
    ):
        """
        Initialize Spark engine.
        
        Args:
            config: Spark job configuration
            enable_hive_support: Enable Hive metastore integration
        """
        self.config = config or SparkJobConfig()
        self.enable_hive_support = enable_hive_support
        self._spark_session: Optional[Any] = None
        self._active_jobs: Dict[str, Any] = {}
        logger.debug(f"SparkEngine initialized: {self.config.master_url}")
    
    def _get_spark_session(self) -> Any:
        """Get or create Spark session."""
        if self._spark_session is None:
            try:
                from pyspark.sql import SparkSession
                
                builder = SparkSession.builder \
                    .appName(self.config.app_name) \
                    .master(self.config.master_url) \
                    .config("spark.executor.memory", self.config.executor_memory) \
                    .config("spark.executor.cores", str(self.config.executor_cores)) \
                    .config("spark.cores.max", str(self.config.num_executors * self.config.executor_cores)) \
                    .config("spark.driver.memory", self.config.driver_memory) \
                    .config("spark.sql.shuffle.partitions", str(self.config.sql_shuffle_partitions)) \
                    .config("spark.default.parallelism", str(self.config.default_parallelism))
                
                if self.enable_hive_support:
                    builder = builder.enableHiveSupport()
                
                self._spark_session = builder.getOrCreate()
                logger.info("Spark session created successfully")
                
            except ImportError:
                logger.error("PySpark not installed")
                raise ImportError("PySpark is required for Spark engine")
        
        return self._spark_session
    
    def is_batch_job(self, features: Dict[str, Any]) -> bool:
        """
        Determine if query should be routed to Spark.
        
        Args:
            features: Query features
            
        Returns:
            True if should route to Spark
        """
        # Check row estimate
        estimated_rows = features.get("estimated_rows", 0)
        if estimated_rows >= self.BATCH_ROW_THRESHOLD:
            return True
        
        # Check complexity
        num_joins = features.get("num_joins", 0)
        if num_joins >= self.COMPLEX_JOIN_THRESHOLD:
            return True
        
        # Check if full table scan without filters
        if not features.get("has_where", False) and features.get("num_tables", 0) >= 1:
            table_rows = features.get("total_table_rows", 0)
            if table_rows >= self.BATCH_ROW_THRESHOLD:
                return True
        
        # Check for complex aggregations
        num_aggregates = features.get("num_aggregates", 0)
        if num_aggregates >= 10:
            return True
        
        return False
    
    async def submit_job(
        self,
        sql: str,
        job_name: Optional[str] = None,
        timeout_seconds: int = 3600
    ) -> str:
        """
        Submit a Spark SQL job asynchronously.
        
        Args:
            sql: SQL query to execute
            job_name: Optional job name
            timeout_seconds: Job timeout
            
        Returns:
            Job ID
        """
        import uuid
        import asyncio
        import time
        
        job_id = str(uuid.uuid4())
        job_name = job_name or f"metamind-job-{job_id[:8]}"
        
        spark = self._get_spark_session()
        
        # Submit job in background
        def _execute():
            try:
                self._active_jobs[job_id] = {
                    "status": SparkJobStatus.RUNNING,
                    "start_time": time.monotonic(),
                }
                
                # Execute SQL
                df = spark.sql(sql)
                
                # Collect results (for smaller results, use toPandas)
                # For large results, should write to storage
                pandas_df = df.toPandas()
                
                self._active_jobs[job_id] = {
                    "status": SparkJobStatus.SUCCEEDED,
                    "result": pandas_df,
                    "end_time": time.monotonic(),
                }
                
            except Exception as e:
                logger.error(f"Spark job {job_id} failed: {e}")
                self._active_jobs[job_id] = {
                    "status": SparkJobStatus.FAILED,
                    "error": str(e),
                    "end_time": time.monotonic(),
                }
        
        # Run in thread pool
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, _execute)
        
        logger.info(f"Spark job submitted: {job_id}")
        return job_id
    
    async def execute(
        self,
        sql: str,
        timeout_seconds: int = 3600
    ) -> SparkJobResult:
        """
        Execute SQL and wait for result (blocking).
        
        Args:
            sql: SQL query
            timeout_seconds: Execution timeout
            
        Returns:
            SparkJobResult with data
        """
        import time
        
        spark = self._get_spark_session()
        start_time = time.time()
        
        try:
            # Execute SQL
            df = spark.sql(sql)
            
            # Get schema
            columns = [field.name for field in df.schema.fields]
            
            # Collect results
            # For large results, consider writing to Parquet instead
            rows = df.collect()
            
            # Convert to Arrow
            data_dict = {col: [] for col in columns}
            for row in rows:
                for i, col in enumerate(columns):
                    data_dict[col].append(row[i])
            
            arrow_table = pa.Table.from_pydict(data_dict)
            
            execution_time_ms = int((time.time() - start_time) * 1000)
            
            # Get Spark metrics
            spark_metrics = self._get_spark_metrics()
            
            return SparkJobResult(
                job_id="sync-" + str(int(start_time)),
                status=SparkJobStatus.SUCCEEDED,
                columns=columns,
                data=arrow_table,
                row_count=len(rows),
                execution_time_ms=execution_time_ms,
                spark_metrics=spark_metrics
            )
            
        except Exception as e:
            logger.error(f"Spark execution failed: {e}")
            raise SparkExecutionError(f"Spark execution failed: {e}")
    
    def _get_spark_metrics(self) -> Dict[str, Any]:
        """Get Spark execution metrics."""
        try:
            spark = self._get_spark_session()
            sc = spark.sparkContext
            
            return {
                "executor_memory": self.config.executor_memory,
                "executor_cores": self.config.executor_cores,
                "num_executors": self.config.num_executors,
                "active_jobs": len(self._active_jobs),
            }
        except Exception as e:
            logger.warning(f"Failed to get Spark metrics: {e}")
            return {}
    
    async def get_job_status(self, job_id: str) -> SparkJobInfo:
        """
        Get job status.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job status information
        """
        job_info = self._active_jobs.get(job_id, {})
        
        return SparkJobInfo(
            job_id=job_id,
            status=job_info.get("status", SparkJobStatus.PENDING),
            submission_time=job_info.get("start_time"),
            completion_time=job_info.get("end_time"),
            stages=[],
            tasks_total=0,
            tasks_completed=0
        )
    
    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if cancelled
        """
        if job_id in self._active_jobs:
            self._active_jobs[job_id]["status"] = SparkJobStatus.CANCELLED
            logger.info(f"Spark job cancelled: {job_id}")
            return True
        return False
    
    async def write_to_table(
        self,
        sql: str,
        output_table: str,
        format: str = "parquet",
        mode: str = "overwrite"
    ) -> str:
        """
        Execute SQL and write results to table.
        
        Args:
            sql: SQL query
            output_table: Output table name
            format: Output format (parquet, delta, iceberg)
            mode: Write mode (overwrite, append)
            
        Returns:
            Job ID
        """
        import uuid
        
        job_id = str(uuid.uuid4())
        spark = self._get_spark_session()
        
        def _write():
            try:
                df = spark.sql(sql)
                
                writer = df.write.mode(mode)
                
                if format == "delta":
                    writer = writer.format("delta")
                elif format == "iceberg":
                    writer = writer.format("iceberg")
                else:
                    writer = writer.format("parquet")
                
                writer.saveAsTable(output_table)
                
                self._active_jobs[job_id] = {
                    "status": SparkJobStatus.SUCCEEDED,
                    "output_table": output_table
                }
                
            except Exception as e:
                logger.error(f"Spark write job {job_id} failed: {e}")
                self._active_jobs[job_id] = {
                    "status": SparkJobStatus.FAILED,
                    "error": str(e)
                }
        
        import asyncio
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, _write)
        
        return job_id
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check Spark cluster health.
        
        Returns:
            Health status
        """
        try:
            spark = self._get_spark_session()
            sc = spark.sparkContext
            
            # Test with simple query
            test_df = spark.range(1)
            test_df.collect()
            
            return {
                "status": "healthy",
                "spark_version": spark.version,
                "master_url": sc.master,
                "app_id": sc.applicationId,
                "active_jobs": len(self._active_jobs)
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    async def close(self) -> None:
        """Close Spark session."""
        if self._spark_session:
            self._spark_session.stop()
            self._spark_session = None
            logger.info("Spark session closed")


class SparkExecutionError(Exception):
    """Spark execution error."""
    pass
