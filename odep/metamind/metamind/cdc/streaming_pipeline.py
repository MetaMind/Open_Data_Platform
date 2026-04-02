"""
CDC Streaming Pipeline — Kafka Consumer & Spark Iceberg MERGE Job

File: metamind/cdc/streaming_pipeline.py
Role: Data Engineer / Distributed Systems Engineer
Addresses: Critical Gap — CDC streaming pipeline entirely absent from both codebases.

Implements the full Debezium → Kafka → Spark → Iceberg pipeline:

  Oracle LogMiner / Redo Log
        │
   Debezium Connector (managed via Kafka Connect REST API)
        │
   Kafka Topic  (mm.{schema}.{table})
        │
   Spark Structured Streaming  (this file)
        │
   Iceberg MERGE INTO  (S3 / MinIO)
        │
   mm_cdc_status update  (PostgreSQL — CDCMonitor reads this)

The pipeline runs as a long-lived Spark job managed by the
MetaMind control plane.  Each table gets its own streaming query
so lag is tracked per-table independently.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


from metamind.cdc.debezium_manager import DebeziumConnectorConfig, DebeziumConnectorManager  # noqa: F401
from metamind.cdc.iceberg_pipeline import StreamingPipelineConfig, IcebergMergePipeline  # noqa: F401

class CDCPipelineOrchestrator:
    """
    Manages multiple table CDC pipelines from a single Spark session.

    One pipeline per table.  The orchestrator monitors pipeline health
    and restarts failed pipelines automatically.
    """

    def __init__(
        self,
        spark: Any,
        debezium_manager: DebeziumConnectorManager,
    ) -> None:
        self.spark = spark
        self.debezium = debezium_manager
        self._pipelines: Dict[str, IcebergMergePipeline] = {}

    def register_table(
        self,
        connector_config: DebeziumConnectorConfig,
        pipeline_config: StreamingPipelineConfig,
    ) -> None:
        """Register a table for CDC and start its pipeline."""
        # Ensure Debezium connector is running
        self.debezium.create_or_update_connector(connector_config)

        # Start Spark streaming pipeline
        pipeline = IcebergMergePipeline(pipeline_config)
        pipeline.start(self.spark)
        self._pipelines[pipeline_config.table_name] = pipeline
        logger.info("CDC pipeline registered for table: %s", pipeline_config.table_name)

    def monitor_and_restart(self) -> None:
        """Check all pipelines and restart any that have terminated unexpectedly."""
        for table_name, pipeline in list(self._pipelines.items()):
            if pipeline._query and not pipeline._query.isActive:
                logger.warning("Pipeline for %s is not active — restarting", table_name)
                try:
                    pipeline.start(self.spark)
                except Exception as exc:
                    logger.error("Failed to restart pipeline for %s: %s", table_name, exc)

    def stop_all(self) -> None:
        """Stop all running pipelines cleanly."""
        for pipeline in self._pipelines.values():
            try:
                pipeline.stop()
            except Exception as exc:
                logger.warning("Error stopping pipeline: %s", exc)
        self._pipelines.clear()


# ---------------------------------------------------------------------------
# Entrypoint  (run as: spark-submit metamind/cdc/streaming_pipeline.py)
# ---------------------------------------------------------------------------


def main() -> None:
    """
    CLI entrypoint for submitting the CDC pipeline to a Spark cluster.

    Configuration is read from environment variables:
        METAMIND_CDC_KAFKA_BOOTSTRAP
        METAMIND_CDC_ORACLE_HOST / PORT / SERVICE / USER / PASSWORD
        METAMIND_CDC_TABLES            (comma-separated SCHEMA.TABLE list)
        METAMIND_CDC_ICEBERG_CATALOG
        METAMIND_CDC_CHECKPOINT_DIR
        METAMIND_CDC_METADATA_DB_URL   (JDBC URL for mm_cdc_status)
        METAMIND_CDC_KAFKA_CONNECT_URL
    """
    try:
        from pyspark.sql import SparkSession  # type: ignore
    except ImportError:
        logger.error("PySpark not available — install pyspark to run the CDC pipeline")
        return

    kafka_bootstrap = os.environ.get("METAMIND_CDC_KAFKA_BOOTSTRAP", "kafka:9092")
    oracle_host = os.environ.get("METAMIND_CDC_ORACLE_HOST", "oracle")
    oracle_port = int(os.environ.get("METAMIND_CDC_ORACLE_PORT", "1521"))
    oracle_service = os.environ.get("METAMIND_CDC_ORACLE_SERVICE", "ORCLPDB1")
    oracle_user = os.environ.get("METAMIND_CDC_ORACLE_USER", "metamind_cdc")
    oracle_password = os.environ.get("METAMIND_CDC_ORACLE_PASSWORD", "")
    raw_tables = os.environ.get("METAMIND_CDC_TABLES", "")
    iceberg_catalog = os.environ.get("METAMIND_CDC_ICEBERG_CATALOG", "iceberg")
    checkpoint_dir = os.environ.get("METAMIND_CDC_CHECKPOINT_DIR", "s3a://metamind/checkpoints")
    metadata_db_url = os.environ.get("METAMIND_CDC_METADATA_DB_URL", "")
    connect_url = os.environ.get("METAMIND_CDC_KAFKA_CONNECT_URL", "http://kafka-connect:8083")

    tables = [t.strip() for t in raw_tables.split(",") if t.strip()]
    if not tables:
        logger.error("No tables configured — set METAMIND_CDC_TABLES")
        return

    spark = (
        SparkSession.builder
        .appName("MetaMind-CDC-Pipeline")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.defaultCatalog", iceberg_catalog)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    connector_cfg = DebeziumConnectorConfig(
        connector_name="metamind-oracle-cdc",
        oracle_host=oracle_host,
        oracle_port=oracle_port,
        oracle_service=oracle_service,
        oracle_user=oracle_user,
        oracle_password=oracle_password,
        tables=tables,
        kafka_bootstrap=kafka_bootstrap,
    )

    orchestrator = CDCPipelineOrchestrator(
        spark=spark,
        debezium_manager=DebeziumConnectorManager(connect_url),
    )

    for table in tables:
        schema, tbl = (table.split(".", 1) + ["unknown"])[:2]
        iceberg_tbl = f"{iceberg_catalog}.default.{tbl.lower()}"
        pipeline_cfg = StreamingPipelineConfig(
            table_name=table,
            kafka_topic=f"mm.{table}",
            iceberg_table=iceberg_tbl,
            primary_keys=["id"],           # Override per table in real deployment
            kafka_bootstrap=kafka_bootstrap,
            iceberg_catalog=iceberg_catalog,
            checkpoint_location=checkpoint_dir,
            metadata_db_url=metadata_db_url,
        )
        orchestrator.register_table(connector_cfg, pipeline_cfg)

    # Keep the driver alive — Spark streaming is async
    logger.info("All CDC pipelines running. Awaiting termination…")
    try:
        while True:
            orchestrator.monitor_and_restart()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down CDC pipelines…")
        orchestrator.stop_all()
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
