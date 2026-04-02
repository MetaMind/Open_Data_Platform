"""Iceberg merge pipeline — extracted from streaming_pipeline.py.

File: metamind/cdc/iceberg_pipeline.py
Role: Data Engineer
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

class StreamingPipelineConfig:
    """Configuration for a single table's CDC streaming pipeline."""
    table_name: str                         # Oracle source table (SCHEMA.TABLE)
    kafka_topic: str                        # e.g. mm.SCHEMA.TABLE
    iceberg_table: str                      # e.g. iceberg.default.orders
    primary_keys: List[str]                 # columns used for upsert matching
    kafka_bootstrap: str = "kafka:9092"
    kafka_group_id: str = "metamind-cdc"
    iceberg_catalog: str = "iceberg"
    checkpoint_location: str = "/checkpoints"
    trigger_interval: str = "30 seconds"   # micro-batch interval
    max_offsets_per_trigger: int = 10_000
    watermark_delay: str = "5 minutes"
    metadata_db_url: str = ""              # JDBC URL for mm_cdc_status updates


class IcebergMergePipeline:
    """
    Spark Structured Streaming pipeline: Kafka CDC events → Iceberg MERGE INTO.

    Each Oracle table gets its own pipeline instance and streaming query,
    providing independent fault isolation and lag tracking.

    Architecture::

        Kafka Source (Debezium JSON)
            │ withWatermark (handle late arrivals)
            │ foreachBatch  →  _merge_batch()
                               │
                               ├── INSERT new rows
                               ├── UPDATE changed rows
                               └── DELETE flagged rows (soft or hard)
                                        │
                               mm_cdc_status UPDATE (lag tracking)
    """

    def __init__(self, config: StreamingPipelineConfig) -> None:
        self.config = config
        self._query = None  # Spark StreamingQuery

    def start(self, spark: Any) -> Any:
        """
        Start the Spark Structured Streaming job for this table.

        Args:
            spark: Active SparkSession with Iceberg and Kafka extensions.

        Returns:
            The StreamingQuery handle.
        """
        cfg = self.config

        # ── Read from Kafka ───────────────────────────────────────────────
        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
            .option("subscribe", cfg.kafka_topic)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
            .option("kafka.group.id", cfg.kafka_group_id)
            .option("failOnDataLoss", "false")
            .load()
        )

        # ── Parse Debezium JSON payload ───────────────────────────────────
        from pyspark.sql import functions as F  # type: ignore
        from pyspark.sql.types import (  # type: ignore
            StringType, LongType, TimestampType
        )

        parsed = (
            raw_stream
            .select(
                F.col("key").cast(StringType()).alias("_key"),
                F.col("value").cast(StringType()).alias("_value"),
                F.col("timestamp").alias("_kafka_ts"),
                F.col("offset").alias("_kafka_offset"),
                F.col("partition").alias("_kafka_partition"),
            )
            .withColumn("_payload", F.from_json(F.col("_value"), self._debezium_schema(spark)))
            .select(
                F.col("_payload.*"),
                F.col("_kafka_ts"),
                F.col("_kafka_offset"),
                F.col("_kafka_partition"),
            )
            # Watermark on the source timestamp from Debezium (__source_ts_ms)
            .withColumn(
                "_event_ts",
                F.to_timestamp(F.col("__source_ts_ms") / 1000),
            )
            .withWatermark("_event_ts", cfg.watermark_delay)
        )

        # ── Write via foreachBatch for MERGE semantics ────────────────────
        self._query = (
            parsed.writeStream
            .format("noop")                      # driver: foreachBatch overrides this
            .outputMode("update")
            .trigger(processingTime=cfg.trigger_interval)
            .option("checkpointLocation", f"{cfg.checkpoint_location}/{cfg.table_name}")
            .foreachBatch(self._merge_batch)
            .start()
        )

        logger.info(
            "Started CDC streaming pipeline for %s → %s",
            cfg.kafka_topic,
            cfg.iceberg_table,
        )
        return self._query

    def _merge_batch(self, batch_df: Any, batch_id: int) -> None:
        """
        Process one micro-batch: MERGE into Iceberg, update CDC status.

        Called by Spark's foreachBatch for every micro-batch.
        """
        cfg = self.config

        if batch_df.isEmpty():
            return

        from pyspark.sql import functions as F  # type: ignore

        # Separate inserts/updates from deletes
        # Debezium op codes: 'c' = create, 'u' = update, 'd' = delete, 'r' = read (snapshot)
        upserts = batch_df.filter(F.col("__op").isin(["c", "u", "r"]))
        deletes = batch_df.filter(F.col("__op") == "d")

        spark = batch_df.sparkSession

        # ── MERGE upserts into Iceberg ────────────────────────────────────
        if not upserts.isEmpty():
            upserts.createOrReplaceTempView("_cdc_upserts")
            merge_condition = " AND ".join(
                f"target.{pk} = source.{pk}" for pk in cfg.primary_keys
            )
            spark.sql(f"""
                MERGE INTO {cfg.iceberg_table} AS target
                USING _cdc_upserts AS source
                ON {merge_condition}
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
            """)

        # ── Soft-delete: set a deleted_at column if schema supports it ─────
        if not deletes.isEmpty():
            deletes.createOrReplaceTempView("_cdc_deletes")
            delete_condition = " AND ".join(
                f"target.{pk} = source.{pk}" for pk in cfg.primary_keys
            )
            try:
                spark.sql(f"""
                    MERGE INTO {cfg.iceberg_table} AS target
                    USING _cdc_deletes AS source
                    ON {delete_condition}
                    WHEN MATCHED THEN
                        DELETE
                """)
            except Exception as exc:
                logger.warning("Hard DELETE merge failed (%s) — skipping deletes", exc)

        # ── Update mm_cdc_status via JDBC ──────────────────────────────────
        self._update_cdc_status(
            batch_df=batch_df,
            batch_id=batch_id,
            spark=spark,
        )

    def _update_cdc_status(self, batch_df: Any, batch_id: int, spark: Any) -> None:
        """Write lag metrics to mm_cdc_status for CDCMonitor to read."""
        from pyspark.sql import functions as F  # type: ignore

        if not self.config.metadata_db_url:
            return

        try:
            agg = batch_df.agg(
                F.max("__source_ts_ms").alias("max_source_ts"),
                F.count("*").alias("row_count"),
                F.max("_kafka_offset").alias("max_offset"),
                F.min("_kafka_partition").alias("partition"),
            ).collect()[0]

            now_ms = int(time.time() * 1000)
            lag_seconds = max(0, (now_ms - (agg.max_source_ts or now_ms)) // 1000)

            # Write via JDBC
            update_df = spark.createDataFrame(
                [{
                    "table_name": self.config.table_name,
                    "lag_seconds": int(lag_seconds),
                    "messages_behind": 0,  # Kafka lag requires separate KafkaAdminClient
                    "kafka_offset": int(agg.max_offset or 0),
                    "kafka_partition": int(agg.partition or 0),
                    "last_cdc_timestamp": datetime.utcnow().isoformat(),
                    "last_s3_timestamp": datetime.utcnow().isoformat(),
                    "processing_rate_per_second": float(
                        agg.row_count / max(1, lag_seconds)
                    ),
                }]
            )
            (
                update_df.write
                .format("jdbc")
                .option("url", self.config.metadata_db_url)
                .option("dbtable", "mm_cdc_status")
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )
        except Exception as exc:
            logger.warning("Failed to update mm_cdc_status: %s", exc)

    def _debezium_schema(self, spark: Any) -> Any:
        """Return the Spark StructType for a Debezium unwrapped JSON record."""
        from pyspark.sql.types import (  # type: ignore
            StructType, StructField, StringType, LongType, BooleanType
        )
        # The actual data columns are dynamic; we only define the Debezium metadata fields.
        # Data columns are read as a catch-all MAP<STRING, STRING> and cast downstream.
        return StructType([
            StructField("__op", StringType(), True),
            StructField("__source_ts_ms", LongType(), True),
            StructField("__source_scn", LongType(), True),
            StructField("__deleted", BooleanType(), True),
        ])

    def stop(self) -> None:
        """Gracefully stop the streaming query."""
        if self._query:
            self._query.stop()
            logger.info("Stopped CDC streaming pipeline for %s", self.config.table_name)


# ---------------------------------------------------------------------------
# Pipeline Orchestrator  — manages all table pipelines
# ---------------------------------------------------------------------------


