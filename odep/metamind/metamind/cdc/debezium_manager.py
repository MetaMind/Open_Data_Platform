"""Debezium connector management — extracted from streaming_pipeline.py.

File: metamind/cdc/debezium_manager.py
Role: Data Engineer
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Debezium Connector Manager  (uses Kafka Connect REST API)
# ---------------------------------------------------------------------------


@dataclass
class DebeziumConnectorConfig:
    """Configuration for a Debezium Oracle source connector."""
    connector_name: str
    oracle_host: str
    oracle_port: int
    oracle_service: str
    oracle_user: str
    oracle_password: str
    tables: List[str]                       # e.g. ["SCHEMA.ORDERS", "SCHEMA.CUSTOMERS"]
    kafka_bootstrap: str = "kafka:9092"
    topic_prefix: str = "mm"               # produces: mm.SCHEMA.TABLE
    poll_interval_ms: int = 500
    batch_size: int = 2048
    snapshot_mode: str = "initial"          # initial | schema_only | never
    log_mining_strategy: str = "online_catalog"


class DebeziumConnectorManager:
    """
    Manages Debezium Oracle CDC connectors via the Kafka Connect REST API.

    Creates, monitors, and restarts connectors.  All state is stored in
    Kafka Connect (not in MetaMind's database), so restarts are safe.
    """

    def __init__(self, kafka_connect_url: str = "http://kafka-connect:8083") -> None:
        self.base_url = kafka_connect_url.rstrip("/")

    def build_connector_config(self, cfg: DebeziumConnectorConfig) -> Dict[str, Any]:
        """Render the Kafka Connect JSON config for a Debezium Oracle connector."""
        table_include = ",".join(cfg.tables)
        return {
            "name": cfg.connector_name,
            "config": {
                "connector.class": "io.debezium.connector.oracle.OracleConnector",
                "tasks.max": "1",

                # Oracle connection
                "database.hostname": cfg.oracle_host,
                "database.port": str(cfg.oracle_port),
                "database.user": cfg.oracle_user,
                "database.password": cfg.oracle_password,
                "database.dbname": cfg.oracle_service,
                "database.pdb.name": cfg.oracle_service,

                # LogMiner
                "database.history.kafka.bootstrap.servers": cfg.kafka_bootstrap,
                "database.history.kafka.topic": f"{cfg.topic_prefix}.schema-changes",
                "log.mining.strategy": cfg.log_mining_strategy,
                "log.mining.batch.size.max": str(cfg.batch_size),

                # Output
                "topic.prefix": cfg.topic_prefix,
                "table.include.list": table_include,

                # Behaviour
                "snapshot.mode": cfg.snapshot_mode,
                "poll.interval.ms": str(cfg.poll_interval_ms),

                # Serialisation  (JSON for readability; use Avro + Schema Registry in prod)
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",

                # Transforms: unwrap Debezium envelope → flat record
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": "false",
                "transforms.unwrap.add.fields": "op,source.ts_ms,source.scn",
                "transforms.unwrap.delete.handling.mode": "rewrite",
            },
        }

    def create_or_update_connector(self, cfg: DebeziumConnectorConfig) -> bool:
        """
        PUT the connector config to Kafka Connect (idempotent upsert).

        Returns True on success.  In production this should use aiohttp
        for async operation; requests is used here for simplicity.
        """
        try:
            import requests  # type: ignore

            connector_config = self.build_connector_config(cfg)
            url = f"{self.base_url}/connectors/{cfg.connector_name}/config"
            resp = requests.put(
                url,
                json=connector_config["config"],
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            resp.raise_for_status()
            logger.info("Debezium connector '%s' created/updated", cfg.connector_name)
            return True
        except Exception as exc:
            logger.error("Failed to create Debezium connector: %s", exc)
            return False

    def get_connector_status(self, connector_name: str) -> Dict[str, Any]:
        """Return the connector + task status from Kafka Connect."""
        try:
            import requests  # type: ignore

            resp = requests.get(
                f"{self.base_url}/connectors/{connector_name}/status",
                timeout=5,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.warning("Could not fetch connector status: %s", exc)
            return {"connector": {"state": "UNKNOWN"}}

    def restart_failed_tasks(self, connector_name: str) -> None:
        """Restart any FAILED tasks for *connector_name*."""
        try:
            import requests  # type: ignore

            status = self.get_connector_status(connector_name)
            for task in status.get("tasks", []):
                if task.get("state") == "FAILED":
                    task_id = task["id"]
                    requests.post(
                        f"{self.base_url}/connectors/{connector_name}/tasks/{task_id}/restart",
                        timeout=5,
                    )
                    logger.warning("Restarted FAILED task %d for connector '%s'", task_id, connector_name)
        except Exception as exc:
            logger.error("Failed to restart connector tasks: %s", exc)
