"""ODEP platform configuration via Pydantic v2 settings."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal, Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class MetadataConfig(BaseSettings):
    """Configuration for the metadata engine."""

    model_config = SettingsConfigDict(env_prefix="ODEP_METADATA_")

    engine: str = "openmeta"
    datahub_url: str = "http://localhost:8080"
    marquez_url: str = "http://localhost:5000"
    opa_url: str = "http://localhost:8181"

    @field_validator("engine")
    @classmethod
    def validate_engine(cls, v: str) -> str:
        valid = {"openmeta", "metamind"}
        if v not in valid:
            raise ValueError(
                f"metadata.engine must be one of {sorted(valid)}, got {v!r}"
            )
        return v


class OrchestrationConfig(BaseSettings):
    """Configuration for the orchestration engine."""

    model_config = SettingsConfigDict(env_prefix="ODEP_ORCHESTRATION_")

    engine: str = "airflow"
    # Airflow
    airflow_url: str = "http://localhost:8090"
    airflow_username: str = "admin"
    airflow_password: str = "admin"
    # Dagster
    dagster_url: str = "http://localhost:3000"
    # Prefect
    prefect_url: str = "http://localhost:4200"
    # Temporal
    temporal_host: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "odep-task-queue"

    @field_validator("engine")
    @classmethod
    def validate_engine(cls, v: str) -> str:
        valid = {"airflow", "dagster", "prefect", "temporal"}
        if v not in valid:
            raise ValueError(
                f"orchestration.engine must be one of {sorted(valid)}, got {v!r}"
            )
        return v


class ExecutionConfig(BaseSettings):
    """Configuration for the execution engine."""

    model_config = SettingsConfigDict(env_prefix="ODEP_EXECUTION_")

    default_engine: str = "duckdb"
    # Spark
    spark_master: str = "local[*]"
    spark_rest_url: str = "http://localhost:8081"
    # Trino
    trino_host: str = "localhost"
    trino_port: int = 8082
    trino_user: str = "odep"
    trino_catalog: str = "tpch"
    trino_schema: str = "tiny"
    # Flink
    flink_jobmanager_url: str = "http://localhost:8083"
    flink_sql_gateway_url: str = "http://localhost:8084"
    # dbt
    dbt_project_dir: str = "."
    dbt_profiles_dir: str = "~/.dbt"
    dbt_target: str = "dev"
    # ClickHouse
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "default"

    @field_validator("default_engine")
    @classmethod
    def validate_default_engine(cls, v: str) -> str:
        valid = {"spark", "flink", "dbt", "duckdb", "trino", "clickhouse"}
        if v not in valid:
            raise ValueError(
                f"execution.default_engine must be one of {sorted(valid)}, got {v!r}"
            )
        return v


class MetaMindConfig(BaseSettings):
    """Configuration for the MetaMind execution engine."""

    model_config = SettingsConfigDict(env_prefix="ODEP_METAMIND_")

    metamind_url: str = "http://localhost:8000"
    tenant_id: str = "default"
    api_token: Optional[str] = None
    timeout: float = 120.0


class OdepConfig(BaseSettings):
    """Top-level ODEP platform configuration."""

    model_config = SettingsConfigDict(
        env_file=".odep.env",
        env_nested_delimiter="__",
    )

    project_name: str = "my-data-platform"
    environment: Literal["local", "dev", "staging", "prod"] = "local"
    metadata: MetadataConfig = MetadataConfig()
    orchestration: OrchestrationConfig = OrchestrationConfig()
    execution: ExecutionConfig = ExecutionConfig()
    metamind: MetaMindConfig = MetaMindConfig()


@lru_cache()
def get_config() -> OdepConfig:
    """Return the singleton OdepConfig, parsed once per process."""
    return OdepConfig()


__all__ = [
    "MetadataConfig",
    "OrchestrationConfig",
    "ExecutionConfig",
    "MetaMindConfig",
    "OdepConfig",
    "get_config",
]
