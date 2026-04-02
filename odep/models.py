"""ODEP data models with Pydantic v2 validation."""

from __future__ import annotations

import re
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

_URN_PATTERN = re.compile(r"^urn:li:dataset:\([a-z]+,[^,]+,(prod|staging|dev)\)$")
_CRON_FIELD_PATTERN = re.compile(r"^[\d\*\/\-\,]+$")


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"


class EngineType(str, Enum):
    SPARK = "SPARK"
    FLINK = "FLINK"
    DBT = "DBT"
    PYTHON = "PYTHON"
    SQL = "SQL"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class DatasetMetadata(BaseModel):
    """Metadata for a registered dataset in the catalog."""

    model_config = ConfigDict(populate_by_name=True)

    urn: str
    name: str
    platform: str
    env: str
    schema_fields: List[Dict[str, Any]] = Field(alias="schema")
    owner: str
    description: Optional[str] = None
    tags: List[str] = []
    custom_properties: Dict[str, str] = {}
    last_modified: Optional[datetime] = None

    @field_validator("urn")
    @classmethod
    def validate_urn(cls, v: str) -> str:
        if not _URN_PATTERN.match(v):
            raise ValueError(
                f"urn must match pattern urn:li:dataset:(platform,name,(prod|staging|dev)), got: {v!r}"
            )
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("name must be a non-empty string")
        return v

    @field_validator("owner")
    @classmethod
    def validate_owner(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("owner must be a non-empty string")
        return v

    @field_validator("schema_fields")
    @classmethod
    def validate_schema_fields(cls, v: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not v:
            raise ValueError("schema must be a non-empty list")
        return v


class LineageEdge(BaseModel):
    """A directed edge in the dataset lineage graph."""

    source_urn: str
    target_urn: str
    transformation: Optional[str] = None
    timestamp: Optional[datetime] = None

    @model_validator(mode="after")
    def validate_no_self_loop(self) -> "LineageEdge":
        if self.source_urn == self.target_urn:
            raise ValueError(
                f"source_urn and target_urn must differ; both are {self.source_urn!r}"
            )
        return self


class JobDefinition(BaseModel):
    """Definition of a deployable pipeline job."""

    job_id: str
    name: str
    schedule: Optional[str] = None
    task_config: Dict[str, Any] = {}
    dependencies: List[str] = []
    retries: int = 3
    timeout_minutes: int = 60
    env_vars: Dict[str, str] = {}
    resource_limits: Dict[str, str] = {}

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        parts = v.strip().split()
        if len(parts) not in (5, 6):
            raise ValueError(
                f"schedule must be a valid cron expression with 5 or 6 fields, got {len(parts)} fields: {v!r}"
            )
        for part in parts:
            if not _CRON_FIELD_PATTERN.match(part):
                raise ValueError(
                    f"Invalid cron field {part!r} in schedule {v!r}; "
                    "each field must match [\\d\\*\\/\\-\\,]+"
                )
        return v

    @field_validator("timeout_minutes")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"timeout_minutes must be > 0, got {v}")
        return v

    @field_validator("retries")
    @classmethod
    def validate_retries(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"retries must be >= 0, got {v}")
        return v


class JobRun(BaseModel):
    """A single execution run of a JobDefinition."""

    run_id: str
    job_id: str
    status: JobStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    logs_url: Optional[str] = None
    metrics: Dict[str, float] = {}


class JobConfig(BaseModel):
    """Configuration for a single execution unit submitted to an ExecutionEngine."""

    engine: EngineType
    code: str
    dependencies: List[str] = []
    cluster_config: Dict[str, Any] = {}
    io_config: Dict[str, Any] = {}


class JobResult(BaseModel):
    """Result returned by ExecutionEngine.wait_for_completion()."""

    success: bool
    records_processed: int = 0
    execution_time_ms: int = 0
    output_location: Optional[str] = None
    error_message: Optional[str] = None
    metrics: Dict[str, Any] = {}


__all__ = [
    "JobStatus",
    "EngineType",
    "DatasetMetadata",
    "LineageEdge",
    "JobDefinition",
    "JobRun",
    "JobConfig",
    "JobResult",
]
