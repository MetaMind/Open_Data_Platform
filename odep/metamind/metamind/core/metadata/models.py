"""MetaMind core metadata domain models."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class BackendType(str, Enum):
    """Supported execution backends."""

    POSTGRES = "postgres"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"
    SPARK = "spark"
    DUCKDB = "duckdb"
    FLINK = "flink"
    PGVECTOR = "pgvector"
    LANCE = "lance"


class DataType(str, Enum):
    """Canonical data types used in metadata."""

    INT = "int"
    BIGINT = "bigint"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    VARCHAR = "varchar"
    TEXT = "text"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    TIMESTAMPTZ = "timestamptz"
    BYTEA = "bytea"
    JSON = "json"
    JSONB = "jsonb"
    ARRAY = "array"
    VECTOR = "vector"
    UNKNOWN = "unknown"


@dataclass
class ColumnMeta:
    """Column-level metadata with statistics."""

    column_name: str
    data_type: DataType
    ordinal_pos: int
    nullable: bool = True
    ndv: int = 0                         # Number of distinct values
    null_fraction: float = 0.0
    avg_width: int = 4
    min_value: Optional[str] = None
    max_value: Optional[str] = None
    most_common_vals: list[str] = field(default_factory=list)
    most_common_freqs: list[float] = field(default_factory=list)
    histogram_bounds: list[str] = field(default_factory=list)
    physical_correlation: Optional[float] = None
    updated_at: Optional[datetime] = None

    @property
    def selectivity_point(self) -> float:
        """Estimated selectivity for an equality predicate."""
        if self.ndv <= 0:
            return 1.0
        return 1.0 / self.ndv

    @property
    def selectivity_range(self) -> float:
        """Estimated selectivity for a range predicate (assume 1/3 of range)."""
        return 0.33


@dataclass
class PartitionMeta:
    """Partition metadata for partition pruning."""

    partition_name: str
    partition_type: str            # range, list, hash
    partition_key: str
    lower_bound: Optional[str] = None
    upper_bound: Optional[str] = None
    list_values: list[str] = field(default_factory=list)
    hash_modulus: Optional[int] = None
    hash_remainder: Optional[int] = None
    row_count: int = 0
    size_bytes: int = 0
    is_prunable: bool = True


@dataclass
class TableMeta:
    """Full table metadata including columns and statistics."""

    table_id: int
    tenant_id: str
    schema_name: str
    table_name: str
    backend: BackendType
    row_count: int = 0
    size_bytes: int = 0
    columns: list[ColumnMeta] = field(default_factory=list)
    partitions: list[PartitionMeta] = field(default_factory=list)
    last_analyzed: Optional[datetime] = None
    properties: dict[str, object] = field(default_factory=dict)
    version: int = 1

    @property
    def full_name(self) -> str:
        """Fully qualified table name."""
        return f"{self.schema_name}.{self.table_name}"

    def get_column(self, name: str) -> Optional[ColumnMeta]:
        """Look up a column by name."""
        for col in self.columns:
            if col.column_name.lower() == name.lower():
                return col
        return None


@dataclass
class MaterializedViewMeta:
    """Materialized view metadata for MV-based rewriting."""

    mv_id: int
    tenant_id: str
    mv_name: str
    schema_name: str
    backend: BackendType
    query_text: str
    query_fingerprint: str
    base_tables: list[str] = field(default_factory=list)
    row_count: int = 0
    size_bytes: int = 0
    refresh_type: str = "manual"
    last_refreshed: Optional[datetime] = None
    refresh_lag_seconds: int = 0
    cost_estimate: float = 0.0
    benefit_score: float = 0.0
    is_active: bool = True


@dataclass
class IndexMeta:
    """Index metadata for access path selection."""

    index_name: str
    table_name: str
    index_type: str          # btree, hash, gin, gist, brin, ivfflat
    columns: list[str] = field(default_factory=list)
    is_unique: bool = False
    is_partial: bool = False
    partial_predicate: Optional[str] = None
    size_bytes: int = 0
    estimated_scan_cost: float = 1.0


@dataclass
class QueryTrace:
    """Execution trace for adaptive feedback."""

    query_id: str
    tenant_id: str
    sql: str
    backend: str
    plan_hash: str
    estimated_rows: int
    actual_rows: int
    estimated_cost: float
    actual_duration_ms: float
    executed_at: datetime
    error: Optional[str] = None
