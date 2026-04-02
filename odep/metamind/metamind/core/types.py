"""Core type definitions for the MetaMind platform."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


class DistanceMetric(enum.Enum):
    COSINE = "cosine"
    L2 = "l2"
    INNER_PRODUCT = "inner_product"


class IndexType(enum.Enum):
    BTREE = "btree"
    HASH = "hash"
    IVFFLAT = "IVFFlat"
    HNSW = "HNSW"


class JoinType(enum.Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


@dataclass
class Predicate:
    column: str
    operator: str  # "=", "!=", "<", ">", "<=", ">=", "IN", "LIKE", "IS NULL"
    value: Any
    table: Optional[str] = None


@dataclass
class ColumnMeta:
    name: str
    dtype: str
    nullable: bool = True
    is_primary_key: bool = False
    is_indexed: bool = False
    distinct_count: Optional[int] = None
    null_fraction: Optional[float] = None
    avg_width: Optional[int] = None


@dataclass
class TableMeta:
    table_name: str
    schema_name: str
    tenant_id: str
    columns: list[ColumnMeta] = field(default_factory=list)
    row_count: int = 0
    size_bytes: int = 0
    backend: str = "postgres"


@dataclass
class IndexMeta:
    index_name: str
    table_name: str
    columns: list[str]
    index_type: str = "btree"
    is_unique: bool = False
    size_bytes: int = 0


@dataclass
class NLQueryResult:
    sql: str
    confidence: float
    tables_used: list[str]
    explanation: str
    was_validated: bool
    validation_error: Optional[str] = None


@dataclass
class CostVector:
    cpu: float = 0.0
    io: float = 0.0
    network: float = 0.0
    memory: float = 0.0

    @property
    def total(self) -> float:
        return self.cpu + self.io + self.network + self.memory


@dataclass
class CostWeights:
    cpu_weight: float = 1.0
    io_weight: float = 1.0
    network_weight: float = 1.0
    memory_weight: float = 0.5


@dataclass
class FeatureFlag:
    name: str
    enabled: bool = False
    description: str = ""


# Logical plan node types
@dataclass
class LogicalNode:
    node_type: str
    children: list[LogicalNode] = field(default_factory=list)
    properties: dict[str, Any] = field(default_factory=dict)
    estimated_rows: float = 0.0
    estimated_cost: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_type": self.node_type,
            "properties": self.properties,
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
            "children": [c.to_dict() for c in self.children],
        }
