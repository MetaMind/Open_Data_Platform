"""MetaMind logical algebra nodes — relational algebra AST."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class JoinType(str, Enum):
    """Join type enumeration."""

    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"
    SEMI = "semi"
    ANTI = "anti"


class SortDirection(str, Enum):
    """Sort direction."""

    ASC = "asc"
    DESC = "desc"


class AggFunc(str, Enum):
    """Aggregate function types."""

    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT_DISTINCT = "count_distinct"
    STDDEV = "stddev"
    VARIANCE = "variance"


@dataclass
class Predicate:
    """A single filter predicate on a column."""

    column: str
    operator: str        # =, !=, <, >, <=, >=, IN, LIKE, IS NULL, BETWEEN
    value: object        # literal value or list for IN
    table_alias: Optional[str] = None
    is_correlated: bool = False   # subquery reference

    @property
    def qualified_name(self) -> str:
        """Return fully qualified column reference."""
        if self.table_alias:
            return f"{self.table_alias}.{self.column}"
        return self.column


@dataclass
class SortKey:
    """A sort key specification."""

    column: str
    direction: SortDirection = SortDirection.ASC
    nulls_first: bool = False


@dataclass
class AggregateExpr:
    """An aggregate expression in GROUP BY / SELECT."""

    func: AggFunc
    column: Optional[str]      # None for COUNT(*)
    alias: str
    is_distinct: bool = False


# ── Base Node ─────────────────────────────────────────────────

class LogicalNode(ABC):
    """Base class for all logical plan nodes."""

    def __init__(self) -> None:
        """Initialize with empty children and metadata."""
        self.children: list[LogicalNode] = []
        self._estimated_rows: Optional[float] = None
        self._estimated_cost: Optional[float] = None
        self.node_id: str = ""

    @property
    def estimated_rows(self) -> float:
        """Estimated output row count."""
        if self._estimated_rows is None:
            return 1000.0
        return self._estimated_rows

    @estimated_rows.setter
    def estimated_rows(self, v: float) -> None:
        """Set estimated row count."""
        self._estimated_rows = max(1.0, v)

    @abstractmethod
    def accept(self, visitor: "NodeVisitor") -> object:
        """Accept a visitor."""
        ...

    @abstractmethod
    def __repr__(self) -> str:
        """Human-readable representation."""
        ...


# ── Leaf Nodes ────────────────────────────────────────────────

@dataclass
class ScanNode(LogicalNode):
    """Full or partial table scan."""

    table_name: str
    schema_name: str = "public"
    alias: Optional[str] = None
    columns: list[str] = field(default_factory=list)   # empty = all columns
    predicates: list[Predicate] = field(default_factory=list)
    backend: Optional[str] = None

    def __post_init__(self) -> None:
        """Initialize base node."""
        super().__init__()

    def accept(self, visitor: "NodeVisitor") -> object:
        """Accept visitor."""
        return visitor.visit_scan(self)

    def __repr__(self) -> str:
        """Repr."""
        alias = f" AS {self.alias}" if self.alias else ""
        return f"Scan({self.schema_name}.{self.table_name}{alias})"


@dataclass
class VectorSearchNode(LogicalNode):
    """Vector similarity search node (F19)."""

    table_name: str
    vector_column: str
    query_vector: list[float]
    top_k: int = 10
    index_type: str = "ivfflat"
    distance_metric: str = "cosine"   # cosine, l2, inner_product
    backend: Optional[str] = None
    alias: Optional[str] = None
    predicates: list[Predicate] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Initialize base node."""
        super().__init__()

    def accept(self, visitor: "NodeVisitor") -> object:
        """Accept visitor."""
        return visitor.visit_vector_search(self)

    def __repr__(self) -> str:
        """Repr."""
        return f"VectorSearch({self.table_name}.{self.vector_column}, k={self.top_k})"


# ── Unary Nodes ───────────────────────────────────────────────

@dataclass
class FilterNode(LogicalNode):
    """Filter / WHERE clause node."""

    predicates: list[Predicate] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Initialize base node."""
        super().__init__()

    def accept(self, visitor: "NodeVisitor") -> object:
        """Accept visitor."""
        return visitor.visit_filter(self)

    def __repr__(self) -> str:
        """Repr."""
        return f"Filter({len(self.predicates)} predicates)"


@dataclass
class ProjectNode(LogicalNode):
    """Projection / SELECT columns node."""

    columns: list[str]
    aliases: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize base node."""
        super().__init__()

    def accept(self, visitor: "NodeVisitor") -> object:
        """Accept visitor."""
        return visitor.visit_project(self)

    def __repr__(self) -> str:
        """Repr."""
        return f"Project({', '.join(self.columns[:5])}{'...' if len(self.columns) > 5 else ''})"


@dataclass
class AggregateNode(LogicalNode):
    """Group-by aggregation node."""

    group_by: list[str]
    aggregates: list[AggregateExpr]
    having: list[Predicate] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Initialize base node."""
        super().__init__()

    def accept(self, visitor: "NodeVisitor") -> object:
        """Accept visitor."""
        return visitor.visit_aggregate(self)

    def __repr__(self) -> str:
        """Repr."""
        return f"Aggregate(group_by={self.group_by}, aggs={[a.alias for a in self.aggregates]})"


@dataclass
class SortNode(LogicalNode):
    """ORDER BY node."""

    sort_keys: list[SortKey]

    def __post_init__(self) -> None:
        """Initialize base node."""
        super().__init__()

    def accept(self, visitor: "NodeVisitor") -> object:
        """Accept visitor."""
        return visitor.visit_sort(self)

    def __repr__(self) -> str:
        """Repr."""
        return f"Sort({[f'{k.column} {k.direction.value}' for k in self.sort_keys]})"


@dataclass
class LimitNode(LogicalNode):
    """LIMIT / OFFSET node."""

    limit: int
    offset: int = 0

    def __post_init__(self) -> None:
        """Initialize base node."""
        super().__init__()

    def accept(self, visitor: "NodeVisitor") -> object:
        """Accept visitor."""
        return visitor.visit_limit(self)

    def __repr__(self) -> str:
        """Repr."""
        return f"Limit({self.limit}, offset={self.offset})"


# ── Binary Nodes ──────────────────────────────────────────────

@dataclass
class JoinNode(LogicalNode):
    """Binary join node."""

    join_type: JoinType = JoinType.INNER
    left_key: Optional[str] = None
    right_key: Optional[str] = None
    conditions: list[Predicate] = field(default_factory=list)
    is_cross: bool = False

    def __post_init__(self) -> None:
        """Initialize base node."""
        super().__init__()

    def accept(self, visitor: "NodeVisitor") -> object:
        """Accept visitor."""
        return visitor.visit_join(self)

    def __repr__(self) -> str:
        """Repr."""
        return f"Join({self.join_type.value}, {self.left_key}={self.right_key})"


@dataclass
class SemiJoinNode(LogicalNode):
    """Semi-join node — returns rows from left where match exists in right."""

    left_key: str
    right_key: str
    is_anti: bool = False         # True for NOT EXISTS / NOT IN semantics

    def __post_init__(self) -> None:
        """Initialize base node."""
        super().__init__()

    def accept(self, visitor: "NodeVisitor") -> object:
        """Accept visitor."""
        return visitor.visit_semijoin(self)

    def __repr__(self) -> str:
        """Repr."""
        kind = "Anti" if self.is_anti else "Semi"
        return f"{kind}Join({self.left_key}={self.right_key})"


# ── Visitor Protocol ──────────────────────────────────────────

class NodeVisitor(ABC):
    """Visitor protocol for traversing the logical plan tree."""

    @abstractmethod
    def visit_scan(self, node: ScanNode) -> object:
        """Visit scan node."""
        ...

    @abstractmethod
    def visit_filter(self, node: FilterNode) -> object:
        """Visit filter node."""
        ...

    @abstractmethod
    def visit_project(self, node: ProjectNode) -> object:
        """Visit project node."""
        ...

    @abstractmethod
    def visit_join(self, node: JoinNode) -> object:
        """Visit join node."""
        ...

    @abstractmethod
    def visit_aggregate(self, node: AggregateNode) -> object:
        """Visit aggregate node."""
        ...

    @abstractmethod
    def visit_sort(self, node: SortNode) -> object:
        """Visit sort node."""
        ...

    @abstractmethod
    def visit_limit(self, node: LimitNode) -> object:
        """Visit limit node."""
        ...

    def visit_semijoin(self, node: SemiJoinNode) -> object:
        """Visit semi-join node."""
        return None

    def visit_vector_search(self, node: VectorSearchNode) -> object:
        """Visit vector search node."""
        return None
