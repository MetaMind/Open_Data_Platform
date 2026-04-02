"""MetaMind API Pydantic models — request and response schemas."""
from __future__ import annotations

from typing import Any, Optional
from pydantic import BaseModel, Field


class ExecuteRequest(BaseModel):
    """Request body for POST /v1/query."""

    sql: str = Field(..., description="SQL query to execute", min_length=1)
    backend: Optional[str] = Field(None, description="Force specific backend (e.g. 'postgres')")
    timeout_seconds: Optional[int] = Field(None, ge=1, le=3600)
    dry_run: bool = Field(False, description="Parse and optimize without executing")
    explain: bool = Field(False, description="Return execution plan instead of results")


class ExecuteResponse(BaseModel):
    """Response body for POST /v1/query."""

    query_id: str
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    duration_ms: float
    optimization_ms: float
    cache_hit: bool
    workload_type: str
    backend_used: str
    optimization_tier: int
    plan_cost: float
    flags_used: list[str] = Field(default_factory=list)


class NLQueryRequest(BaseModel):
    """Request body for POST /v1/nl/query (F28)."""

    nl_text: str = Field(..., description="Natural language question", min_length=3)
    table_hints: Optional[list[str]] = Field(
        None, description="List of relevant table names for schema context"
    )
    execute: bool = Field(True, description="Execute generated SQL after conversion")


class NLQueryResponse(BaseModel):
    """Response for NL query conversion."""

    nl_text: str
    generated_sql: str
    confidence: float
    execution_result: Optional[dict[str, Any]] = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    services: dict[str, str]
    version: str


class TableListResponse(BaseModel):
    """List of registered tables."""

    tables: list[str]
    count: int


class RegisterTableRequest(BaseModel):
    """Request to register a table in the catalog."""

    schema_name: str = Field(default="public")
    table_name: str = Field(..., min_length=1)
    backend: str = Field(default="postgres")
    row_count: int = Field(default=0, ge=0)
    size_bytes: int = Field(default=0, ge=0)


class CostEstimateRequest(BaseModel):
    """Request for F23 cloud cost pre-estimation."""

    sql: str
    backend: Optional[str] = None


class CostEstimateResponse(BaseModel):
    """F23 cloud cost estimate response."""

    estimated_cost_usd: float
    bytes_scanned_estimate: int
    backend: str
    budget_remaining_usd: Optional[float] = None
    budget_utilization_pct: Optional[float] = None
    would_exceed_budget: bool = False
