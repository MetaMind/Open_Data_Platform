"""
FastAPI Server - MetaMind API

File: metamind/api/server.py
Role: API Engineer
Phase: 1
Dependencies: FastAPI, uvicorn

Main FastAPI application with query endpoints.
"""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from metamind.bootstrap import AppContext, bootstrap
from metamind.config.settings import get_settings
from metamind.api.admin_routes import admin_router
from metamind.api.security_middleware import (
    QueryCancellationTracker,
    SecurityHeadersMiddleware,
)
from metamind.api.query_logger import log_query
from metamind.api.middleware import RateLimitMiddleware  # W-05: register rate limiter
from metamind.api.audit_routes import audit_router  # Task 05
from metamind.api.graphql_gateway import build_graphql_router  # Task 07
from metamind.api.ab_routes import ab_router  # Task 10
from metamind.api.admin_routes_ext import admin_ext_router  # Tasks 12, 14, 18
from metamind.api.onboarding_routes import onboarding_router  # Task 15
from metamind.api.billing_routes import billing_router  # Task 16
from metamind.api.trace_routes import trace_router  # Task 19
from metamind.observability.metrics import get_metrics

logger = logging.getLogger(__name__)


# Pydantic models for API
class QueryRequest(BaseModel):
    """Query execution request."""
    sql: str = Field(..., description="SQL query to execute", min_length=1)
    tenant_id: str = Field(default="default", description="Tenant identifier")
    user_id: Optional[str] = Field(default=None, description="User identifier")
    freshness_tolerance_seconds: Optional[int] = Field(
        default=None,
        description="Maximum acceptable staleness in seconds"
    )
    use_cache: bool = Field(default=True, description="Whether to use cache")
    
    class Config:
        json_schema_extra = {
            "example": {
                "sql": "SELECT COUNT(*) FROM orders WHERE created_at > '2024-01-01'",
                "tenant_id": "default",
                "freshness_tolerance_seconds": 300
            }
        }


class QueryResponse(BaseModel):
    """Query execution response."""
    query_id: str
    status: str
    routed_to: str
    execution_strategy: str
    freshness_seconds: int
    estimated_cost_ms: float
    confidence: float
    cache_hit: bool
    execution_time_ms: Optional[int] = None
    row_count: Optional[int] = None
    columns: Optional[list] = None
    data: Optional[list] = None
    rewritten_sql: Optional[str] = None
    reason: str


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    version: str
    checks: Dict[str, Any]


class CDCStatusResponse(BaseModel):
    """CDC status response."""
    total_tables: int
    healthy: int
    warning: int
    critical: int
    max_lag_seconds: int
    overall_status: str
    lagging_tables: list


class ErrorResponse(BaseModel):
    """Error response model."""
    error: str
    detail: Optional[str] = None
    query_id: Optional[str] = None


# Global context for lifespan management
_app_context: Optional[AppContext] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global _app_context
    
    # Startup
    logger.info("Starting MetaMind API...")
    try:
        _app_context = await bootstrap()
        app.state.context = _app_context
        # Start AI synthesis background loop (60-minute cycle)
        try:
            await _app_context.synthesis_engine.start_background_synthesis(interval_minutes=60)
            logger.info("SynthesisEngine background synthesis started")
        except Exception as exc:
            logger.error("Failed to start SynthesisEngine background task: %s", exc)
        logger.info("MetaMind API started successfully")
    except Exception as e:
        logger.error("Failed to start MetaMind API: %s", e)
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down MetaMind API...")
    if _app_context:
        # Graceful synthesis engine shutdown
        try:
            await _app_context.synthesis_engine.stop()
            logger.info("SynthesisEngine stopped cleanly")
        except Exception as exc:
            logger.error("SynthesisEngine stop error: %s", exc)
        await _app_context.close()
    app.state.context = None
    logger.info("MetaMind API shut down successfully")


def create_app() -> FastAPI:
    """Create FastAPI application."""
    settings = get_settings()
    
    app = FastAPI(
        title="MetaMind Enterprise Query Intelligence Platform",
        description="AI-driven query routing and optimization",
        version="4.0.0",
        lifespan=lifespan,
        docs_url="/docs" if settings.is_development else None,
        redoc_url="/redoc" if settings.is_development else None,
    )
    
    # Register admin routes
    app.include_router(admin_router, prefix="/api/v1")
    app.include_router(audit_router, prefix="/api/v1")  # Task 05

    # Task 07: GraphQL gateway (mounted at /graphql when strawberry is available)
    _gql_router = build_graphql_router(None)  # context injected after bootstrap
    if _gql_router is not None:
        app.include_router(_gql_router, prefix="/graphql")
        logger.info("GraphQL endpoint mounted at /graphql")
    app.include_router(ab_router, prefix="/api/v1")  # Task 10
    app.include_router(admin_ext_router, prefix="/api/v1")  # Tasks 12, 14, 18
    app.include_router(onboarding_router, prefix="/api/v1")  # Task 15
    app.include_router(billing_router, prefix="/api/v1")  # Task 16
    app.include_router(trace_router)  # Task 19 — mounts /traces UI and /api/v1/traces

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.is_development else ["https://metamind.io"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["*"],
    )

    # Security headers middleware
    app.add_middleware(
        SecurityHeadersMiddleware,
        environment="development" if settings.is_development else "production",
    )

    # W-05: Rate limit middleware — was built but never registered
    app.add_middleware(
        RateLimitMiddleware,
        max_rps=10,  # 600 requests/minute
        burst=50,
    )
    
    # Request timing middleware
    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response
    
    # Exception handlers
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content=ErrorResponse(
                error=exc.detail if isinstance(exc.detail, str) else "Error",
                detail=str(exc.detail) if not isinstance(exc.detail, str) else None
            ).dict()
        )
    
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error(f"Unhandled exception: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error="Internal server error",
                detail=str(exc) if settings.is_development else None
            ).dict()
        )
    
    # Health check endpoint
    @app.get("/api/v1/health", response_model=HealthResponse, tags=["Health"])
    async def health_check():
        """Health check endpoint."""
        if _app_context is None:
            raise HTTPException(status_code=503, detail="Application not initialized")
        
        health = await _app_context.health_check()
        return HealthResponse(
            status=health["status"],
            version=health["version"],
            checks=health["checks"]
        )
    
    # Query execution endpoint
    @app.post("/api/v1/query", response_model=QueryResponse, tags=["Query"])
    async def execute_query(request: QueryRequest):
        """Execute a SQL query through the unified pipeline (W-06).

        Routes via QueryRouter → QueryEngine (Cascades optimizer + firewall +
        RLS + online learner) in a single call.  The old two-step
        router-then-execute pattern has been replaced by UnifiedQueryPipeline.
        """
        if _app_context is None:
            raise HTTPException(status_code=503, detail="Application not initialized")

        start_time = time.time()
        query_id = str(uuid.uuid4())

        try:
            # W-06: Single unified pipeline call — replaces separate router + execute
            pipeline_result = await _app_context.unified_pipeline.execute(
                sql=request.sql,
                tenant_id=request.tenant_id,
                user_id=request.user_id,
                freshness_tolerance_seconds=request.freshness_tolerance_seconds,
                use_cache=request.use_cache,
            )

            execution_time_ms = int((time.time() - start_time) * 1000)
            result = pipeline_result.result
            row_count = getattr(result, "row_count", 0)
            columns = getattr(result, "columns", [])
            raw_data = getattr(result, "rows", None) or []

            await log_query(
                app_context=_app_context,
                query_id=pipeline_result.query_id,
                tenant_id=request.tenant_id,
                user_id=request.user_id or "anonymous",
                sql=request.sql,
                decision=None,
                execution_time_ms=execution_time_ms,
                row_count=row_count,
                status="success",
            )

            return QueryResponse(
                query_id=pipeline_result.query_id,
                status="success",
                routed_to=pipeline_result.backend_used,
                execution_strategy=pipeline_result.workload_type,
                freshness_seconds=0,
                estimated_cost_ms=pipeline_result.plan_cost,
                confidence=1.0,
                cache_hit=pipeline_result.cache_hit,
                execution_time_ms=execution_time_ms,
                row_count=row_count,
                columns=columns,
                data=raw_data[:100] if raw_data else None,
                rewritten_sql=None,
                reason=f"flags={pipeline_result.flags_used}",
            )
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}", exc_info=True)
            
            # Log failed query
            await log_query(
                app_context=_app_context,
                query_id=query_id,
                tenant_id=request.tenant_id,
                user_id=request.user_id or "anonymous",
                sql=request.sql,
                decision=None,
                execution_time_ms=int((time.time() - start_time) * 1000),
                row_count=0,
                status="failed",
                error_message=str(e),
            )
            
            raise HTTPException(
                status_code=400,
                detail=f"Query execution failed: {str(e)}"
            )
    
    # CDC status endpoint
    @app.get("/api/v1/cdc/status", response_model=CDCStatusResponse, tags=["CDC"])
    async def get_cdc_status(tenant_id: str = "default"):
        """Get CDC replication status."""
        if _app_context is None:
            raise HTTPException(status_code=503, detail="Application not initialized")
        
        summary = _app_context.cdc_monitor.get_health_summary(tenant_id)
        
        return CDCStatusResponse(
            total_tables=summary["total_tables"],
            healthy=summary["healthy"],
            warning=summary["warning"],
            critical=summary["critical"],
            max_lag_seconds=summary["max_lag_seconds"],
            overall_status=summary["overall_status"],
            lagging_tables=summary["lagging_tables"]
        )
    
    # Cache stats endpoint
    @app.get("/api/v1/cache/stats", tags=["Cache"])
    async def get_cache_stats():
        """Get cache statistics."""
        if _app_context is None:
            raise HTTPException(status_code=503, detail="Application not initialized")
        
        return _app_context.cache_manager.get_stats()

    @app.get("/metrics", include_in_schema=False, tags=["Observability"])
    async def metrics_endpoint():
        """Prometheus metrics endpoint."""
        metrics = get_metrics()
        return Response(
            content=metrics.get_metrics(),
            media_type=metrics.get_content_type(),
        )
    
    # Cache invalidate endpoint
    @app.post("/api/v1/cache/invalidate", tags=["Cache"])
    async def invalidate_cache(pattern: str = ""):
        """Invalidate cache entries matching pattern."""
        if _app_context is None:
            raise HTTPException(status_code=503, detail="Application not initialized")
        
        if pattern:
            count = await _app_context.cache_manager.invalidate_pattern(pattern)
        else:
            await _app_context.cache_manager.clear()
            count = -1  # All cleared
        
        return {"status": "success", "invalidated_count": count}
    
    # Register metadata and query-history routes from auxiliary module
    from metamind.api.metadata_routes import register_metadata_routes
    register_metadata_routes(app, lambda: _app_context)

    return app




# Create the application instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    
    settings = get_settings()
    uvicorn.run(
        "metamind.api.server:app",
        host=settings.host,
        port=settings.port,
        reload=settings.is_development,
        workers=1 if settings.is_development else settings.workers
    )
