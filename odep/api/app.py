"""ODEP FastAPI application — API Gateway."""
from fastapi import FastAPI, Response

from odep.api.audit import AuditLogMiddleware
from odep.api.middleware import RateLimitMiddleware
from odep.api.observability import generate_latest, CONTENT_TYPE_LATEST
from odep.api.routes.execution import router as execution_router
from odep.api.routes.metadata import router as metadata_router
from odep.api.routes.orchestration import router as orchestration_router

app = FastAPI(title="ODEP API Gateway", version="0.1.0")

app.add_middleware(AuditLogMiddleware)
app.add_middleware(RateLimitMiddleware, requests_per_minute=60)

app.include_router(metadata_router)
app.include_router(orchestration_router)
app.include_router(execution_router)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
