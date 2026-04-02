"""Audit logging middleware and helpers for the ODEP API Gateway."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

__all__ = ["AuditLogMiddleware", "emit_audit_log"]

logger = logging.getLogger("odep.audit")

MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
AUDITED_PATH_FRAGMENTS = {"/deploy", "/delete", "/tag", "/config"}


class AuditLogMiddleware(BaseHTTPMiddleware):
    """Structured audit logging middleware for mutating API operations."""

    async def dispatch(self, request: Request, call_next):
        if request.method in MUTATING_METHODS and any(
            frag in request.url.path for frag in AUDITED_PATH_FRAGMENTS
        ):
            user_identity = self._extract_user(request)
            resource_urn = request.query_params.get("urn", request.url.path)
            timestamp = datetime.now(timezone.utc).isoformat()
            logger.info(
                "AUDIT",
                extra={
                    "user": user_identity,
                    "timestamp": timestamp,
                    "resource_urn": resource_urn,
                    "method": request.method,
                    "path": request.url.path,
                },
            )
        return await call_next(request)

    def _extract_user(self, request: Request) -> str:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            try:
                from jose import jwt as jose_jwt
                payload = jose_jwt.get_unverified_claims(auth[7:])
                return payload.get("sub", "anonymous")
            except Exception:
                pass
        return "anonymous"


def emit_audit_log(user: str, resource_urn: str, action: str) -> None:
    """Emit a structured audit log entry (for use in route handlers)."""
    logger.info(
        "AUDIT",
        extra={
            "user": user,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "resource_urn": resource_urn,
            "action": action,
        },
    )
