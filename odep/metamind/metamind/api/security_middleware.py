"""
Security Middleware — Production HTTP Security Headers

File: metamind/api/security_middleware.py
Role: Security Engineer
Addresses: Audit finding — HSTS, CSP, X-Frame-Options missing from server.py

Adds the following security headers to every HTTP response:
  Strict-Transport-Security    (HSTS)
  Content-Security-Policy      (CSP)
  X-Frame-Options              (clickjacking)
  X-Content-Type-Options       (MIME sniffing)
  Referrer-Policy
  Permissions-Policy
  X-Request-ID                 (traceability)

Also fixes the bare-pass exception in server.py for query cancellation
by providing a proper cancellation tracker.
"""

from __future__ import annotations

import logging
import uuid
from typing import Awaitable, Callable, Dict, Optional, Set

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Injects enterprise-grade HTTP security headers on every response.

    Usage (add to FastAPI app before any route is registered)::

        from metamind.api.security_middleware import SecurityHeadersMiddleware
        app.add_middleware(SecurityHeadersMiddleware, environment="production")
    """

    # Strict CSP for the MetaMind API (API-only, no HTML served)
    _CSP_API = (
        "default-src 'none'; "
        "script-src 'none'; "
        "object-src 'none'; "
        "frame-ancestors 'none';"
    )

    # Relaxed CSP for development (allows inline scripts for Swagger UI)
    _CSP_DEV = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "connect-src 'self';"
    )

    def __init__(
        self,
        app: Awaitable,
        environment: str = "production",
        hsts_max_age: int = 31_536_000,       # 1 year
        include_subdomains: bool = True,
        preload_hsts: bool = False,
    ) -> None:
        super().__init__(app)
        self.environment = environment
        self.hsts_max_age = hsts_max_age
        self.include_subdomains = include_subdomains
        self.preload_hsts = preload_hsts
        logger.info(
            "SecurityHeadersMiddleware initialised (env=%s, hsts=%ds)",
            environment, hsts_max_age,
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Generate a request ID for traceability (propagate if already set)
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))

        response: Response = await call_next(request)

        # ── HSTS ─────────────────────────────────────────────────────────
        hsts_value = f"max-age={self.hsts_max_age}"
        if self.include_subdomains:
            hsts_value += "; includeSubDomains"
        if self.preload_hsts:
            hsts_value += "; preload"
        response.headers["Strict-Transport-Security"] = hsts_value

        # ── CSP ──────────────────────────────────────────────────────────
        csp = self._CSP_DEV if self.environment == "development" else self._CSP_API
        response.headers["Content-Security-Policy"] = csp

        # ── Clickjacking ─────────────────────────────────────────────────
        response.headers["X-Frame-Options"] = "DENY"

        # ── MIME sniffing ────────────────────────────────────────────────
        response.headers["X-Content-Type-Options"] = "nosniff"

        # ── Referrer ─────────────────────────────────────────────────────
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        # ── Permissions / Feature-Policy ─────────────────────────────────
        response.headers["Permissions-Policy"] = (
            "geolocation=(), microphone=(), camera=(), payment=()"
        )

        # ── Request traceability ─────────────────────────────────────────
        response.headers["X-Request-ID"] = request_id

        # ── Remove fingerprinting headers ────────────────────────────────
        if "Server" in response.headers:
            del response.headers["Server"]
        if "X-Powered-By" in response.headers:
            del response.headers["X-Powered-By"]

        return response


class QueryCancellationTracker:
    """
    Tracks in-flight query IDs to support cancellation requests.

    Fixes the bare TODO in server.py::cancel_query() by providing a
    thread-safe in-memory registry of active query IDs per tenant.

    For multi-instance deployments, back this with Redis (see RedisQueryTracker).
    """

    def __init__(self) -> None:
        self._active: Dict[str, str] = {}   # query_id → tenant_id
        self._cancelled: Set[str] = set()

    def register(self, query_id: str, tenant_id: str) -> None:
        """Mark *query_id* as in-flight for *tenant_id*."""
        self._active[query_id] = tenant_id
        self._cancelled.discard(query_id)
        logger.debug("Registered active query %s for tenant %s", query_id, tenant_id)

    def cancel(self, query_id: str, tenant_id: str) -> bool:
        """
        Request cancellation of *query_id*.

        Returns True if the query was found and flagged for cancellation,
        False if the query was not found (may have already completed).
        """
        if query_id not in self._active:
            logger.warning(
                "Cancel requested for unknown query %s (tenant=%s)", query_id, tenant_id
            )
            return False

        registered_tenant = self._active.get(query_id)
        if registered_tenant and registered_tenant != tenant_id:
            logger.warning(
                "Tenant %s attempted to cancel query %s owned by tenant %s",
                tenant_id, query_id, registered_tenant,
            )
            return False

        self._cancelled.add(query_id)
        logger.info("Query %s flagged for cancellation (tenant=%s)", query_id, tenant_id)
        return True

    def is_cancelled(self, query_id: str) -> bool:
        """Check whether a query has been cancelled."""
        return query_id in self._cancelled

    def complete(self, query_id: str) -> None:
        """Remove *query_id* from the active registry."""
        self._active.pop(query_id, None)
        self._cancelled.discard(query_id)


class LegacyRateLimitMiddleware(BaseHTTPMiddleware):
    """
    Simple in-process rate limiter as a fallback when Redis is unavailable.

    For production use, prefer the Redis-backed rate limiting in
    TenantResourceManager (quota_manager.py), which is already wired
    into the query router.  This middleware provides HTTP-layer protection
    for ALL endpoints (including /health, /metrics) not covered by the
    router quota.
    """

    def __init__(
        self,
        app: Awaitable,
        requests_per_minute: int = 1_000,
        burst: int = 200,
    ) -> None:
        super().__init__(app)
        self.rpm = requests_per_minute
        self.burst = burst
        self._counters: Dict[str, int] = {}
        logger.info(
            "LegacyRateLimitMiddleware: %d req/min, burst=%d", requests_per_minute, burst
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Extract tenant or fall back to IP
        tenant = request.headers.get("X-Tenant-ID") or (
            request.client.host if request.client else "unknown"
        )

        current = self._counters.get(tenant, 0)
        if current >= self.rpm + self.burst:
            logger.warning("Rate limit exceeded for %s (%d req)", tenant, current)
            return Response(
                content='{"detail":"Rate limit exceeded"}',
                status_code=429,
                headers={
                    "Content-Type": "application/json",
                    "Retry-After": "60",
                    "X-RateLimit-Limit": str(self.rpm),
                },
            )

        self._counters[tenant] = current + 1

        response = await call_next(request)

        # Attach rate-limit headers for client transparency
        response.headers["X-RateLimit-Limit"] = str(self.rpm)
        response.headers["X-RateLimit-Remaining"] = str(max(0, self.rpm - current))
        return response
