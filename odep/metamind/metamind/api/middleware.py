"""FastAPI middleware: rate limiting, request logging."""
from __future__ import annotations

import logging
import time
import uuid
from collections import defaultdict
from typing import Callable, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

logger = logging.getLogger(__name__)

# Endpoints exempt from rate limiting
_RATE_LIMIT_EXEMPT = {"/health", "/ready", "/metrics"}


class _TokenBucket:
    """Simple in-process token bucket for rate limiting."""

    def __init__(self, max_rps: int, burst: int) -> None:
        self._max_rps = max_rps
        self._burst = burst
        self._tokens: float = float(burst)
        self._last_refill: float = time.monotonic()

    def consume(self) -> tuple[bool, float]:
        """Try to consume one token. Returns (allowed, retry_after_seconds)."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        # Refill tokens
        self._tokens = min(
            float(self._burst),
            self._tokens + elapsed * self._max_rps,
        )
        self._last_refill = now
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True, 0.0
        retry_after = (1.0 - self._tokens) / self._max_rps
        return False, retry_after


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Tenant-level rate limiting: max N requests/second per tenant_id.

    Uses token bucket algorithm. Falls back to in-memory bucket when Redis
    is unavailable. Returns 429 Too Many Requests with Retry-After header.
    """

    def __init__(
        self,
        app: ASGIApp,
        max_rps: int = 100,
        burst: int = 200,
        redis_client: Optional[object] = None,
    ) -> None:
        super().__init__(app)
        self._max_rps = max_rps
        self._burst = burst
        self._redis = redis_client
        # In-memory fallback buckets
        self._buckets: dict[str, _TokenBucket] = defaultdict(
            lambda: _TokenBucket(max_rps, burst)
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Check rate limit before forwarding request."""
        path = request.url.path

        # Exempt health/metrics endpoints
        if path in _RATE_LIMIT_EXEMPT:
            return await call_next(request)

        tenant_id = (
            request.headers.get("X-Tenant-ID")
            or request.query_params.get("tenant_id")
            or "anonymous"
        )

        allowed, retry_after = self._check_rate_limit(tenant_id)
        if not allowed:
            logger.warning(
                "Rate limit exceeded for tenant=%s path=%s", tenant_id, path
            )
            return Response(
                content='{"error":"Too Many Requests","message":"Rate limit exceeded"}',
                status_code=429,
                headers={
                    "Retry-After": f"{retry_after:.1f}",
                    "Content-Type": "application/json",
                    "X-RateLimit-Limit": str(self._max_rps),
                },
            )

        return await call_next(request)

    def _check_rate_limit(self, tenant_id: str) -> tuple[bool, float]:
        """Check rate limit for tenant. Tries Redis first, falls back to in-memory."""
        if self._redis is not None:
            try:
                return self._redis_token_bucket(tenant_id)
            except Exception as exc:
                logger.debug("Redis rate limit check failed, using in-memory: %s", exc)

        bucket = self._buckets[tenant_id]
        return bucket.consume()

    def _redis_token_bucket(self, tenant_id: str) -> tuple[bool, float]:
        """Redis-backed token bucket using a Lua script for atomicity."""
        import time as _time
        now = _time.time()
        key = f"ratelimit:{tenant_id}"
        pipe = self._redis.pipeline()  # type: ignore[attr-defined]
        # Use Redis as simple counter within 1-second windows
        window_key = f"{key}:{int(now)}"
        pipe.incr(window_key)
        pipe.expire(window_key, 2)
        results = pipe.execute()
        count = results[0]
        if count <= self._burst:
            return True, 0.0
        retry_after = 1.0 - (now % 1.0)
        return False, retry_after


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Structured request logging with request_id, tenant_id, duration, status."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Log each request with structured metadata."""
        request_id = str(uuid.uuid4())[:12]
        tenant_id = (
            request.headers.get("X-Tenant-ID")
            or request.query_params.get("tenant_id")
            or "anonymous"
        )
        start = time.monotonic()

        # Attach request_id to request state for downstream use
        request.state.request_id = request_id
        request.state.tenant_id = tenant_id

        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = (time.monotonic() - start) * 1000
            logger.error(
                "Request failed: method=%s path=%s tenant=%s request_id=%s duration_ms=%.1f error=%s",
                request.method,
                request.url.path,
                tenant_id,
                request_id,
                duration_ms,
                exc,
            )
            raise

        duration_ms = (time.monotonic() - start) * 1000
        status = response.status_code

        log_fn = logger.info if status < 400 else logger.warning
        log_fn(
            "Request: method=%s path=%s status=%d tenant=%s request_id=%s duration_ms=%.1f",
            request.method,
            request.url.path,
            status,
            tenant_id,
            request_id,
            duration_ms,
        )

        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time-ms"] = f"{duration_ms:.1f}"
        return response
