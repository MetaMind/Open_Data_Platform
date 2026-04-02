"""In-memory rate limiting middleware for the ODEP API Gateway."""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Dict, Tuple

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

__all__ = ["RateLimitMiddleware"]


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Per-user and per-tenant rate limiting middleware."""

    def __init__(self, app, requests_per_minute: int = 60):
        super().__init__(app)
        self._rpm = requests_per_minute
        self._counters: Dict[str, Tuple[int, float]] = defaultdict(lambda: (0, time.time()))

    async def dispatch(self, request: Request, call_next):
        key = self._get_rate_limit_key(request)
        now = time.time()
        count, window_start = self._counters[key]

        if now - window_start >= 60:
            self._counters[key] = (1, now)
        else:
            count += 1
            self._counters[key] = (count, window_start)
            if count > self._rpm:
                return JSONResponse(
                    status_code=429,
                    content={"detail": f"Rate limit exceeded. Max {self._rpm} requests/minute."},
                )

        return await call_next(request)

    def _get_rate_limit_key(self, request: Request) -> str:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
            try:
                from jose import jwt as jose_jwt
                payload = jose_jwt.get_unverified_claims(token)
                return payload.get("sub", request.client.host if request.client else "unknown")
            except Exception:
                pass
        return request.client.host if request.client else "unknown"
