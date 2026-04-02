"""MetaMind API authentication — JWT-based tenant auth."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import Header, HTTPException, status
from jose import JWTError, jwt

from metamind.config.settings import get_settings

logger = logging.getLogger(__name__)


def get_current_tenant(
    authorization: Optional[str] = Header(default=None),
    x_tenant_id: Optional[str] = Header(default=None, alias="X-Tenant-ID"),
) -> str:
    """Extract and validate tenant_id from JWT token or X-Tenant-ID header.

    In development mode (METAMIND_ENV=development), falls back to 'default' tenant.
    """
    settings = get_settings()
    security = settings.security

    # Development shortcut: allow X-Tenant-ID header directly
    if settings.app_env == "development" and x_tenant_id:
        return x_tenant_id

    if not authorization:
        if settings.app_env == "development":
            return "default"
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bearer token required",
        )

    token = authorization[7:]

    try:
        payload = jwt.decode(
            token,
            security.jwt_secret,
            algorithms=[security.jwt_algorithm],
        )
        tenant_id: Optional[str] = payload.get("tenant_id") or payload.get("sub")
        if not tenant_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token missing tenant_id claim",
            )
        return tenant_id

    except JWTError as exc:
        logger.warning("JWT validation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


def create_token(tenant_id: str, expire_minutes: Optional[int] = None) -> str:
    """Create a JWT token for a tenant (used in tests and CLI)."""
    import datetime

    settings = get_settings()
    security = settings.security
    exp = expire_minutes or (security.jwt_expiration_hours * 60)
    payload = {
        "tenant_id": tenant_id,
        "sub": tenant_id,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=exp),
        "iat": datetime.datetime.utcnow(),
    }
    return jwt.encode(payload, security.jwt_secret, algorithm=security.jwt_algorithm)
