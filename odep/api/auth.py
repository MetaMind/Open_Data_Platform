"""OIDC/OAuth2 JWT authentication middleware for the ODEP API Gateway."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from jose import JWTError, jwt
from pydantic_settings import BaseSettings, SettingsConfigDict

from odep.exceptions import AuthenticationError

__all__ = ["JWTSettings", "TokenData", "verify_jwt_token", "get_current_user"]


class JWTSettings(BaseSettings):
    """JWT validation settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_prefix="ODEP_JWT_")

    jwt_secret: str = "dev-secret-change-in-production"
    jwt_algorithm: str = "HS256"
    jwt_issuer: Optional[str] = None


@dataclass
class TokenData:
    """Decoded JWT token payload."""

    sub: str
    tenant_id: Optional[str] = None
    roles: List[str] = field(default_factory=list)
    exp: Optional[int] = None


def verify_jwt_token(token: str, settings: Optional[JWTSettings] = None) -> TokenData:
    """Decode and validate a JWT token, returning extracted TokenData.

    Raises AuthenticationError on invalid or expired tokens.
    """
    if settings is None:
        settings = JWTSettings()
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    except JWTError as e:
        raise AuthenticationError("api", f"Invalid or expired JWT: {e}") from e
    return TokenData(
        sub=payload.get("sub", ""),
        tenant_id=payload.get("tenant_id"),
        roles=payload.get("roles", []),
        exp=payload.get("exp"),
    )


# FastAPI dependency
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> TokenData:
    """FastAPI dependency — validates Bearer JWT and returns TokenData. Returns 401 on failure."""
    try:
        return verify_jwt_token(credentials.credentials)
    except AuthenticationError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e)) from e
