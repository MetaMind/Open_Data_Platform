"""Role-based access control — full implementation."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Role hierarchy
ROLE_PERMISSIONS: dict[str, set[str]] = {
    "admin": {
        "execute_query", "manage_features", "view_metrics",
        "manage_budgets", "manage_roles", "register_tables",
    },
    "analyst": {
        "execute_query", "view_metrics",
    },
    "readonly": {
        "view_metrics",
    },
}


class RBACManager:
    """Full role-based access control.

    Roles: admin, analyst, readonly
    Permissions: execute_query, manage_features, view_metrics,
                 manage_budgets, manage_roles, register_tables
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        # In-memory cache: (tenant_id, user_id) → set[role]
        self._role_cache: dict[tuple[str, str], set[str]] = {}

    def check_permission(
        self,
        tenant_id: str,
        user_id: str,
        resource: str,
        action: str,
    ) -> bool:
        """Check if user has permission for action on resource.

        Admin passes all. Analyst: execute_query + view_metrics.
        Readonly: view_metrics only.
        """
        roles = self._get_roles(tenant_id, user_id)
        if not roles:
            # No roles assigned → deny
            logger.debug(
                "RBAC deny (no roles): tenant=%s user=%s %s:%s",
                tenant_id, user_id, resource, action,
            )
            return False

        required = action
        for role in roles:
            allowed = ROLE_PERMISSIONS.get(role, set())
            if required in allowed:
                logger.debug(
                    "RBAC allow (role=%s): tenant=%s user=%s %s",
                    role, tenant_id, user_id, required,
                )
                return True

        logger.warning(
            "RBAC deny: tenant=%s user=%s roles=%s %s:%s",
            tenant_id, user_id, roles, resource, action,
        )
        return False

    def grant_role(
        self,
        tenant_id: str,
        user_id: str,
        role: str,
        granted_by: str = "system",
    ) -> None:
        """Grant role to user. Upsert into mm_rbac_roles."""
        if role not in ROLE_PERMISSIONS:
            raise ValueError(f"Unknown role: {role}. Valid: {list(ROLE_PERMISSIONS)}")
        try:
            with self._engine.begin() as conn:
                # Idempotent: delete then insert
                conn.execute(
                    sa.text(
                        "DELETE FROM mm_rbac_roles "
                        "WHERE tenant_id = :tid AND user_id = :uid AND role = :role"
                    ),
                    {"tid": tenant_id, "uid": user_id, "role": role},
                )
                conn.execute(
                    sa.text(
                        "INSERT INTO mm_rbac_roles "
                        "(tenant_id, user_id, role, granted_at, granted_by) "
                        "VALUES (:tid, :uid, :role, :now, :by)"
                    ),
                    {
                        "tid": tenant_id,
                        "uid": user_id,
                        "role": role,
                        "now": datetime.utcnow().isoformat(),
                        "by": granted_by,
                    },
                )
        except Exception as exc:
            logger.error("Failed to grant role %s to %s: %s", role, user_id, exc)
            raise
        # Invalidate cache
        self._role_cache.pop((tenant_id, user_id), None)
        logger.info("Granted role=%s to user=%s (tenant=%s)", role, user_id, tenant_id)

    def revoke_role(self, tenant_id: str, user_id: str, role: str) -> None:
        """Revoke role from user."""
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    sa.text(
                        "DELETE FROM mm_rbac_roles "
                        "WHERE tenant_id = :tid AND user_id = :uid AND role = :role"
                    ),
                    {"tid": tenant_id, "uid": user_id, "role": role},
                )
        except Exception as exc:
            logger.error("Failed to revoke role %s from %s: %s", role, user_id, exc)
            raise
        self._role_cache.pop((tenant_id, user_id), None)
        logger.info("Revoked role=%s from user=%s (tenant=%s)", role, user_id, tenant_id)

    def list_roles(self, tenant_id: str, user_id: str) -> list[str]:
        """Return all roles assigned to user."""
        return sorted(self._get_roles(tenant_id, user_id))

    def _get_roles(self, tenant_id: str, user_id: str) -> set[str]:
        """Fetch roles from DB (with in-memory cache)."""
        cache_key = (tenant_id, user_id)
        if cache_key in self._role_cache:
            return self._role_cache[cache_key]
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    sa.text(
                        "SELECT role FROM mm_rbac_roles "
                        "WHERE tenant_id = :tid AND user_id = :uid"
                    ),
                    {"tid": tenant_id, "uid": user_id},
                ).fetchall()
            roles = {row[0] for row in rows}
        except Exception as exc:
            logger.warning(
                "Could not fetch roles for %s/%s (table may not exist): %s",
                tenant_id, user_id, exc,
            )
            roles = set()
        self._role_cache[cache_key] = roles
        return roles
