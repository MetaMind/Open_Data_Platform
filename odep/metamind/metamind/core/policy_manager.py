"""
Routing Policy Manager — Control Plane Component

File: metamind/core/policy_manager.py
Role: Platform Engineer
Split from: metamind/core/control_plane.py (Golden Rule: ≤500 lines)

Priority-ordered routing policies drive per-tenant engine selection.
Policies are loaded from PostgreSQL and cached in Redis (5 min TTL).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)


class RoutingPolicyType(Enum):
    """Supported routing policy kinds."""
    COST_BASED = "cost_based"
    FRESHNESS_BASED = "freshness_based"
    LOAD_BALANCED = "load_balanced"
    CUSTOM = "custom"


@dataclass
class RoutingPolicy:
    """A single routing policy configuration for a tenant."""
    policy_id: str
    tenant_id: str
    policy_type: RoutingPolicyType
    name: str
    description: str
    priority: int           # Higher value = evaluated earlier
    rules: Dict[str, Any]
    conditions: Dict[str, Any]
    target_engine: str
    fallback_engine: Optional[str] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "policy_type": self.policy_type.value,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_row(cls, row: Any) -> RoutingPolicy:
        """Construct from a SQLAlchemy result row."""
        return cls(
            policy_id=row.policy_id,
            tenant_id=row.tenant_id,
            policy_type=RoutingPolicyType(row.policy_type),
            name=row.name,
            description=row.description,
            priority=row.priority,
            rules=json.loads(row.rules) if row.rules else {},
            conditions=json.loads(row.conditions) if row.conditions else {},
            target_engine=row.target_engine,
            fallback_engine=row.fallback_engine,
            is_active=row.is_active,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


class RoutingPolicyManager:
    """
    Load, cache, and evaluate tenant routing policies.

    Policy evaluation is short-circuit: the first matching policy
    (by descending priority) is returned; remaining policies are skipped.

    Conditions currently supported:
    - ``max_freshness_seconds``  — route only if query tolerance ≤ value
    - ``max_complexity_score``   — route only if complexity ≤ value
    - ``max_estimated_rows``     — route only if cardinality ≤ value
    - target engine must not be UNHEALTHY
    """

    _POLICY_CACHE_TTL = 300  # seconds

    def __init__(self, db_engine: Any, redis_client: Any) -> None:
        self.db_engine = db_engine
        self.redis = redis_client
        logger.debug("RoutingPolicyManager initialised")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def load_policies(self, tenant_id: str) -> List[RoutingPolicy]:
        """Return active policies for *tenant_id*, sorted by priority desc."""
        cache_key = f"policies:{tenant_id}"
        cached = await self.redis.get(cache_key)
        if cached:
            return [RoutingPolicy(**p) for p in json.loads(cached)]

        policies = self._fetch_from_db(tenant_id)
        await self.redis.setex(
            cache_key,
            self._POLICY_CACHE_TTL,
            json.dumps([p.to_dict() for p in policies]),
        )
        return policies

    async def evaluate_policies(
        self,
        tenant_id: str,
        query_features: Dict[str, Any],
        engine_health: Dict[str, Any],
    ) -> Optional[RoutingPolicy]:
        """Return the first matching policy, or None if no policy matches."""
        for policy in await self.load_policies(tenant_id):
            if self._matches(policy, query_features, engine_health):
                logger.debug(
                    "Policy '%s' matched for tenant %s", policy.name, tenant_id
                )
                return policy
        return None

    async def create_policy(self, policy: RoutingPolicy) -> None:
        """Persist a new routing policy and invalidate the tenant cache."""
        with self.db_engine.connect() as conn:
            conn.execute(
                text("""
                INSERT INTO mm_routing_policies
                    (policy_id, tenant_id, policy_type, name, description, priority,
                     rules, conditions, target_engine, fallback_engine, is_active, created_at)
                VALUES
                    (:policy_id, :tenant_id, :policy_type, :name, :description, :priority,
                     :rules::jsonb, :conditions::jsonb, :target_engine, :fallback_engine,
                     TRUE, NOW())
                ON CONFLICT (policy_id) DO UPDATE
                    SET updated_at = NOW(), is_active = TRUE
                """),
                {
                    "policy_id": policy.policy_id,
                    "tenant_id": policy.tenant_id,
                    "policy_type": policy.policy_type.value,
                    "name": policy.name,
                    "description": policy.description,
                    "priority": policy.priority,
                    "rules": json.dumps(policy.rules),
                    "conditions": json.dumps(policy.conditions),
                    "target_engine": policy.target_engine,
                    "fallback_engine": policy.fallback_engine,
                },
            )
            conn.commit()
        await self.redis.delete(f"policies:{policy.tenant_id}")
        logger.info("Created routing policy '%s' for tenant %s", policy.name, policy.tenant_id)

    async def deactivate_policy(self, policy_id: str, tenant_id: str) -> None:
        """Soft-delete a policy and invalidate cache."""
        with self.db_engine.connect() as conn:
            conn.execute(
                text("UPDATE mm_routing_policies SET is_active = FALSE, updated_at = NOW() WHERE policy_id = :id"),
                {"id": policy_id},
            )
            conn.commit()
        await self.redis.delete(f"policies:{tenant_id}")
        logger.info("Deactivated routing policy %s", policy_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_from_db(self, tenant_id: str) -> List[RoutingPolicy]:
        try:
            with self.db_engine.connect() as conn:
                rows = conn.execute(
                    text("""
                    SELECT policy_id, tenant_id, policy_type, name, description,
                           priority, rules, conditions, target_engine, fallback_engine,
                           is_active, created_at, updated_at
                    FROM mm_routing_policies
                    WHERE tenant_id = :tenant_id AND is_active = TRUE
                    ORDER BY priority DESC
                    """),
                    {"tenant_id": tenant_id},
                ).fetchall()
            return [RoutingPolicy.from_row(r) for r in rows]
        except Exception as exc:
            logger.warning("Failed to load policies for tenant %s: %s", tenant_id, exc)
            return []

    def _matches(
        self,
        policy: RoutingPolicy,
        features: Dict[str, Any],
        engine_health: Dict[str, Any],
    ) -> bool:
        """Return True if *all* policy conditions are satisfied."""
        cond = policy.conditions

        freshness_ok = (
            features.get("freshness_tolerance_seconds", 300)
            <= cond.get("max_freshness_seconds", float("inf"))
        )
        complexity_ok = (
            features.get("complexity_score", 0)
            <= cond.get("max_complexity_score", float("inf"))
        )
        rows_ok = (
            features.get("estimated_cardinality", 0)
            <= cond.get("max_estimated_rows", float("inf"))
        )

        target_h = engine_health.get(policy.target_engine)
        engine_ok = target_h is None or getattr(target_h, "status", None) != "unhealthy"

        return freshness_ok and complexity_ok and rows_ok and engine_ok
