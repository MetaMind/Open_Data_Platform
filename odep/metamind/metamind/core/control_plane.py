"""
Control Plane — Unified Import Facade

File: metamind/core/control_plane.py
Refactored: Split into three sub-modules to comply with Golden Rule (≤500 lines).

This module re-exports all public symbols so existing imports continue to work:

    from metamind.core.control_plane import (
        EngineHealthRegistry, EngineHealth, EngineStatus,
        RoutingPolicyManager, RoutingPolicy, RoutingPolicyType,
        TenantResourceManager, TenantQuota,
    )
"""

from __future__ import annotations

from metamind.core.health_registry import (   # noqa: F401
    EngineHealth,
    EngineHealthRegistry,
    EngineStatus,
)
from metamind.core.policy_manager import (    # noqa: F401
    RoutingPolicy,
    RoutingPolicyManager,
    RoutingPolicyType,
)
from metamind.core.quota_manager import (     # noqa: F401
    TenantQuota,
    TenantResourceManager,
)

__all__ = [
    # Health
    "EngineHealth",
    "EngineHealthRegistry",
    "EngineStatus",
    # Policy
    "RoutingPolicy",
    "RoutingPolicyManager",
    "RoutingPolicyType",
    # Quota
    "TenantQuota",
    "TenantResourceManager",
]
