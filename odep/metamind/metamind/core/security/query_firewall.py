"""Query Fingerprint Firewall — allowlist and denylist for query shapes.

Blocks known-bad query patterns and optionally restricts tenants to
pre-approved query fingerprints.

Redis keys:
  mm:firewall:deny:{tenant_id}   — SET of denied fingerprints
  mm:firewall:allow:{tenant_id}  — SET of allowed fingerprints
  mm:firewall:mode:{tenant_id}   — "allowlist" | "open" (default open)
"""
from __future__ import annotations

import hashlib
import inspect
import logging
import re
from dataclasses import dataclass

import sqlglot
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_LITERAL_RE = re.compile(r"'[^']*'|\b\d+(?:\.\d+)?\b", re.IGNORECASE)


@dataclass
class FirewallDecision:
    """Result of a firewall check."""

    allowed: bool
    reason: str
    fingerprint: str


class QueryFirewall:
    """Enforce per-tenant query allowlist and denylist policies.

    Args:
        db_engine: SQLAlchemy Engine (used to persist rules).
        redis_client: Synchronous Redis client for hot-path lookups.
    """

    _DENY_KEY = "mm:firewall:deny:{tenant}"
    _ALLOW_KEY = "mm:firewall:allow:{tenant}"
    _MODE_KEY = "mm:firewall:mode:{tenant}"

    def __init__(self, db_engine: Engine, redis_client: object) -> None:
        self._engine = db_engine
        self._redis = redis_client

    # ------------------------------------------------------------------
    # Fingerprinting
    # ------------------------------------------------------------------

    def fingerprint(self, sql: str) -> str:
        """Produce a stable SHA-256 fingerprint of a normalized SQL string.

        Strips literals and whitespace so parameterized variants of the
        same query shape map to the same fingerprint.
        """
        normalized = self._normalize(sql)
        return hashlib.sha256(normalized.encode()).hexdigest()

    def _normalize(self, sql: str) -> str:
        """Lowercase, strip literals, collapse whitespace."""
        try:
            stmts = sqlglot.parse(sql)
            if stmts and stmts[0] is not None:
                sql = stmts[0].sql(pretty=False)
        except Exception:
            pass
        s = sql.lower()
        s = _LITERAL_RE.sub("?", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    # ------------------------------------------------------------------
    # Firewall check (hot path)
    # ------------------------------------------------------------------

    async def check(self, sql: str, tenant_id: str) -> FirewallDecision:
        """Check whether sql is allowed for this tenant.

        Evaluation order:
        1. Denylist (Redis SET) — blocked immediately if present.
        2. Allowlist mode — blocked if fingerprint not in allowlist SET.
        3. Otherwise allowed.
        """
        fp = self.fingerprint(sql)

        try:
            deny_key = self._DENY_KEY.format(tenant=tenant_id)
            deny_member = self._redis.sismember(deny_key, fp)  # type: ignore[union-attr]
            if inspect.isawaitable(deny_member):
                deny_member = await deny_member
            if bool(deny_member):
                logger.warning(
                    "Firewall DENY tenant=%s fp=%s", tenant_id, fp[:16]
                )
                return FirewallDecision(
                    allowed=False,
                    reason="Query fingerprint is on the denylist.",
                    fingerprint=fp,
                )

            mode_key = self._MODE_KEY.format(tenant=tenant_id)
            mode = self._redis.get(mode_key)  # type: ignore[union-attr]
            if inspect.isawaitable(mode):
                mode = await mode
            if mode and (
                mode == b"allowlist" or mode == "allowlist"
            ):
                allow_key = self._ALLOW_KEY.format(tenant=tenant_id)
                allow_member = self._redis.sismember(allow_key, fp)  # type: ignore[union-attr]
                if inspect.isawaitable(allow_member):
                    allow_member = await allow_member
                if not bool(allow_member):
                    logger.warning(
                        "Firewall DENY (allowlist mode) tenant=%s fp=%s",
                        tenant_id,
                        fp[:16],
                    )
                    return FirewallDecision(
                        allowed=False,
                        reason="Allowlist mode active; query fingerprint not permitted.",
                        fingerprint=fp,
                    )

        except Exception as exc:
            logger.error(
                "QueryFirewall.check failed tenant=%s: %s — allowing query",
                tenant_id,
                exc,
            )
            return FirewallDecision(
                allowed=True,
                reason=f"Firewall check error (fail-open): {exc}",
                fingerprint=fp,
            )

        return FirewallDecision(allowed=True, reason="Allowed.", fingerprint=fp)

    # ------------------------------------------------------------------
    # Rule management
    # ------------------------------------------------------------------

    def add_deny(self, tenant_id: str, fingerprint: str) -> None:
        """Add fingerprint to the tenant denylist."""
        key = self._DENY_KEY.format(tenant=tenant_id)
        try:
            self._redis.sadd(key, fingerprint)  # type: ignore[union-attr]
            logger.info("Firewall added to denylist tenant=%s fp=%s", tenant_id, fingerprint[:16])
        except Exception as exc:
            logger.error("Firewall.add_deny failed: %s", exc)

    def add_allow(self, tenant_id: str, fingerprint: str) -> None:
        """Add fingerprint to the tenant allowlist."""
        key = self._ALLOW_KEY.format(tenant=tenant_id)
        try:
            self._redis.sadd(key, fingerprint)  # type: ignore[union-attr]
            logger.info("Firewall added to allowlist tenant=%s fp=%s", tenant_id, fingerprint[:16])
        except Exception as exc:
            logger.error("Firewall.add_allow failed: %s", exc)

    def remove_rule(self, tenant_id: str, fingerprint: str) -> int:
        """Remove fingerprint from both deny and allow sets; return count removed."""
        removed = 0
        for key in (
            self._DENY_KEY.format(tenant=tenant_id),
            self._ALLOW_KEY.format(tenant=tenant_id),
        ):
            try:
                removed += int(self._redis.srem(key, fingerprint) or 0)  # type: ignore[union-attr]
            except Exception as exc:
                logger.error("Firewall.remove_rule failed: %s", exc)
        return removed

    def list_rules(self, tenant_id: str) -> dict[str, list[str]]:
        """Return all deny and allow fingerprints for tenant."""
        result: dict[str, list[str]] = {"deny": [], "allow": []}
        for label, key in (
            ("deny", self._DENY_KEY.format(tenant=tenant_id)),
            ("allow", self._ALLOW_KEY.format(tenant=tenant_id)),
        ):
            try:
                members = self._redis.smembers(key) or set()  # type: ignore[union-attr]
                result[label] = [
                    m.decode() if isinstance(m, bytes) else m for m in members
                ]
            except Exception as exc:
                logger.error("Firewall.list_rules failed label=%s: %s", label, exc)
        return result
