"""Unit tests for QueryFirewall (Task 06) — W-04."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from metamind.core.security.query_firewall import QueryFirewall


def _make_fw(deny: set | None = None, allow: set | None = None, mode: str = "open") -> tuple[QueryFirewall, MagicMock]:
    """Return (firewall, mock_redis) with pre-seeded sets."""
    redis = MagicMock()
    db = MagicMock()

    deny_set: set[str] = deny or set()
    allow_set: set[str] = allow or set()

    def sismember(key: str, val: str) -> bool:
        if "deny" in key:
            return val in deny_set
        if "allow" in key:
            return val in allow_set
        return False

    def get(key: str) -> bytes | None:
        if "mode" in key:
            return mode.encode()
        return None

    redis.sismember.side_effect = sismember
    redis.get.side_effect = get
    fw = QueryFirewall(db_engine=db, redis_client=redis)
    return fw, redis


class TestFingerprint:
    """Fingerprint is stable and formatting-independent."""

    def test_same_query_same_fp(self) -> None:
        fw, _ = _make_fw()
        fp1 = fw.fingerprint("SELECT id FROM users WHERE id = 1")
        fp2 = fw.fingerprint("  select  ID  from  USERS  where  id=1  ")
        assert fp1 == fp2

    def test_different_queries_different_fp(self) -> None:
        fw, _ = _make_fw()
        fp1 = fw.fingerprint("SELECT id FROM users")
        fp2 = fw.fingerprint("SELECT name FROM users")
        assert fp1 != fp2

    def test_fp_is_hex_string(self) -> None:
        fw, _ = _make_fw()
        fp = fw.fingerprint("SELECT 1")
        assert len(fp) == 64
        int(fp, 16)  # must be valid hex


class TestDenylistMode:
    """In open mode, only denylist is checked."""

    @pytest.mark.asyncio
    async def test_allowed_when_not_on_denylist(self) -> None:
        fw, _ = _make_fw(deny=set(), mode="open")
        decision = await fw.check("SELECT id FROM users", "tenant1")
        assert decision.allowed is True

    @pytest.mark.asyncio
    async def test_blocked_when_on_denylist(self) -> None:
        fw, _ = _make_fw(mode="open")
        sql = "SELECT * FROM users WHERE 1=1"
        fp = fw.fingerprint(sql)
        fw2, _ = _make_fw(deny={fp}, mode="open")
        decision = await fw2.check(sql, "tenant1")
        assert decision.allowed is False
        assert "deny" in decision.reason.lower()


class TestAllowlistMode:
    """In allowlist mode, queries not on allowlist are blocked."""

    @pytest.mark.asyncio
    async def test_blocked_when_not_on_allowlist(self) -> None:
        fw, _ = _make_fw(allow=set(), mode="allow")
        decision = await fw.check("SELECT secret FROM vault", "tenant1")
        assert decision.allowed is False

    @pytest.mark.asyncio
    async def test_allowed_when_on_allowlist(self) -> None:
        sql = "SELECT id FROM products LIMIT 10"
        fw, _ = _make_fw(mode="open")
        fp = fw.fingerprint(sql)
        fw2, _ = _make_fw(allow={fp}, mode="allow")
        decision = await fw2.check(sql, "tenant1")
        assert decision.allowed is True


class TestRedisFailOpen:
    """If Redis is unavailable, firewall allows queries."""

    @pytest.mark.asyncio
    async def test_redis_error_is_fail_open(self) -> None:
        redis = MagicMock()
        redis.sismember.side_effect = ConnectionError("Redis down")
        redis.get.side_effect = ConnectionError("Redis down")
        fw = QueryFirewall(db_engine=MagicMock(), redis_client=redis)
        decision = await fw.check("SELECT 1", "tenant1")
        assert decision.allowed is True
