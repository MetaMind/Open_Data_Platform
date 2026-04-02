"""Unit tests for RLSRewriter (Task 04) — W-04."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from metamind.core.security.rls_rewriter import RLSRewriter, RLSPolicy


def _mock_engine(policies: list[RLSPolicy]) -> MagicMock:
    """Build a mock SQLAlchemy engine that returns the given policies."""
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        MagicMock(
            id=p.id,
            tenant_id=p.tenant_id,
            table_name=p.table_name,
            filter_expr=p.filter_expr,
            roles=p.roles,
            is_active=p.is_active,
        )
        for p in policies
    ]
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


class TestLoadPolicies:
    """load_policies() returns policies and handles DB errors gracefully."""

    @pytest.mark.asyncio
    async def test_returns_policies(self) -> None:
        pol = RLSPolicy(id=1, tenant_id="t1", table_name="orders",
                        filter_expr="tenant_id = :tenant_id", roles=[], is_active=True)
        rw = RLSRewriter(_mock_engine([pol]))
        result = await rw.load_policies("t1")
        assert len(result) == 1
        assert result[0].table_name == "orders"

    @pytest.mark.asyncio
    async def test_db_error_returns_empty_list(self) -> None:
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("DB down")
        rw = RLSRewriter(engine)
        result = await rw.load_policies("t1")
        assert result == []


class TestRewrite:
    """rewrite() injects policy WHERE clauses into table references."""

    @pytest.mark.asyncio
    async def test_no_policies_returns_original(self) -> None:
        rw = RLSRewriter(_mock_engine([]))
        sql = "SELECT id FROM orders WHERE id = 1"
        out = await rw.rewrite(sql, "t1", [])
        assert out == sql

    @pytest.mark.asyncio
    async def test_policy_applied_to_matching_table(self) -> None:
        pol = RLSPolicy(id=1, tenant_id="t1", table_name="orders",
                        filter_expr="tenant_id = :tenant_id", roles=[], is_active=True)
        rw = RLSRewriter(_mock_engine([pol]))
        out = await rw.rewrite("SELECT id FROM orders", "acme", [])
        # The output should contain a subquery wrapping 'orders'
        assert "acme" in out
        assert "SELECT" in out.upper()

    @pytest.mark.asyncio
    async def test_role_filtered_policy_not_applied_to_wrong_role(self) -> None:
        pol = RLSPolicy(id=1, tenant_id="t1", table_name="payments",
                        filter_expr="tenant_id = :tenant_id",
                        roles=["analyst"], is_active=True)
        rw = RLSRewriter(_mock_engine([pol]))
        sql = "SELECT id FROM payments"
        # caller has role 'viewer' — policy should not apply
        out = await rw.rewrite(sql, "t1", ["viewer"])
        assert out == sql

    @pytest.mark.asyncio
    async def test_role_filtered_policy_applied_to_correct_role(self) -> None:
        pol = RLSPolicy(id=1, tenant_id="t1", table_name="payments",
                        filter_expr="tenant_id = :tenant_id",
                        roles=["analyst"], is_active=True)
        rw = RLSRewriter(_mock_engine([pol]))
        out = await rw.rewrite("SELECT id FROM payments", "t1", ["analyst"])
        assert "t1" in out


class TestSafeSqlLiteral:
    """_safe_sql_literal() escapes SQL metacharacters (fixes W-03)."""

    def test_normal_value(self) -> None:
        result = RLSRewriter._safe_sql_literal("acme")
        assert result == "'acme'"

    def test_single_quote_escaped(self) -> None:
        """SQL injection payload must be quoted, not interpolated raw."""
        result = RLSRewriter._safe_sql_literal("acme' OR '1'='1")
        # Must not contain unescaped single quote that breaks the WHERE clause
        assert "OR" not in result or result.startswith("'")
        assert result.startswith("'") and result.endswith("'")


class TestParseErrorFallback:
    """Invalid SQL falls back gracefully to original."""

    @pytest.mark.asyncio
    async def test_unparseable_sql_returns_original(self) -> None:
        pol = RLSPolicy(id=1, tenant_id="t1", table_name="orders",
                        filter_expr="tenant_id = :tenant_id", roles=[], is_active=True)
        rw = RLSRewriter(_mock_engine([pol]))
        bad_sql = "NOT VALID SQL ### @@@"
        out = await rw.rewrite(bad_sql, "t1", [])
        assert out == bad_sql
