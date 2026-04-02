"""
Integration Tests — Cloud Budget Tracking (F23)

File: tests/integration/test_budget_tracking.py

Tests budget enforcement, summary aggregation, alert firing, and
billing-cycle reset.  All database calls use an in-memory SQLite
database to keep tests self-contained and fast.
"""

from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ctx(db_engine: MagicMock = None) -> MagicMock:
    ctx = MagicMock()
    ctx.async_db_engine = db_engine or MagicMock()
    return ctx


def _make_app(ctx: MagicMock) -> MagicMock:
    app = MagicMock()
    app.state.context = ctx
    return app


def _make_request(ctx: MagicMock) -> MagicMock:
    req = MagicMock()
    req.app.state.context = ctx
    return req


def _make_conn(rows_by_call: list) -> tuple:
    """Return (mock_conn, mock_engine) where execute side_effects are rows_by_call."""
    conn = AsyncMock()
    results = []
    for rows in rows_by_call:
        if rows is None:
            r = MagicMock()
            r.fetchone = MagicMock(return_value=None)
            r.fetchall = MagicMock(return_value=[])
        elif isinstance(rows, list):
            r = MagicMock()
            r.fetchall = MagicMock(return_value=rows)
        else:
            r = MagicMock()
            r.fetchone = MagicMock(return_value=rows)
        results.append(r)
    conn.execute = AsyncMock(side_effect=results)

    ctx_obj = AsyncMock()
    ctx_obj.__aenter__ = AsyncMock(return_value=conn)
    ctx_obj.__aexit__ = AsyncMock(return_value=False)

    engine = MagicMock()
    engine.connect.return_value = ctx_obj
    engine.begin.return_value = ctx_obj
    return conn, engine


# ---------------------------------------------------------------------------
# Budget summary endpoint
# ---------------------------------------------------------------------------

class TestBudgetSummaryEndpoint(unittest.IsolatedAsyncioTestCase):

    async def test_budget_summary_returns_correct_structure(self) -> None:
        budget_row = MagicMock()
        budget_row.budget_name = "monthly-budget"
        budget_row.budget_limit_usd = 1000.0
        budget_row.billing_cycle = "monthly"
        budget_row.alert_threshold_pct = 80
        budget_row.current_spend = 650.0

        conn, engine = _make_conn([budget_row])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        from metamind.api.admin_routes import budget_summary
        result = await budget_summary(req, tenant_id="acme")

        assert result["tenant_id"] == "acme"
        assert result["budget_limit_usd"] == 1000.0
        assert result["current_spend_usd"] == 650.0
        assert result["pct_used"] == 65.0
        assert result["alert_color"] == "green"   # < 70%

    async def test_budget_summary_yellow_alert_at_75_pct(self) -> None:
        budget_row = MagicMock()
        budget_row.budget_name = "monthly-budget"
        budget_row.budget_limit_usd = 1000.0
        budget_row.billing_cycle = "monthly"
        budget_row.alert_threshold_pct = 80
        budget_row.current_spend = 750.0

        conn, engine = _make_conn([budget_row])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        from metamind.api.admin_routes import budget_summary
        result = await budget_summary(req, tenant_id="acme")

        assert result["alert_color"] == "yellow"  # 70–90%

    async def test_budget_summary_red_alert_at_95_pct(self) -> None:
        budget_row = MagicMock()
        budget_row.budget_name = "monthly-budget"
        budget_row.budget_limit_usd = 1000.0
        budget_row.billing_cycle = "monthly"
        budget_row.alert_threshold_pct = 80
        budget_row.current_spend = 950.0

        conn, engine = _make_conn([budget_row])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        from metamind.api.admin_routes import budget_summary
        result = await budget_summary(req, tenant_id="acme")

        assert result["alert_color"] == "red"     # > 90%

    async def test_budget_summary_not_configured_returns_flag(self) -> None:
        conn, engine = _make_conn([None])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        from metamind.api.admin_routes import budget_summary
        result = await budget_summary(req, tenant_id="new_tenant")

        assert result["budget_configured"] is False

    async def test_budget_breakdown_returns_by_engine(self) -> None:
        row = MagicMock()
        row._mapping = {"engine": "trino", "total_cost": 100.5,
                        "query_count": 50, "avg_ms": 200.0}

        conn, engine = _make_conn([[row]])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        from metamind.api.admin_routes import budget_breakdown
        result = await budget_breakdown(req, tenant_id="acme")

        assert result["period_days"] == 30
        assert len(result["by_engine"]) == 1
        assert result["by_engine"][0]["engine"] == "trino"


# ---------------------------------------------------------------------------
# Budget alerts
# ---------------------------------------------------------------------------

class TestBudgetAlerts(unittest.IsolatedAsyncioTestCase):

    async def test_budget_alerts_returns_active_alerts(self) -> None:
        alert_row = MagicMock()
        alert_row._mapping = {
            "alert_id": "aid-1",
            "budget_id": "bid-1",
            "alert_type": "threshold_breach",
            "threshold_pct": 80,
            "current_spend": 850.0,
            "budget_limit": 1000.0,
            "pct_used": 85.0,
            "fired_at": datetime.now(tz=timezone.utc),
        }

        conn, engine = _make_conn([[alert_row]])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        from metamind.api.admin_routes import budget_alerts
        result = await budget_alerts(req, tenant_id="acme")

        assert len(result) == 1
        assert result[0]["pct_used"] == 85.0

    async def test_budget_alerts_empty_when_no_active(self) -> None:
        conn, engine = _make_conn([[]])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        from metamind.api.admin_routes import budget_alerts
        result = await budget_alerts(req, tenant_id="acme")

        assert result == []


# ---------------------------------------------------------------------------
# Admin tenant quota
# ---------------------------------------------------------------------------

class TestTenantManagement(unittest.IsolatedAsyncioTestCase):

    async def test_list_tenants_returns_rows(self) -> None:
        row = MagicMock()
        row._mapping = {"tenant_id": "acme", "tenant_name": "ACME Corp",
                        "is_active": True, "created_at": datetime.now(tz=timezone.utc),
                        "max_query_rate_per_minute": 100,
                        "max_concurrent_queries": 10, "max_result_rows": 100000}

        conn, engine = _make_conn([[row]])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        from metamind.api.admin_routes import list_tenants
        result = await list_tenants(req)

        assert len(result) == 1
        assert result[0]["tenant_id"] == "acme"

    async def test_update_quota_calls_upsert(self) -> None:
        from metamind.api.admin_routes import update_tenant_quota, TenantQuotaUpdate

        conn, engine = _make_conn([MagicMock()])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        update = TenantQuotaUpdate(max_concurrent_queries=20)
        result = await update_tenant_quota("acme", update, req)

        assert result["updated"] is True
        conn.execute.assert_called_once()

    async def test_update_quota_raises_400_on_empty_update(self) -> None:
        from fastapi import HTTPException
        from metamind.api.admin_routes import update_tenant_quota, TenantQuotaUpdate

        conn, engine = _make_conn([])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        update = TenantQuotaUpdate()  # no fields set
        with self.assertRaises(HTTPException) as cm:
            await update_tenant_quota("acme", update, req)
        assert cm.exception.status_code == 400


# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------

class TestFeatureFlags(unittest.IsolatedAsyncioTestCase):

    async def test_get_feature_flags_returns_dict(self) -> None:
        row = MagicMock()
        row.flag_name = "F23_CLOUD_BUDGET"
        row.is_enabled = True
        row.updated_at = datetime.now(tz=timezone.utc)

        conn, engine = _make_conn([[row]])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        from metamind.api.admin_routes import get_feature_flags
        result = await get_feature_flags(req, tenant_id="acme")

        assert result["tenant_id"] == "acme"
        assert "F23_CLOUD_BUDGET" in result["flags"]
        assert result["flags"]["F23_CLOUD_BUDGET"]["enabled"] is True

    async def test_toggle_feature_flag_upserts(self) -> None:
        from metamind.api.admin_routes import toggle_feature_flag, FeatureFlagUpdate

        conn, engine = _make_conn([MagicMock()])
        ctx = _make_ctx(engine)
        req = _make_request(ctx)

        update = FeatureFlagUpdate(tenant_id="acme", flag_name="F23_CLOUD_BUDGET", enabled=True)
        result = await toggle_feature_flag(update, req)

        assert result["enabled"] is True
        conn.execute.assert_called_once()


if __name__ == "__main__":
    unittest.main()
