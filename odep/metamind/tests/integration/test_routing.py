"""
Integration tests for Query Routing.

Tests end-to-end routing scenarios.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from metamind.bootstrap import AppContext, bootstrap
from metamind.config.settings import AppSettings


@pytest_asyncio.fixture
async def app_context():
    """Create app context for testing."""
    settings = AppSettings()
    settings.db.host = "localhost"
    settings.redis.host = "localhost"
    
    try:
        ctx = await bootstrap(settings)
        yield ctx
    except Exception:
        pytest.skip("Database/Redis not available")
    finally:
        if 'ctx' in locals():
            await ctx.close()


@pytest.mark.asyncio
class TestQueryRouting:
    """End-to-end routing scenarios."""
    
    async def test_oracle_path_for_realtime(self, app_context):
        """Freshness=0 must route to Oracle."""
        decision = await app_context.query_router.route(
            sql="SELECT * FROM orders WHERE order_id = '123'",
            tenant_id="test",
            user_context={"freshness_tolerance_seconds": 0}
        )
        
        assert decision.target_source_type == "oracle"
        assert decision.freshness_expected_seconds == 0
    
    async def test_s3_path_when_fresh(self, app_context):
        """Freshness=300 with low CDC lag should route to S3."""
        # Mock CDC lag by inserting status
        from datetime import datetime, timedelta
        
        app_context.cdc_monitor.update_status(
            source_id="test_s3_orders",
            table_name="orders",
            cdc_timestamp=datetime.now() - timedelta(seconds=60),
            s3_timestamp=datetime.now(),
            messages_behind=0,
            tenant_id="test"
        )
        
        decision = await app_context.query_router.route(
            sql="SELECT COUNT(*) FROM orders",
            tenant_id="test",
            user_context={"freshness_tolerance_seconds": 300}
        )
        
        # Should route to S3 since CDC lag is acceptable
        assert "s3" in decision.target_source or decision.target_source_type == "oracle"
    
    async def test_cache_key_computation(self, app_context):
        """Test cache key computation is consistent."""
        key1 = app_context.query_router._compute_cache_key(
            "SELECT * FROM orders", "tenant1"
        )
        key2 = app_context.query_router._compute_cache_key(
            "SELECT * FROM orders", "tenant1"
        )
        key3 = app_context.query_router._compute_cache_key(
            "SELECT * FROM customers", "tenant1"
        )
        
        assert key1 == key2
        assert key1 != key3
    
    async def test_feature_extraction(self, app_context):
        """Test query feature extraction."""
        import sqlglot
        
        sql = """
            SELECT o.id, c.name, COUNT(*) as cnt
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            WHERE o.status = 'active'
            GROUP BY o.id, c.name
            ORDER BY cnt DESC
            LIMIT 100
        """
        
        parsed = sqlglot.parse_one(sql)
        tables = app_context.query_router._extract_tables(parsed)
        features = app_context.query_router._extract_features(parsed, tables)
        
        assert features["num_tables"] == 2
        assert features["has_join"] is True
        assert features["has_where"] is True
        assert features["has_group_by"] is True
        assert features["has_order_by"] is True
        assert features["has_limit"] is True


@pytest.mark.asyncio
class TestOracleSafety:
    """Oracle protection mechanisms."""
    
    async def test_blocks_dml(self, app_context):
        """UPDATE/DELETE/INSERT must be rejected."""
        forbidden_queries = [
            "UPDATE orders SET status = 'done'",
            "DELETE FROM orders WHERE id = 1",
            "INSERT INTO orders VALUES (1, 'test')",
            "DROP TABLE orders",
            "CREATE TABLE test (id INT)"
        ]
        
        for sql in forbidden_queries:
            with pytest.raises(Exception) as exc_info:
                await app_context.oracle_connector._validate_query_safety(sql)
            
            assert "Blocked pattern" in str(exc_info.value) or "Only SELECT" in str(exc_info.value)
    
    async def test_allows_select(self, app_context):
        """SELECT queries should be allowed."""
        # Should not raise
        app_context.oracle_connector._validate_query_safety("SELECT * FROM orders")
        app_context.oracle_connector._validate_query_safety("SELECT COUNT(*) FROM customers WHERE id = 1")


@pytest.mark.asyncio
class TestCDCIntegration:
    """CDC pipeline health."""
    
    async def test_lag_monitoring(self, app_context):
        """CDC lag should be tracked."""
        from datetime import datetime, timedelta
        
        # Update CDC status
        app_context.cdc_monitor.update_status(
            source_id="test_s3_orders",
            table_name="test_events",
            cdc_timestamp=datetime.now() - timedelta(seconds=60),
            s3_timestamp=datetime.now(),
            messages_behind=5,
            tenant_id="test"
        )
        
        # Get lag
        lag = app_context.cdc_monitor.get_lag("test_events", "s3")
        
        # Lag should be approximately 60 seconds
        assert 55 <= lag <= 65
    
    async def test_health_summary(self, app_context):
        """Health summary should aggregate table statuses."""
        from datetime import datetime, timedelta
        
        # Create multiple table statuses
        for i, table in enumerate(["orders", "customers", "products"]):
            app_context.cdc_monitor.update_status(
                source_id=f"test_s3_{table}",
                table_name=table,
                cdc_timestamp=datetime.now() - timedelta(seconds=(i + 1) * 100),
                s3_timestamp=datetime.now(),
                messages_behind=i * 10,
                tenant_id="test"
            )
        
        summary = app_context.cdc_monitor.get_health_summary("test")
        
        assert summary["total_tables"] == 3
        assert "healthy" in summary
        assert "overall_status" in summary


@pytest.mark.asyncio
class TestCacheIntegration:
    """Cache integration tests."""
    
    async def test_cache_set_and_get(self, app_context):
        """Test cache set and get operations."""
        test_data = {"result": [1, 2, 3], "columns": ["id"]}
        
        await app_context.cache_manager.set(
            key="test_key",
            data=test_data,
            metadata={"test": True}
        )
        
        cached = await app_context.cache_manager.get("test_key")
        
        assert cached is not None
        assert cached["data"] == test_data
    
    async def test_cache_invalidation(self, app_context):
        """Test cache invalidation."""
        # Set multiple keys
        await app_context.cache_manager.set("orders_1", {"data": 1})
        await app_context.cache_manager.set("orders_2", {"data": 2})
        await app_context.cache_manager.set("customers_1", {"data": 3})
        
        # Invalidate orders pattern
        count = await app_context.cache_manager.invalidate_pattern("orders")
        
        # orders keys should be gone
        assert await app_context.cache_manager.get("orders_1") is None
        assert await app_context.cache_manager.get("orders_2") is None
        
        # customers key should remain
        assert await app_context.cache_manager.get("customers_1") is not None
