"""
Unit tests for Query Router.

Tests routing logic with mocked dependencies.
"""

from __future__ import annotations

import pytest
from unittest.mock import Mock, MagicMock

from metamind.core.router import QueryRouter, ExecutionStrategy


class TestQueryRouter:
    """Test query routing logic."""
    
    @pytest.fixture
    def router(self):
        """Create router with mocked dependencies."""
        catalog = Mock()
        cdc_monitor = Mock()
        cost_model = Mock()
        cache_manager = Mock()
        settings = Mock()
        
        # Default settings
        settings.cache = Mock()
        settings.cache.l1_enabled = True
        settings.cache.l1_ttl_seconds = 300
        settings.cache.include_user_context = True
        settings.cache.fingerprint_algorithm = "sha256"
        
        router = QueryRouter(
            catalog=catalog,
            cdc_monitor=cdc_monitor,
            cost_model=cost_model,
            cache_manager=cache_manager,
            settings=settings
        )
        
        return router, {
            "catalog": catalog,
            "cdc": cdc_monitor,
            "cost_model": cost_model,
            "cache": cache_manager
        }
    
    @pytest.mark.asyncio
    async def test_route_simple_select(self, router):
        """Test routing simple SELECT query."""
        r, mocks = router
        
        # Mock CDC lag - fresh enough
        mocks["cdc"].get_lag.return_value = 60  # 60 seconds lag
        
        # Mock cost model
        mocks["cost_model"].predict.return_value = 150.0
        
        # Mock cache miss
        mocks["cache"].get.return_value = None
        
        decision = await r.route(
            sql="SELECT * FROM orders LIMIT 100",
            tenant_id="test",
            user_context={"freshness_tolerance_seconds": 300}
        )
        
        assert decision.target_source_type in ["trino", "oracle"]
        assert decision.execution_strategy == ExecutionStrategy.DIRECT
        assert decision.confidence > 0
    
    @pytest.mark.asyncio
    async def test_route_realtime_requirement(self, router):
        """Test routing with realtime freshness requirement."""
        r, mocks = router
        
        # Mock cache miss
        mocks["cache"].get.return_value = None
        
        decision = await r.route(
            sql="SELECT * FROM orders WHERE order_id = '123'",
            tenant_id="test",
            user_context={"freshness_tolerance_seconds": 0}  # Realtime
        )
        
        # Should route to Oracle for realtime
        assert decision.target_source_type == "oracle"
        assert decision.freshness_expected_seconds == 0
    
    def test_extract_tables(self, router):
        """Test table extraction from SQL."""
        r, _ = router
        
        import sqlglot
        parsed = sqlglot.parse_one("SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id")
        tables = r._extract_tables(parsed)
        
        assert "orders" in tables
        assert "customers" in tables
    
    def test_extract_features(self, router):
        """Test feature extraction."""
        r, _ = router
        
        import sqlglot
        parsed = sqlglot.parse_one("""
            SELECT o.id, COUNT(*) 
            FROM orders o 
            JOIN customers c ON o.customer_id = c.id 
            WHERE o.status = 'active'
            GROUP BY o.id
        """)
        
        tables = ["orders", "customers"]
        features = r._extract_features(parsed, tables)
        
        assert features["num_tables"] == 2
        assert features["has_join"] is True
        assert features["has_where"] is True
        assert features["has_group_by"] is True
        assert features["has_aggregate"] is True
    
    def test_infer_freshness(self, router):
        """Test freshness inference."""
        r, _ = router
        
        import sqlglot
        
        # Query with NOW() - needs recent data
        parsed = sqlglot.parse_one("SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '1 hour'")
        freshness = r._infer_freshness(parsed, {})
        assert freshness == r.FRESHNESS_LEVELS["recent"]
        
        # Query with explicit freshness
        freshness = r._infer_freshness(parsed, {"freshness_tolerance_seconds": 60})
        assert freshness == 60
    
    def test_compute_cache_key(self, router):
        """Test cache key computation."""
        r, _ = router
        
        key1 = r._compute_cache_key("SELECT * FROM orders", "tenant1")
        key2 = r._compute_cache_key("SELECT * FROM orders", "tenant1")
        key3 = r._compute_cache_key("SELECT * FROM customers", "tenant1")
        
        # Same query = same key
        assert key1 == key2
        # Different query = different key
        assert key1 != key3
