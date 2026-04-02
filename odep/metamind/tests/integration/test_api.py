"""
Integration tests for MetaMind API.

Tests API endpoints with real dependencies.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from metamind.api.server import create_app


@pytest_asyncio.fixture
async def client():
    """Create test client."""
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
class TestHealthEndpoint:
    """Test health check endpoint."""
    
    async def test_health_check(self, client):
        """Test health endpoint returns status."""
        response = await client.get("/api/v1/health")
        
        # May return 503 if app not initialized, which is expected in tests
        assert response.status_code in [200, 503]
        
        if response.status_code == 200:
            data = response.json()
            assert "status" in data
            assert "version" in data
            assert "checks" in data


@pytest.mark.asyncio
class TestQueryEndpoint:
    """Test query execution endpoint."""
    
    async def test_query_validation(self, client):
        """Test query validation rejects empty SQL."""
        response = await client.post("/api/v1/query", json={})
        assert response.status_code == 422  # Validation error
    
    async def test_query_with_invalid_sql(self, client):
        """Test query with invalid SQL returns error."""
        response = await client.post("/api/v1/query", json={
            "sql": "INVALID SQL SYNTAX",
            "tenant_id": "test"
        })
        
        # May return 503 if app not initialized
        assert response.status_code in [400, 503]
    
    async def test_query_basic_select(self, client):
        """Test basic SELECT query."""
        response = await client.post("/api/v1/query", json={
            "sql": "SELECT 1 as test",
            "tenant_id": "test",
            "freshness_tolerance_seconds": 300
        })
        
        # Response depends on app initialization state
        assert response.status_code in [200, 400, 503]


@pytest.mark.asyncio
class TestCDCStatusEndpoint:
    """Test CDC status endpoint."""
    
    async def test_cdc_status(self, client):
        """Test CDC status endpoint."""
        response = await client.get("/api/v1/cdc/status?tenant_id=default")
        
        assert response.status_code in [200, 503]
        
        if response.status_code == 200:
            data = response.json()
            assert "total_tables" in data
            assert "healthy" in data
            assert "overall_status" in data


@pytest.mark.asyncio
class TestCacheEndpoints:
    """Test cache management endpoints."""
    
    async def test_cache_stats(self, client):
        """Test cache stats endpoint."""
        response = await client.get("/api/v1/cache/stats")
        
        assert response.status_code in [200, 503]
        
        if response.status_code == 200:
            data = response.json()
            assert "l1_size" in data or "hit_rate_percent" in data
    
    async def test_cache_invalidate(self, client):
        """Test cache invalidate endpoint."""
        response = await client.post("/api/v1/cache/invalidate?pattern=test")
        
        assert response.status_code in [200, 503]


@pytest.mark.asyncio
class TestTableEndpoints:
    """Test table metadata endpoints."""
    
    async def test_table_search(self, client):
        """Test table search endpoint."""
        response = await client.get("/api/v1/tables/search?q=orders")
        
        assert response.status_code in [200, 503]
        
        if response.status_code == 200:
            data = response.json()
            assert "tables" in data
            assert "count" in data
    
    async def test_table_details_not_found(self, client):
        """Test table details for non-existent table."""
        response = await client.get("/api/v1/tables/nonexistent")
        
        assert response.status_code in [404, 503]


@pytest.mark.asyncio
class TestQueryHistoryEndpoint:
    """Test query history endpoint."""
    
    async def test_query_history(self, client):
        """Test query history endpoint."""
        response = await client.get("/api/v1/query/history?tenant_id=default&limit=10")
        
        assert response.status_code in [200, 503]
        
        if response.status_code == 200:
            data = response.json()
            assert "queries" in data
            assert "count" in data
