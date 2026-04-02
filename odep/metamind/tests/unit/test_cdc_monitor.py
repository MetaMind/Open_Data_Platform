"""
Unit tests for CDC Monitor.
"""

from __future__ import annotations

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock

from metamind.core.cdc_monitor import CDCMonitor, CDCStatus


class TestCDCMonitor:
    """Test CDC monitoring functionality."""
    
    @pytest.fixture
    def monitor(self):
        """Create monitor with mocked engine."""
        engine = Mock()
        return CDCMonitor(engine=engine), engine
    
    def test_get_lag_found(self, monitor):
        """Test getting lag when status exists."""
        m, engine = monitor
        
        # Mock query result
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        
        result = Mock()
        result.lag_seconds = 120
        result.last_cdc_timestamp = datetime.now() - timedelta(seconds=120)
        conn.execute.return_value.fetchone.return_value = result
        
        lag = m.get_lag("orders", "s3_iceberg")
        
        assert lag == 120
    
    def test_get_lag_not_found(self, monitor):
        """Test getting lag when no status exists."""
        m, engine = monitor
        
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        conn.execute.return_value.fetchone.return_value = None
        
        lag = m.get_lag("unknown_table", "s3_iceberg")
        
        # Should return large number for unknown
        assert lag == 999999
    
    def test_is_healthy(self, monitor):
        """Test health check."""
        m, engine = monitor
        
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        
        # Healthy lag
        result = Mock()
        result.lag_seconds = 60
        conn.execute.return_value.fetchone.return_value = result
        
        assert m.is_healthy("orders", max_lag_seconds=300) is True
        
        # Unhealthy lag
        result.lag_seconds = 600
        assert m.is_healthy("orders", max_lag_seconds=300) is False
    
    def test_get_health_summary(self, monitor):
        """Test health summary."""
        m, engine = monitor
        
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        
        # Mock multiple tables
        results = [
            Mock(table_name="orders", lag_seconds=60, messages_behind=0, health_status="healthy"),
            Mock(table_name="customers", lag_seconds=400, messages_behind=10, health_status="warning"),
            Mock(table_name="products", lag_seconds=1200, messages_behind=50, health_status="critical"),
        ]
        conn.execute.return_value.fetchall.return_value = results
        
        summary = m.get_health_summary("default")
        
        assert summary["total_tables"] == 3
        assert summary["healthy"] == 1
        assert summary["warning"] == 1
        assert summary["critical"] == 1
        assert summary["overall_status"] == "unhealthy"
    
    def test_update_status(self, monitor):
        """Test status update."""
        m, engine = monitor
        
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        
        cdc_time = datetime.now() - timedelta(seconds=60)
        m.update_status(
            source_id="oracle_prod_orders",
            table_name="orders",
            cdc_timestamp=cdc_time,
            s3_timestamp=datetime.now(),
            messages_behind=0,
            tenant_id="default"
        )
        
        # Verify insert was called
        assert conn.execute.called
        assert conn.commit.called
