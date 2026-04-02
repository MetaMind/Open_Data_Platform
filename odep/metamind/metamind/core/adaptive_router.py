"""
Adaptive Router - CDC Lag Aware Routing

File: metamind/core/adaptive_router.py
Role: ML Engineer
Phase: 1
Dependencies: CDC Monitor, Feature Store

Implements CDC lag adaptive routing:
- Dynamic routing based on real-time CDC lag
- Predictive routing based on lag trends
- Fallback routing when CDC is unhealthy
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class CDCLagTrend:
    """CDC lag trend information."""
    table_name: str
    current_lag_seconds: int
    lag_trend: str  # increasing, decreasing, stable
    avg_lag_5min: float
    avg_lag_1hour: float
    predicted_lag_5min: int
    last_updated: datetime


@dataclass
class AdaptiveRoutingDecision:
    """Adaptive routing decision."""
    table_name: str
    recommended_source: str  # oracle, s3
    confidence: float
    reason: str
    current_lag: int
    lag_trend: str
    freshness_guarantee_seconds: int


class CDCLagAdaptiveRouter:
    """
    CDC lag adaptive router.
    
    Dynamically adjusts routing decisions based on:
    - Current CDC lag
    - Lag trends
    - Predicted future lag
    """
    
    # Lag thresholds
    HEALTHY_LAG_SECONDS = 300      # 5 minutes
    WARNING_LAG_SECONDS = 600      # 10 minutes
    CRITICAL_LAG_SECONDS = 1800    # 30 minutes
    
    def __init__(
        self,
        cdc_monitor: Any,
        redis_client: Any,
        trend_window_minutes: int = 5
    ):
        """
        Initialize adaptive router.
        
        Args:
            cdc_monitor: CDC monitor
            redis_client: Redis client
            trend_window_minutes: Window for trend calculation
        """
        self.cdc_monitor = cdc_monitor
        self.redis = redis_client
        self.trend_window = trend_window_minutes
        self._lag_history: Dict[str, List[tuple]] = {}  # table -> [(timestamp, lag)]
        logger.debug("CDCLagAdaptiveRouter initialized")
    
    async def get_lag_trend(self, table_name: str) -> Optional[CDCLagTrend]:
        """
        Get CDC lag trend for a table.
        
        Args:
            table_name: Table name
            
        Returns:
            Lag trend information
        """
        # Get current lag
        current_lag = self.cdc_monitor.get_lag(table_name, "s3_iceberg")
        
        # Get historical data
        history = await self._get_lag_history(table_name)
        
        if not history:
            return CDCLagTrend(
                table_name=table_name,
                current_lag_seconds=current_lag,
                lag_trend="unknown",
                avg_lag_5min=current_lag,
                avg_lag_1hour=current_lag,
                predicted_lag_5min=current_lag,
                last_updated=datetime.now()
            )
        
        # Calculate averages
        now = datetime.now()
        lag_5min = [
            lag for ts, lag in history
            if now - ts <= timedelta(minutes=5)
        ]
        lag_1hour = [
            lag for ts, lag in history
            if now - ts <= timedelta(hours=1)
        ]
        
        avg_5min = sum(lag_5min) / len(lag_5min) if lag_5min else current_lag
        avg_1hour = sum(lag_1hour) / len(lag_1hour) if lag_1hour else current_lag
        
        # Determine trend
        trend = self._calculate_trend(lag_5min)
        
        # Predict future lag (simple linear extrapolation)
        predicted_lag = self._predict_lag(history, minutes_ahead=5)
        
        return CDCLagTrend(
            table_name=table_name,
            current_lag_seconds=current_lag,
            lag_trend=trend,
            avg_lag_5min=avg_5min,
            avg_lag_1hour=avg_1hour,
            predicted_lag_5min=predicted_lag,
            last_updated=datetime.now()
        )
    
    async def _get_lag_history(
        self,
        table_name: str
    ) -> List[tuple]:
        """Get lag history for a table."""
        # Try Redis first
        cache_key = f"cdc_lag_history:{table_name}"
        cached = await self.redis.get(cache_key)
        
        if cached:
            import json
            data = json.loads(cached)
            return [
                (datetime.fromisoformat(ts), lag)
                for ts, lag in data
            ]
        
        return []
    
    async def record_lag(
        self,
        table_name: str,
        lag_seconds: int
    ) -> None:
        """Record lag measurement for trend calculation."""
        import json
        
        # Get existing history
        history = await self._get_lag_history(table_name)
        
        # Add new measurement
        history.append((datetime.now(), lag_seconds))
        
        # Keep only last hour
        cutoff = datetime.now() - timedelta(hours=1)
        history = [(ts, lag) for ts, lag in history if ts > cutoff]
        
        # Store
        cache_key = f"cdc_lag_history:{table_name}"
        await self.redis.setex(
            cache_key,
            3600,  # 1 hour TTL
            json.dumps([(ts.isoformat(), lag) for ts, lag in history])
        )
    
    def _calculate_trend(self, lags: List[int]) -> str:
        """Calculate lag trend from recent measurements."""
        if len(lags) < 3:
            return "stable"
        
        # Simple trend: compare first half to second half
        mid = len(lags) // 2
        first_half_avg = sum(lags[:mid]) / mid if mid > 0 else 0
        second_half_avg = sum(lags[mid:]) / (len(lags) - mid) if len(lags) > mid else 0
        
        diff = second_half_avg - first_half_avg
        threshold = first_half_avg * 0.1  # 10% change threshold
        
        if diff > threshold:
            return "increasing"
        elif diff < -threshold:
            return "decreasing"
        else:
            return "stable"
    
    def _predict_lag(
        self,
        history: List[tuple],
        minutes_ahead: int = 5
    ) -> int:
        """Predict future lag using linear regression."""
        if len(history) < 2:
            return history[-1][1] if history else 0
        
        try:
            import numpy as np
            
            # Convert to numpy arrays
            times = np.array([
                (ts - history[0][0]).total_seconds()
                for ts, _ in history
            ])
            lags = np.array([lag for _, lag in history])
            
            # Linear regression
            A = np.vstack([times, np.ones(len(times))]).T
            slope, intercept = np.linalg.lstsq(A, lags, rcond=None)[0]
            
            # Predict
            future_time = times[-1] + (minutes_ahead * 60)
            predicted = slope * future_time + intercept
            
            return max(0, int(predicted))
            
        except Exception as e:
            logger.warning(f"Failed to predict lag: {e}")
            return history[-1][1] if history else 0
    
    async def get_adaptive_routing_decision(
        self,
        table_name: str,
        freshness_tolerance_seconds: int
    ) -> AdaptiveRoutingDecision:
        """
        Get adaptive routing decision for a table.
        
        Args:
            table_name: Table name
            freshness_tolerance_seconds: Required freshness
            
        Returns:
            Adaptive routing decision
        """
        trend = await self.get_lag_trend(table_name)
        current_lag = trend.current_lag_seconds
        predicted_lag = trend.predicted_lag_5min
        
        # Decision logic
        if current_lag <= freshness_tolerance_seconds:
            # CDC is fresh enough
            if trend.lag_trend == "increasing" and predicted_lag > freshness_tolerance_seconds:
                # Lag is increasing and will exceed tolerance soon
                return AdaptiveRoutingDecision(
                    table_name=table_name,
                    recommended_source="oracle",
                    confidence=0.7,
                    reason=f"CDC lag increasing ({current_lag}s -> predicted {predicted_lag}s)",
                    current_lag=current_lag,
                    lag_trend=trend.lag_trend,
                    freshness_guarantee_seconds=0
                )
            else:
                # CDC is good and stable/improving
                return AdaptiveRoutingDecision(
                    table_name=table_name,
                    recommended_source="s3",
                    confidence=0.9,
                    reason=f"CDC lag acceptable ({current_lag}s, trend: {trend.lag_trend})",
                    current_lag=current_lag,
                    lag_trend=trend.lag_trend,
                    freshness_guarantee_seconds=current_lag
                )
        else:
            # CDC lag exceeds tolerance
            if trend.lag_trend == "decreasing":
                # Lag is improving, might be acceptable soon
                if predicted_lag <= freshness_tolerance_seconds:
                    return AdaptiveRoutingDecision(
                        table_name=table_name,
                        recommended_source="s3",
                        confidence=0.6,
                        reason=f"CDC lag high but improving ({current_lag}s -> predicted {predicted_lag}s)",
                        current_lag=current_lag,
                        lag_trend=trend.lag_trend,
                        freshness_guarantee_seconds=predicted_lag
                    )
            
            # CDC lag too high and not improving
            return AdaptiveRoutingDecision(
                table_name=table_name,
                recommended_source="oracle",
                confidence=0.95,
                reason=f"CDC lag exceeds tolerance ({current_lag}s > {freshness_tolerance_seconds}s)",
                current_lag=current_lag,
                lag_trend=trend.lag_trend,
                freshness_guarantee_seconds=0
            )
    
    async def should_use_hybrid(
        self,
        table_name: str,
        freshness_tolerance_seconds: int,
        query_time_range_hours: Optional[int] = None
    ) -> bool:
        """
        Determine if hybrid routing (Oracle + S3) should be used.
        
        Args:
            table_name: Table name
            freshness_tolerance_seconds: Required freshness
            query_time_range_hours: Query time range if known
            
        Returns:
            True if hybrid routing recommended
        """
        trend = await self.get_lag_trend(table_name)
        
        # Hybrid makes sense when:
        # 1. CDC lag is moderate (5-30 min)
        # 2. Query spans both recent and historical data
        # 3. Query is complex enough to benefit from S3 for historical
        
        if trend.current_lag_seconds <= freshness_tolerance_seconds:
            return False  # CDC is fresh enough
        
        if trend.current_lag_seconds > self.CRITICAL_LAG_SECONDS:
            return False  # CDC is too stale, use Oracle entirely
        
        # CDC is in warning zone - hybrid might help
        if query_time_range_hours and query_time_range_hours > 24:
            return True  # Long time range benefits from hybrid
        
        return False
    
    async def get_routing_recommendation(
        self,
        tables: List[str],
        freshness_tolerance_seconds: int
    ) -> Dict[str, Any]:
        """
        Get routing recommendation for multiple tables.
        
        Args:
            tables: List of table names
            freshness_tolerance_seconds: Required freshness
            
        Returns:
            Routing recommendation
        """
        decisions = []
        
        for table in tables:
            decision = await self.get_adaptive_routing_decision(
                table, freshness_tolerance_seconds
            )
            decisions.append(decision)
        
        # Aggregate decisions
        oracle_tables = [d.table_name for d in decisions if d.recommended_source == "oracle"]
        s3_tables = [d.table_name for d in decisions if d.recommended_source == "s3"]
        
        # Overall recommendation
        if len(oracle_tables) == len(tables):
            overall_source = "oracle"
            confidence = min(d.confidence for d in decisions)
        elif len(s3_tables) == len(tables):
            overall_source = "s3"
            confidence = min(d.confidence for d in decisions)
        else:
            overall_source = "hybrid"
            confidence = 0.7
        
        return {
            "recommended_source": overall_source,
            "confidence": confidence,
            "oracle_tables": oracle_tables,
            "s3_tables": s3_tables,
            "table_decisions": [{
                "table": d.table_name,
                "source": d.recommended_source,
                "confidence": d.confidence,
                "reason": d.reason,
                "current_lag": d.current_lag,
                "lag_trend": d.lag_trend
            } for d in decisions]
        }
