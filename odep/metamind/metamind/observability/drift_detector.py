"""
Model Drift Detector

File: metamind/observability/drift_detector.py
Role: ML Engineer
Phase: 1
Dependencies: numpy, scipy

Detects model drift and sends alerts:
- Data drift (feature distribution changes)
- Concept drift (prediction accuracy degradation)
- Performance drift (latency changes)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class DriftAlert:
    """Model drift alert."""
    alert_id: str
    model_id: str
    model_name: str
    drift_type: str  # data_drift, concept_drift, performance_drift
    
    drift_score: float
    is_significant: bool
    threshold: float
    
    affected_features: List[str]
    feature_drift_scores: Dict[str, float]
    
    detected_at: datetime
    window_start: datetime
    window_end: datetime
    
    # Impact
    accuracy_drop: Optional[float] = None
    latency_increase_percent: Optional[float] = None
    
    # Status
    status: str = "open"  # open, investigating, retraining, resolved
    resolution_notes: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "detected_at": self.detected_at.isoformat() if self.detected_at else None,
            "window_start": self.window_start.isoformat() if self.window_start else None,
            "window_end": self.window_end.isoformat() if self.window_end else None
        }


class DriftDetector:
    """
    Detects model drift and triggers alerts.
    
    Monitors:
    - Data drift: Feature distribution changes
    - Concept drift: Prediction accuracy degradation
    - Performance drift: Latency/throughput changes
    """
    
    def __init__(
        self,
        db_engine: Any,
        redis_client: Any,
        alert_callback: Optional[Callable[[DriftAlert], None]] = None
    ):
        """
        Initialize drift detector.
        
        Args:
            db_engine: Database engine
            redis_client: Redis client
            alert_callback: Callback for drift alerts
        """
        self.db_engine = db_engine
        self.redis = redis_client
        self.alert_callback = alert_callback
        
        # Drift thresholds
        self.data_drift_threshold = 0.1  # PSI threshold
        self.concept_drift_threshold = 0.15  # Accuracy drop threshold
        self.performance_drift_threshold = 0.2  # 20% latency increase
        
        logger.debug("DriftDetector initialized")
    
    def detect_data_drift(
        self,
        reference_distribution: Dict[str, np.ndarray],
        current_distribution: Dict[str, np.ndarray]
    ) -> Dict[str, float]:
        """
        Detect data drift using PSI (Population Stability Index).
        
        Args:
            reference_distribution: Reference feature distributions
            current_distribution: Current feature distributions
            
        Returns:
            Dict of feature -> drift score
        """
        drift_scores = {}
        
        for feature_name in reference_distribution:
            if feature_name not in current_distribution:
                continue
            
            ref_dist = reference_distribution[feature_name]
            curr_dist = current_distribution[feature_name]
            
            # Calculate PSI
            psi = self._calculate_psi(ref_dist, curr_dist)
            drift_scores[feature_name] = psi
        
        return drift_scores
    
    def _calculate_psi(
        self,
        expected: np.ndarray,
        actual: np.ndarray,
        buckets: int = 10
    ) -> float:
        """
        Calculate Population Stability Index.
        
        Args:
            expected: Expected distribution
            actual: Actual distribution
            buckets: Number of buckets
            
        Returns:
            PSI score
        """
        def scale_range(input_array: np.ndarray, min_val: float, max_val: float) -> np.ndarray:
            """Scale array to range."""
            input_array += -(np.min(input_array))
            input_array /= np.max(input_array) / (max_val - min_val)
            input_array += min_val
            return input_array
        
        breakpoints = np.linspace(0, buckets, buckets + 1)
        breakpoints = scale_range(breakpoints, np.min(expected), np.max(expected))
        
        expected_percents = np.histogram(expected, breakpoints)[0] / len(expected)
        actual_percents = np.histogram(actual, breakpoints)[0] / len(actual)
        
        def sub_psi(e_perc: float, a_perc: float) -> float:
            """Calculate PSI for a single bucket."""
            if a_perc == 0:
                a_perc = 0.0001
            if e_perc == 0:
                e_perc = 0.0001
            return (e_perc - a_perc) * np.log(e_perc / a_perc)
        
        psi_value = np.sum(
            sub_psi(expected_percents[i], actual_percents[i])
            for i in range(buckets)
        )
        
        return psi_value
    
    def detect_concept_drift(
        self,
        historical_errors: List[float],
        recent_errors: List[float]
    ) -> Dict[str, float]:
        """
        Detect concept drift by comparing error distributions.
        
        Args:
            historical_errors: Historical prediction errors
            recent_errors: Recent prediction errors
            
        Returns:
            Drift metrics
        """
        if not historical_errors or not recent_errors:
            return {"drift_detected": False, "error_increase": 0.0}
        
        hist_mean = np.mean(historical_errors)
        recent_mean = np.mean(recent_errors)
        
        error_increase = (recent_mean - hist_mean) / hist_mean if hist_mean > 0 else 0
        
        # Two-sample t-test
        from scipy import stats
        t_stat, p_value = stats.ttest_ind(historical_errors, recent_errors)
        
        drift_detected = p_value < 0.05 and error_increase > self.concept_drift_threshold
        
        return {
            "drift_detected": drift_detected,
            "error_increase": error_increase,
            "p_value": p_value,
            "t_statistic": t_stat
        }
    
    def detect_performance_drift(
        self,
        historical_latency: List[float],
        recent_latency: List[float]
    ) -> Dict[str, float]:
        """
        Detect performance drift by comparing latency distributions.
        
        Args:
            historical_latency: Historical latencies
            recent_latency: Recent latencies
            
        Returns:
            Drift metrics
        """
        if not historical_latency or not recent_latency:
            return {"drift_detected": False, "latency_increase": 0.0}
        
        hist_p95 = np.percentile(historical_latency, 95)
        recent_p95 = np.percentile(recent_latency, 95)
        
        latency_increase = (
            (recent_p95 - hist_p95) / hist_p95 if hist_p95 > 0 else 0
        )
        
        drift_detected = latency_increase > self.performance_drift_threshold
        
        return {
            "drift_detected": drift_detected,
            "latency_increase": latency_increase,
            "historical_p95": hist_p95,
            "recent_p95": recent_p95
        }
    
    async def check_model_drift(
        self,
        model_id: str,
        model_name: str,
        lookback_days: int = 7
    ) -> Optional[DriftAlert]:
        """
        Check for drift in a model.
        
        Args:
            model_id: Model identifier
            model_name: Model name
            lookback_days: Days to look back
            
        Returns:
            Drift alert if drift detected
        """
        import uuid
        
        # Get model predictions
        predictions = self._get_model_predictions(model_id, lookback_days)
        
        if len(predictions) < 100:
            logger.debug(f"Not enough predictions for drift detection: {len(predictions)}")
            return None
        
        # Split into historical and recent
        split_point = len(predictions) // 2
        historical = predictions[:split_point]
        recent = predictions[split_point:]
        
        # Check concept drift (prediction errors)
        hist_errors = [p.get("error", 0) for p in historical if p.get("error")]
        recent_errors = [p.get("error", 0) for p in recent if p.get("error")]
        
        concept_drift = self.detect_concept_drift(hist_errors, recent_errors)
        
        # Check performance drift (latency)
        hist_latency = [p.get("prediction_time_ms", 0) for p in historical]
        recent_latency = [p.get("prediction_time_ms", 0) for p in recent]
        
        perf_drift = self.detect_performance_drift(hist_latency, recent_latency)
        
        # Determine if drift is significant
        drift_detected = (
            concept_drift.get("drift_detected", False) or
            perf_drift.get("drift_detected", False)
        )
        
        if not drift_detected:
            return None
        
        # Create alert
        alert = DriftAlert(
            alert_id=str(uuid.uuid4()),
            model_id=model_id,
            model_name=model_name,
            drift_type="concept_drift" if concept_drift["drift_detected"] else "performance_drift",
            drift_score=max(
                concept_drift.get("error_increase", 0),
                perf_drift.get("latency_increase", 0)
            ),
            is_significant=True,
            threshold=self.concept_drift_threshold,
            affected_features=[],
            feature_drift_scores={},
            detected_at=datetime.now(),
            window_start=datetime.now() - timedelta(days=lookback_days),
            window_end=datetime.now(),
            accuracy_drop=concept_drift.get("error_increase"),
            latency_increase_percent=perf_drift.get("latency_increase")
        )
        
        # Store alert
        await self._store_alert(alert)
        
        # Send notification
        if self.alert_callback:
            self.alert_callback(alert)
        
        logger.warning(
            f"Drift detected for model {model_name}: "
            f"score={alert.drift_score:.3f}"
        )
        
        return alert
    
    def _get_model_predictions(
        self,
        model_id: str,
        lookback_days: int
    ) -> List[Dict[str, Any]]:
        """Get model predictions from database."""
        try:
            with self.db_engine.connect() as conn:
                results = conn.execute(
                    text("""
                    SELECT predicted_value, actual_value, error,
                           prediction_time_ms, created_at
                    FROM mm_model_predictions
                    WHERE model_id = :model_id
                    AND created_at > NOW() - INTERVAL ':days days'
                    ORDER BY created_at
                    """),
                    {"model_id": model_id, "days": lookback_days}
                ).fetchall()
                
                return [
                    {
                        "predicted": r.predicted_value,
                        "actual": r.actual_value,
                        "error": r.error,
                        "prediction_time_ms": r.prediction_time_ms
                    }
                    for r in results
                ]
        except Exception as e:
            logger.warning(f"Failed to get model predictions: {e}")
            return []
    
    async def _store_alert(self, alert: DriftAlert) -> None:
        """Store drift alert in database."""
        try:
            with self.db_engine.connect() as conn:
                conn.execute(
                    text("""
                    INSERT INTO mm_model_drift
                    (drift_id, model_id, drift_type, drift_score, is_significant,
                     affected_features, feature_drift_scores, accuracy_drop,
                     latency_increase_percent, detected_at, detection_window_start,
                     detection_window_end, status)
                    VALUES (:drift_id, :model_id, :drift_type, :drift_score, :is_significant,
                            :affected_features, :feature_drift_scores, :accuracy_drop,
                            :latency_increase, :detected_at, :window_start,
                            :window_end, 'open')
                    """),
                    {
                        "drift_id": alert.alert_id,
                        "model_id": alert.model_id,
                        "drift_type": alert.drift_type,
                        "drift_score": alert.drift_score,
                        "is_significant": alert.is_significant,
                        "affected_features": alert.affected_features,
                        "feature_drift_scores": json.dumps(alert.feature_drift_scores),
                        "accuracy_drop": alert.accuracy_drop,
                        "latency_increase": alert.latency_increase_percent,
                        "detected_at": alert.detected_at,
                        "window_start": alert.window_start,
                        "window_end": alert.window_end
                    }
                )
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to store drift alert: {e}")
    
    async def get_open_alerts(self) -> List[DriftAlert]:
        """Get all open drift alerts."""
        try:
            with self.db_engine.connect() as conn:
                results = conn.execute(
                    text("""
                    SELECT d.*, m.model_name
                    FROM mm_model_drift d
                    JOIN mm_learned_models m ON d.model_id = m.model_id
                    WHERE d.status = 'open'
                    ORDER BY d.detected_at DESC
                    """)
                ).fetchall()
                
                return [
                    DriftAlert(
                        alert_id=r.drift_id,
                        model_id=r.model_id,
                        model_name=r.model_name,
                        drift_type=r.drift_type,
                        drift_score=r.drift_score,
                        is_significant=r.is_significant,
                        affected_features=r.affected_features or [],
                        feature_drift_scores=r.feature_drift_scores or {},
                        detected_at=r.detected_at,
                        window_start=r.detection_window_start,
                        window_end=r.detection_window_end,
                        accuracy_drop=r.accuracy_drop,
                        latency_increase_percent=r.latency_increase_percent,
                        status=r.status
                    )
                    for r in results
                ]
        except Exception as e:
            logger.warning(f"Failed to get open alerts: {e}")
            return []
