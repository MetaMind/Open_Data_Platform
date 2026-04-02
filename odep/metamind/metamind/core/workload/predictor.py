"""F26 — Predictive Pre-Warming with temporal and sequence pattern detection."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional

import sqlalchemy as sa
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from metamind.core.cache.plan_cache import PlanCache
    from metamind.core.query_engine import QueryEngine
    from metamind.core.workload.queue import QueryQueue

logger = logging.getLogger(__name__)

_MIN_CONFIDENCE = 0.7
_SEQUENCE_WINDOW_MINUTES = 2
_MAX_PREDICTIONS = 5
_MIN_OCCURRENCES = 3

_LITERAL_PATTERN = re.compile(r"'[^']*'|\"[^\"]*\"|\b\d+(?:\.\d+)?\b")


def _normalize_template(sql: str) -> str:
    normalized = sql.strip().upper()
    normalized = re.sub(r"\s+", " ", normalized)
    return _LITERAL_PATTERN.sub("?", normalized)


def _template_hash(template: str) -> str:
    return hashlib.sha256(template.encode()).hexdigest()[:16]


@dataclass
class QueryTrace:
    query_id: str
    tenant_id: str
    sql: str
    executed_at: datetime
    duration_ms: float
    backend: str = "unknown"


@dataclass
class PredictedQuery:
    sql_template: str
    expected_params: list[object]
    predicted_execution_time: datetime
    confidence: float
    expected_rows: int
    prediction_type: str = "temporal"


class QueryPredictor:
    """F26: Predicts which queries will run next based on temporal patterns."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def predict_next(
        self,
        tenant_id: str,
        current_time: datetime,
        recent_history: list[QueryTrace],
    ) -> list[PredictedQuery]:
        predictions: list[PredictedQuery] = []
        seasonal = self.detect_seasonality(recent_history)
        current_hour = current_time.hour
        current_dow = current_time.strftime("%a").upper()

        for template, info in seasonal.items():
            hour = info.get("hour")
            confidence = float(info.get("confidence", 0.0))
            day = info.get("day")
            if confidence < _MIN_CONFIDENCE:
                continue
            day_match = day is None or day == current_dow
            hour_match = hour is not None and abs(int(hour) - current_hour) <= 0
            if hour_match and day_match:
                predicted_time = current_time.replace(
                    minute=0, second=0, microsecond=0
                ) + timedelta(minutes=5)
                predictions.append(PredictedQuery(
                    sql_template=template,
                    expected_params=[],
                    predicted_execution_time=predicted_time,
                    confidence=confidence,
                    expected_rows=int(info.get("expected_rows", 1000)),
                    prediction_type="temporal",
                ))

        if recent_history:
            recent_templates = [_normalize_template(t.sql) for t in recent_history[-5:]]
            sequences = self.detect_sequences(recent_history)
            for seq in sequences:
                if seq.get("antecedent") in recent_templates:
                    conf = float(seq.get("probability", 0.0))
                    if conf >= _MIN_CONFIDENCE:
                        predictions.append(PredictedQuery(
                            sql_template=seq["consequent"],
                            expected_params=[],
                            predicted_execution_time=current_time + timedelta(minutes=_SEQUENCE_WINDOW_MINUTES),
                            confidence=conf,
                            expected_rows=1000,
                            prediction_type="sequence",
                        ))

        predictions.sort(key=lambda p: p.confidence, reverse=True)
        return predictions[:_MAX_PREDICTIONS]

    def detect_seasonality(
        self, query_history: list[QueryTrace]
    ) -> dict[str, dict[str, object]]:
        if not query_history:
            return {}
        buckets: dict[str, dict[tuple[int, str], list[datetime]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for trace in query_history:
            template = _normalize_template(trace.sql)
            hour = trace.executed_at.hour
            dow = trace.executed_at.strftime("%a").upper()
            buckets[template][(hour, dow)].append(trace.executed_at)

        result: dict[str, dict[str, object]] = {}
        for template, time_buckets in buckets.items():
            best_bucket: Optional[tuple[int, str]] = None
            best_count = 0
            for (hour, dow), timestamps in time_buckets.items():
                if len(timestamps) > best_count:
                    best_count = len(timestamps)
                    best_bucket = (hour, dow)
            if best_bucket is None or best_count < _MIN_OCCURRENCES:
                continue
            hour, dow = best_bucket
            total_for_template = sum(len(ts) for ts in time_buckets.values())
            frequency = best_count / total_for_template
            all_timestamps = time_buckets[best_bucket]
            if len(all_timestamps) >= 2:
                week_counts: dict[int, int] = defaultdict(int)
                for ts in all_timestamps:
                    week_counts[ts.isocalendar()[1]] += 1
                counts = list(week_counts.values())
                mean = sum(counts) / len(counts)
                variance = sum((c - mean) ** 2 for c in counts) / len(counts)
                stddev = math.sqrt(variance)
                confidence = min(0.99, frequency * max(0.0, 1.0 - stddev / (mean + 1)))
            else:
                confidence = frequency * 0.5
            result[template] = {
                "hour": hour,
                "day": dow,
                "frequency": round(frequency, 3),
                "confidence": round(confidence, 3),
                "expected_rows": 1000,
            }
        return result

    def detect_sequences(self, query_history: list[QueryTrace]) -> list[dict]:
        if len(query_history) < 2:
            return []
        sorted_history = sorted(query_history, key=lambda t: t.executed_at)
        templates = [_normalize_template(t.sql) for t in sorted_history]
        times = [t.executed_at for t in sorted_history]
        pair_counts: dict[tuple[str, str], int] = defaultdict(int)
        antecedent_counts: dict[str, int] = defaultdict(int)
        for i, template_a in enumerate(templates):
            antecedent_counts[template_a] += 1
            for j in range(i + 1, len(templates)):
                delta = (times[j] - times[i]).total_seconds() / 60
                if delta > _SEQUENCE_WINDOW_MINUTES:
                    break
                template_b = templates[j]
                if template_b != template_a:
                    pair_counts[(template_a, template_b)] += 1
        sequences = []
        for (template_a, template_b), count in pair_counts.items():
            total_a = antecedent_counts.get(template_a, 1)
            probability = count / total_a
            if probability >= _MIN_CONFIDENCE and count >= _MIN_OCCURRENCES:
                sequences.append({
                    "antecedent": template_a,
                    "consequent": template_b,
                    "probability": round(probability, 3),
                    "count": count,
                })
        sequences.sort(key=lambda s: s["probability"], reverse=True)
        return sequences

    def confidence_score(self, pattern: dict) -> float:
        freq = float(pattern.get("frequency", 0.5))
        regularity = float(pattern.get("confidence", 0.5))
        recency = float(pattern.get("recency_weight", 1.0))
        return min(0.99, freq * regularity * recency)

    def persist_patterns(self, tenant_id: str, patterns: list[dict]) -> None:
        if not patterns:
            return
        try:
            with self._engine.begin() as conn:
                for pattern in patterns:
                    template = pattern.get("template", "")
                    hour = pattern.get("hour")
                    day = pattern.get("day")
                    confidence = float(pattern.get("confidence", 0.0))
                    thash = _template_hash(template)
                    conn.execute(
                        sa.text(
                            "DELETE FROM mm_workload_patterns "
                            "WHERE tenant_id = :tid AND query_template_hash = :thash"
                        ),
                        {"tid": tenant_id, "thash": thash},
                    )
                    conn.execute(
                        sa.text(
                            "INSERT INTO mm_workload_patterns "
                            "(tenant_id, query_template_hash, query_template, "
                            " predicted_execution_time, confidence_score, "
                            " day_of_week, hour_of_day, created_at) "
                            "VALUES (:tid, :thash, :template, :pet, :conf, :dow, :hod, :now)"
                        ),
                        {
                            "tid": tenant_id,
                            "thash": thash,
                            "template": template,
                            "pet": f"{day or '*'} {hour or 0}:00",
                            "conf": confidence,
                            "dow": day or "*",
                            "hod": hour,
                            "now": datetime.utcnow().isoformat(),
                        },
                    )
        except Exception as exc:
            logger.warning("Failed to persist workload patterns: %s", exc)


class PreWarmingScheduler:
    """Schedules and executes pre-warming of plans based on predictions."""

    def __init__(
        self,
        predictor: QueryPredictor,
        queue: "QueryQueue",
        plan_cache: "PlanCache",
    ) -> None:
        self._predictor = predictor
        self._queue = queue
        self._plan_cache = plan_cache

    def schedule(self, predictions: list[PredictedQuery]) -> list[dict]:
        results = []
        for prediction in predictions:
            if not self.should_prewarm(prediction):
                continue
            fingerprint = _template_hash(prediction.sql_template)
            cached = self._plan_cache.get(fingerprint, "prewarm")
            if cached is not None:
                logger.debug("Plan already cached for template %s; skipping", fingerprint)
                continue
            results.append({
                "template": prediction.sql_template,
                "warmed_at": datetime.utcnow().isoformat(),
                "cache_key": fingerprint,
                "confidence": prediction.confidence,
            })
        return results

    def execute_prewarm(
        self, template: str, tenant_id: str, engine: "QueryEngine"
    ) -> bool:
        from metamind.core.query_engine import QueryContext
        ctx = QueryContext(
            query_id=f"prewarm-{_template_hash(template)}",
            tenant_id=tenant_id,
            sql=template.replace("?", "1"),
            dry_run=True,
        )
        try:
            engine.execute(ctx)
            logger.debug("Pre-warmed plan (tenant=%s, hash=%s)", tenant_id, _template_hash(template))
            return True
        except Exception as exc:
            logger.warning("Pre-warm failed for %s: %s", _template_hash(template), exc)
            return False

    def should_prewarm(self, prediction: PredictedQuery) -> bool:
        if prediction.confidence < _MIN_CONFIDENCE:
            return False
        now = datetime.utcnow()
        delta = (prediction.predicted_execution_time - now).total_seconds() / 60
        return delta < 10.0

    async def run_background(
        self,
        engine: "QueryEngine",
        interval_seconds: int = 60,
        tenant_id: str = "system",
        history_provider: Optional[object] = None,
    ) -> None:
        logger.info("PreWarmingScheduler background task started")
        while True:
            try:
                recent_history: list[QueryTrace] = []
                if history_provider is not None and callable(history_provider):
                    recent_history = history_provider()  # type: ignore[assignment]
                predictions = self._predictor.predict_next(
                    tenant_id=tenant_id,
                    current_time=datetime.utcnow(),
                    recent_history=recent_history,
                )
                warmed = self.schedule(predictions)
                for job in warmed:
                    self.execute_prewarm(job["template"], tenant_id, engine)
                if warmed:
                    logger.info("Pre-warmed %d plans for tenant=%s", len(warmed), tenant_id)
            except asyncio.CancelledError:
                logger.info("PreWarmingScheduler background task cancelled")
                return
            except Exception as exc:
                logger.exception("Error in pre-warming background task: %s", exc)
            await asyncio.sleep(interval_seconds)
