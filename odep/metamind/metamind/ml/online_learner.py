"""Online Cost Model Learner.

Maintains a rolling buffer in Redis and performs incremental (online) updates
to the cost model after every batch_size observations.

Compatible with sklearn SGDRegressor and River regression models that expose
a partial_fit(X, y) interface.
"""
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

_BUFFER_KEY_PREFIX = "mm:ol:buffer"


@dataclass
class UpdateResult:
    """Result of an online partial_fit update."""

    samples_used: int
    mae_before: float
    mae_after: float
    weights_saved: bool
    model_id: str = "default"


class OnlineCostLearner:
    """Incremental cost model updater using Redis-buffered observations.

    Buffers (features, predicted_cost, actual_cost) tuples in Redis.
    When buffer length >= batch_size, calls model.partial_fit() and
    persists updated weights if MAE improves by > 5%.

    Args:
        cost_model: A sklearn-style model with partial_fit(X, y) and predict(X).
        redis_client: Synchronous Redis client.
        batch_size: Number of observations before triggering an update.
    """

    def __init__(
        self,
        cost_model: Any,
        redis_client: object,
        batch_size: int = 50,
    ) -> None:
        self._model = cost_model
        self._redis = redis_client
        self._batch_size = batch_size
        self._model_id: str = getattr(cost_model, "model_id", "default")
        self._buffer_key = f"{_BUFFER_KEY_PREFIX}:{self._model_id}"
        self._model_path: Optional[str] = getattr(cost_model, "model_path", None)

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record(
        self,
        query_id: str,
        features: dict[str, Any],
        predicted_cost: float,
        actual_cost: float,
    ) -> None:
        """Buffer a single observation for future partial_fit.

        Args:
            query_id: Identifier for logging.
            features: Feature dict as produced by the cost model feature extractor.
            predicted_cost: Cost predicted before execution.
            actual_cost: Actual observed cost (typically duration_ms).
        """
        observation = json.dumps(
            {
                "query_id": query_id,
                "features": features,
                "predicted": predicted_cost,
                "actual": actual_cost,
            }
        )
        try:
            self._redis.rpush(self._buffer_key, observation)  # type: ignore[union-attr]
            logger.debug(
                "OnlineCostLearner buffered query=%s pred=%.1f actual=%.1f",
                query_id,
                predicted_cost,
                actual_cost,
            )
        except Exception as exc:
            logger.error("OnlineCostLearner.record failed: %s", exc)

    # ------------------------------------------------------------------
    # Trigger check
    # ------------------------------------------------------------------

    def should_update(self) -> bool:
        """Return True when the buffer has enough samples for an update."""
        try:
            length = self._redis.llen(self._buffer_key)  # type: ignore[union-attr]
            return int(length or 0) >= self._batch_size
        except Exception as exc:
            logger.error("OnlineCostLearner.should_update failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Partial fit
    # ------------------------------------------------------------------

    def partial_fit(self) -> UpdateResult:
        """Pop a batch from the buffer and call model.partial_fit(X, y).

        Uses a Redis SET NX distributed lock so only one worker trains at a
        time — prevents the double-train race condition (fixes W-10).

        Persists updated weights only if MAE improves by > 5%.

        Returns:
            UpdateResult with before/after MAE and whether weights were saved.
        """
        lock_key = f"mm:ol:lock:{self._model_id}"
        lock_ttl = 30  # seconds — generous for slow models

        # Acquire lock: SET NX EX — only one process proceeds
        try:
            acquired = self._redis.set(  # type: ignore[union-attr]
                lock_key, "1", nx=True, ex=lock_ttl
            )
        except Exception as exc:
            logger.error("OnlineCostLearner lock acquire failed: %s", exc)
            acquired = True  # fail-open: proceed without lock

        if not acquired:
            logger.debug(
                "OnlineCostLearner.partial_fit skipped — another worker holds lock"
            )
            return UpdateResult(
                samples_used=0,
                mae_before=float("inf"),
                mae_after=float("inf"),
                weights_saved=False,
                model_id=self._model_id,
            )

        try:
            return self._do_partial_fit()
        finally:
            try:
                self._redis.delete(lock_key)  # type: ignore[union-attr]
            except Exception as exc:
                logger.warning("OnlineCostLearner lock release failed: %s", exc)

    def _do_partial_fit(self) -> UpdateResult:
        """Internal: perform the actual partial_fit after lock is held."""
    def _do_partial_fit(self) -> UpdateResult:
        """Internal: perform the actual partial_fit after lock is held."""
        raw_items = self._pop_batch()
        if not raw_items:
            logger.warning("OnlineCostLearner.partial_fit called with empty buffer")
            return UpdateResult(
                samples_used=0,
                mae_before=float("inf"),
                mae_after=float("inf"),
                weights_saved=False,
                model_id=self._model_id,
            )

        X, y_true, y_pred_before = self._unpack(raw_items)

        # MAE before update
        mae_before = _mae(y_pred_before, y_true)

        # Partial fit
        try:
            self._model.partial_fit(X, y_true)
        except AttributeError:
            logger.error(
                "Cost model %s does not support partial_fit", type(self._model)
            )
            return UpdateResult(
                samples_used=len(raw_items),
                mae_before=mae_before,
                mae_after=mae_before,
                weights_saved=False,
                model_id=self._model_id,
            )
        except Exception as exc:
            logger.error("OnlineCostLearner partial_fit error: %s", exc)
            return UpdateResult(
                samples_used=len(raw_items),
                mae_before=mae_before,
                mae_after=mae_before,
                weights_saved=False,
                model_id=self._model_id,
            )

        # MAE after update
        try:
            y_pred_after = self._model.predict(X)
            mae_after = _mae(list(y_pred_after), y_true)
        except Exception as exc:
            logger.warning("MAE post-update computation failed: %s", exc)
            mae_after = mae_before

        improvement = (mae_before - mae_after) / max(mae_before, 1e-9)
        weights_saved = False

        if improvement > 0.05 and self._model_path:
            weights_saved = self._persist_weights()

        logger.info(
            "OnlineCostLearner update: samples=%d mae_before=%.2f mae_after=%.2f "
            "improvement=%.1f%% saved=%s",
            len(raw_items),
            mae_before,
            mae_after,
            improvement * 100,
            weights_saved,
        )
        return UpdateResult(
            samples_used=len(raw_items),
            mae_before=mae_before,
            mae_after=mae_after,
            weights_saved=weights_saved,
            model_id=self._model_id,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _pop_batch(self) -> list[dict]:
        """Atomically pop batch_size items from the Redis list."""
        items: list[dict] = []
        try:
            pipe = self._redis.pipeline()  # type: ignore[union-attr]
            for _ in range(self._batch_size):
                pipe.lpop(self._buffer_key)
            results = pipe.execute()
            for raw in results:
                if raw is None:
                    continue
                try:
                    items.append(json.loads(raw))
                except json.JSONDecodeError as exc:
                    logger.error("OnlineCostLearner JSON decode error: %s", exc)
        except Exception as exc:
            logger.error("OnlineCostLearner._pop_batch failed: %s", exc)
        return items

    def _unpack(
        self, items: list[dict]
    ) -> tuple[list[list[float]], list[float], list[float]]:
        """Convert observation dicts to (X, y_true, y_pred_before)."""
        X: list[list[float]] = []
        y_true: list[float] = []
        y_pred: list[float] = []
        for obs in items:
            feats = obs.get("features", {})
            X.append([float(v) for v in feats.values()])
            y_true.append(float(obs.get("actual", 0)))
            y_pred.append(float(obs.get("predicted", 0)))
        return X, y_true, y_pred

    def _persist_weights(self) -> bool:
        """Serialize model weights to disk."""
        try:
            import pickle
            with open(self._model_path, "wb") as fh:  # type: ignore[arg-type]
                pickle.dump(self._model, fh)
            logger.info("OnlineCostLearner: saved weights to %s", self._model_path)
            return True
        except Exception as exc:
            logger.error("OnlineCostLearner._persist_weights failed: %s", exc)
            return False


def _mae(predicted: list[float], actual: list[float]) -> float:
    """Compute mean absolute error."""
    if not predicted or not actual:
        return float("inf")
    n = min(len(predicted), len(actual))
    return sum(abs(p - a) for p, a in zip(predicted, actual)) / n
