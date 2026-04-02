"""Vector-specific filter and aggregate operators.

Provides pre-filter and post-aggregate functionality for vector search
pipelines, including centroid computation for cluster analysis.

Feature: F19_vector_search
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np

from metamind.core.types import Predicate

logger = logging.getLogger(__name__)


class VectorFilterOperator:
    """Pre-filter rows before ANN search using standard predicates."""

    def apply(
        self, rows: list[dict[str, Any]], predicates: list[Predicate]
    ) -> list[dict[str, Any]]:
        """Apply filter predicates to rows before vector search.

        Args:
            rows: List of row dicts to filter.
            predicates: Filter conditions to apply.

        Returns:
            Filtered list of rows matching all predicates.
        """
        if not predicates:
            return rows

        result: list[dict[str, Any]] = []
        for row in rows:
            if self._matches_all(row, predicates):
                result.append(row)
        return result

    def _matches_all(self, row: dict[str, Any], predicates: list[Predicate]) -> bool:
        """Check if a row matches all predicates."""
        for pred in predicates:
            if not self._matches_predicate(row, pred):
                return False
        return True

    def _matches_predicate(self, row: dict[str, Any], pred: Predicate) -> bool:
        """Evaluate a single predicate against a row."""
        col = pred.column
        if col not in row:
            logger.debug("Column '%s' not found in row, predicate fails", col)
            return False

        row_val = row[col]
        op = pred.operator.upper()

        if op == "IS NULL":
            return row_val is None
        if op == "IS NOT NULL":
            return row_val is not None
        if row_val is None:
            return False

        if op == "=":
            return row_val == pred.value
        elif op == "!=":
            return row_val != pred.value
        elif op == "<":
            return row_val < pred.value
        elif op == "<=":
            return row_val <= pred.value
        elif op == ">":
            return row_val > pred.value
        elif op == ">=":
            return row_val >= pred.value
        elif op == "IN":
            if isinstance(pred.value, (list, tuple, set)):
                return row_val in pred.value
            return row_val == pred.value
        elif op == "LIKE":
            return self._like_match(str(row_val), str(pred.value))
        else:
            logger.warning("Unsupported predicate operator: %s", op)
            return True

    @staticmethod
    def _like_match(value: str, pattern: str) -> bool:
        """Simple SQL LIKE pattern matching (% and _ wildcards)."""
        import re

        regex = "^"
        for ch in pattern:
            if ch == "%":
                regex += ".*"
            elif ch == "_":
                regex += "."
            else:
                regex += re.escape(ch)
        regex += "$"
        return bool(re.match(regex, value, re.IGNORECASE))


class VectorAggregateOperator:
    """Post-search aggregate operations for vector data."""

    def compute_centroid(self, vectors: list[list[float]]) -> list[float]:
        """Compute the centroid (mean) of a set of vectors.

        Useful for cluster analysis after vector search results
        are grouped or for finding representative embeddings.

        Args:
            vectors: List of embedding vectors (all same dimensionality).

        Returns:
            Centroid vector (element-wise mean).

        Raises:
            ValueError: If vectors list is empty.
        """
        if not vectors:
            raise ValueError("Cannot compute centroid of empty vector set")

        matrix = np.array(vectors, dtype=np.float64)
        centroid = np.mean(matrix, axis=0)
        return centroid.tolist()

    def compute_spread(self, vectors: list[list[float]]) -> float:
        """Compute the average distance from centroid (cluster spread).

        Args:
            vectors: List of embedding vectors.

        Returns:
            Average L2 distance from centroid.
        """
        if len(vectors) < 2:
            return 0.0

        matrix = np.array(vectors, dtype=np.float64)
        centroid = np.mean(matrix, axis=0)
        diffs = matrix - centroid
        distances = np.sqrt(np.sum(diffs ** 2, axis=1))
        return float(np.mean(distances))

    def nearest_to_centroid(
        self, vectors: list[list[float]], data: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Find the data row whose vector is nearest to the centroid.

        Args:
            vectors: List of embedding vectors.
            data: Corresponding list of row dicts.

        Returns:
            Row dict of the medoid (nearest actual point to centroid).
        """
        if not vectors or not data:
            raise ValueError("Cannot find nearest to centroid of empty data")

        centroid = np.array(self.compute_centroid(vectors))
        matrix = np.array(vectors, dtype=np.float64)
        diffs = matrix - centroid
        distances = np.sqrt(np.sum(diffs ** 2, axis=1))
        min_idx = int(np.argmin(distances))
        return data[min_idx]
