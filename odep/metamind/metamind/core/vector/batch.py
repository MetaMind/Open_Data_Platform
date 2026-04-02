"""PyArrow batch processor for efficient ANN computation.

Provides vectorized distance calculations using numpy for high-throughput
vector similarity search when data is held in columnar format.

Feature: F11_compiled_execution, F19_vector_search
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

try:
    import pyarrow as pa
    import pyarrow.compute as pc

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    logger.warning("PyArrow not available; VectorBatchProcessor will use fallback mode")


class VectorBatchProcessor:
    """Processes vector distance calculations in batches using PyArrow and numpy.

    Supports cosine, L2 (Euclidean), and inner product distance metrics
    with vectorized computation for high throughput.
    """

    def __init__(self, batch_size: int = 10_000) -> None:
        self.batch_size = batch_size

    def compute_distances_batch(
        self,
        query_vector: list[float],
        data: list[dict[str, object]],
        vector_column: str,
        metric: str = "cosine",
    ) -> list[dict[str, object]]:
        """Compute distances between query_vector and all vectors in data.

        Returns the data with an additional '__distance' field per row.
        Uses numpy for vectorized computation.

        Args:
            query_vector: The query embedding vector.
            data: List of row dicts, each containing the vector column.
            vector_column: Name of the column holding embedding vectors.
            metric: Distance metric: "cosine", "l2", or "inner_product".

        Returns:
            List of row dicts with '__distance' field added.
        """
        if not data:
            return []

        query = np.array(query_vector, dtype=np.float64)
        result: list[dict[str, object]] = []

        for batch_start in range(0, len(data), self.batch_size):
            batch = data[batch_start : batch_start + self.batch_size]
            vectors_raw = [row.get(vector_column, []) for row in batch]

            matrix = np.array(vectors_raw, dtype=np.float64)
            if matrix.ndim == 1:
                matrix = matrix.reshape(1, -1)

            distances = self._compute_metric(query, matrix, metric)

            for i, row in enumerate(batch):
                row_copy = dict(row)
                row_copy["__distance"] = float(distances[i])
                result.append(row_copy)

        return result

    def compute_distances_arrow(
        self,
        query_vector: list[float],
        table: object,
        vector_column: str,
        metric: str = "cosine",
    ) -> object:
        """Compute distances using PyArrow tables when available.

        Args:
            query_vector: The query embedding vector.
            table: A PyArrow Table or compatible object.
            vector_column: Column name holding embedding vectors.
            metric: Distance metric to use.

        Returns:
            PyArrow Table with '__distance' column appended.
        """
        if not HAS_PYARROW:
            raise RuntimeError("PyArrow is required for arrow-based distance computation")

        pa_table = table  # type: ignore[assignment]
        query = np.array(query_vector, dtype=np.float64)

        col = pa_table.column(vector_column)  # type: ignore[union-attr]
        vectors_list = col.to_pylist()  # type: ignore[union-attr]
        matrix = np.array(vectors_list, dtype=np.float64)

        distances = self._compute_metric(query, matrix, metric)
        dist_array = pa.array(distances.tolist(), type=pa.float64())  # type: ignore[union-attr]

        return pa_table.append_column("__distance", dist_array)  # type: ignore[union-attr]

    def top_k_filter(
        self,
        data: list[dict[str, object]],
        k: int,
    ) -> list[dict[str, object]]:
        """Return top-k rows by __distance column (ascending).

        Args:
            data: Rows with '__distance' field.
            k: Number of top results to return.

        Returns:
            Top-k rows sorted by distance ascending.
        """
        sorted_data = sorted(data, key=lambda r: float(r.get("__distance", float("inf"))))
        return sorted_data[:k]

    def top_k_filter_arrow(
        self,
        table: object,
        k: int,
    ) -> object:
        """Return top-k rows from a PyArrow table by __distance column.

        Args:
            table: PyArrow Table with '__distance' column.
            k: Number of top results.

        Returns:
            PyArrow Table with top-k rows.
        """
        if not HAS_PYARROW:
            raise RuntimeError("PyArrow is required for arrow-based top-k")

        pa_table = table  # type: ignore[assignment]
        indices = pc.sort_indices(pa_table, sort_keys=[("__distance", "ascending")])  # type: ignore[union-attr]
        top_indices = indices[:k].to_pylist()  # type: ignore[union-attr]
        return pa_table.take(top_indices)  # type: ignore[union-attr]

    def _compute_metric(
        self, query: np.ndarray, matrix: np.ndarray, metric: str
    ) -> np.ndarray:
        """Compute distance between query and matrix of vectors.

        Args:
            query: 1D numpy array of shape (d,)
            matrix: 2D numpy array of shape (n, d)
            metric: "cosine", "l2", or "inner_product"

        Returns:
            1D numpy array of distances, shape (n,)
        """
        if matrix.shape[0] == 0:
            return np.array([], dtype=np.float64)

        if metric == "cosine":
            return self._cosine_distance(query, matrix)
        elif metric == "l2":
            return self._l2_distance(query, matrix)
        elif metric == "inner_product":
            return self._inner_product_distance(query, matrix)
        else:
            logger.warning("Unknown metric '%s', falling back to cosine", metric)
            return self._cosine_distance(query, matrix)

    @staticmethod
    def _cosine_distance(query: np.ndarray, matrix: np.ndarray) -> np.ndarray:
        """Cosine distance: 1 - cos(θ) = 1 - (A·B)/(|A||B|)"""
        query_norm = np.linalg.norm(query)
        if query_norm == 0:
            return np.ones(matrix.shape[0], dtype=np.float64)

        row_norms = np.linalg.norm(matrix, axis=1)
        zero_mask = row_norms == 0
        row_norms[zero_mask] = 1.0

        dots = matrix @ query
        cosines = dots / (row_norms * query_norm)
        cosines = np.clip(cosines, -1.0, 1.0)
        distances = 1.0 - cosines
        distances[zero_mask] = 1.0
        return distances

    @staticmethod
    def _l2_distance(query: np.ndarray, matrix: np.ndarray) -> np.ndarray:
        """Euclidean (L2) distance: sqrt(sum((a-b)²))"""
        diff = matrix - query
        return np.sqrt(np.sum(diff ** 2, axis=1))

    @staticmethod
    def _inner_product_distance(query: np.ndarray, matrix: np.ndarray) -> np.ndarray:
        """Negative inner product: -(A·B) so we sort ASC for top similarity."""
        dots = matrix @ query
        return -dots
