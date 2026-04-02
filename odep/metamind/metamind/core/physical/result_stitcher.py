"""
Cross-Engine Result Stitcher

File: metamind/core/physical/result_stitcher.py
Role: Senior Distributed Systems Engineer
Addresses: execution_graph.py stitch_results() raised NotImplementedError
           for join-type stitching (audit finding 4.3)

Implements three stitching strategies for combining results from
multiple execution engines into a single Apache Arrow table:

  UNION  — vertical concatenation (same schema required)
  JOIN   — hash-join on specified key columns (cross-engine)
  MERGE  — Oracle real-time + S3 historical deduplication by primary key
            (the 5 % hybrid path in the documented query lifecycle)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger(__name__)


class StitchType(Enum):
    """How to combine partial results from multiple engines."""
    UNION = "union"       # Simple vertical append
    JOIN = "join"         # Hash-join on key columns
    MERGE = "merge"       # Upsert: right (fresh) rows override left (cached)


@dataclass
class StitchSpec:
    """Specification for a stitching operation."""
    stitch_type: StitchType
    join_keys: List[str] = field(default_factory=list)      # for JOIN / MERGE
    join_type: str = "inner"                                 # inner | left | full
    dedup_key: Optional[str] = None                         # for MERGE uniqueness
    timestamp_col: Optional[str] = None                     # for MERGE freshness
    coalesce_nulls: bool = True                             # prefer non-null values
    output_columns: Optional[List[str]] = None              # column projection


class ResultStitcher:
    """
    Combines partial query results from heterogeneous execution engines.

    Designed for the MetaMind hybrid execution path where:
    - Engine A (Oracle / Trino)  returns recent/fresh rows
    - Engine B (S3 / Trino)      returns historical/bulk rows

    All operations work on Apache Arrow Tables and avoid materialising
    large intermediate datasets into Python lists.
    """

    def stitch(
        self,
        results: List[pa.Table],
        spec: StitchSpec,
    ) -> pa.Table:
        """
        Combine *results* according to *spec*.

        Args:
            results:  List of Arrow Tables from each engine.
            spec:     Stitching specification.

        Returns:
            A single merged Arrow Table.

        Raises:
            ValueError: If results is empty or spec is invalid.
        """
        if not results:
            raise ValueError("No results to stitch — list is empty")

        # Drop empty tables
        non_empty = [t for t in results if t.num_rows > 0]
        if not non_empty:
            # Return an empty table with the schema of the first result
            return results[0].slice(0, 0)

        if len(non_empty) == 1:
            return self._project(non_empty[0], spec)

        if spec.stitch_type == StitchType.UNION:
            return self._union(non_empty, spec)
        if spec.stitch_type == StitchType.JOIN:
            return self._join(non_empty, spec)
        if spec.stitch_type == StitchType.MERGE:
            return self._merge(non_empty, spec)

        raise ValueError(f"Unknown stitch type: {spec.stitch_type}")

    # ------------------------------------------------------------------
    # UNION  — vertical concatenation
    # ------------------------------------------------------------------

    def _union(self, tables: List[pa.Table], spec: StitchSpec) -> pa.Table:
        """
        Concatenate tables vertically.
        Schemas are unified (missing columns filled with null).
        """
        try:
            unified = self._unify_schemas(tables)
            combined = pa.concat_tables(unified, promote_options="default")
            return self._project(combined, spec)
        except Exception as exc:
            logger.error("UNION stitch failed: %s", exc)
            raise

    # ------------------------------------------------------------------
    # JOIN  — hash-join on key columns
    # ------------------------------------------------------------------

    def _join(self, tables: List[pa.Table], spec: StitchSpec) -> pa.Table:
        """
        Hash-join two Arrow tables on *spec.join_keys*.

        Supports inner, left, and full-outer join semantics.
        For more than two tables, tables are joined left-to-right.
        """
        if not spec.join_keys:
            raise ValueError("JOIN stitch requires at least one join_key in StitchSpec")

        result = tables[0]
        for right in tables[1:]:
            result = self._hash_join(result, right, spec)
        return self._project(result, spec)

    def _hash_join(
        self,
        left: pa.Table,
        right: pa.Table,
        spec: StitchSpec,
    ) -> pa.Table:
        """Single two-table hash join using PyArrow join kernel."""
        join_type_map = {
            "inner": "inner",
            "left": "left outer",
            "full": "full outer",
        }
        join_type = join_type_map.get(spec.join_type.lower(), "inner")

        # Rename duplicate non-key columns in right table to avoid collision
        right_renamed = self._disambiguate_columns(left, right, spec.join_keys)

        try:
            joined = left.join(
                right_renamed,
                keys=spec.join_keys,
                join_type=join_type,
                coalesce_keys=True,
            )
        except Exception as exc:
            logger.error(
                "Hash-join on keys=%s failed: %s — falling back to UNION", spec.join_keys, exc
            )
            return pa.concat_tables(self._unify_schemas([left, right]))

        if spec.coalesce_nulls:
            joined = self._coalesce_columns(joined, left, right_renamed, spec.join_keys)

        return joined

    # ------------------------------------------------------------------
    # MERGE  — upsert for hybrid Oracle + S3 path
    # ------------------------------------------------------------------

    def _merge(self, tables: List[pa.Table], spec: StitchSpec) -> pa.Table:
        """
        Merge tables using an upsert strategy.

        The *last* table in the list is treated as the authoritative
        (freshest) source.  Its rows take precedence over earlier tables
        when the dedup_key matches.

        If no dedup_key is provided, falls back to UNION.
        """
        if not spec.dedup_key:
            logger.warning("MERGE requested without dedup_key — falling back to UNION")
            return self._union(tables, spec)

        # Start with the bulk/historical table (first)
        base = tables[0]
        for fresh in tables[1:]:
            base = self._upsert(base, fresh, spec)

        return self._project(base, spec)

    def _upsert(
        self,
        base: pa.Table,
        fresh: pa.Table,
        spec: StitchSpec,
    ) -> pa.Table:
        """
        Upsert *fresh* rows into *base*.

        Rows in *fresh* whose dedup_key matches a row in *base*
        override it; new keys from *fresh* are appended.

        Strategy:
        1. Build a set of keys present in *fresh*.
        2. Keep only *base* rows whose key is NOT in *fresh* (the
           "cold" rows not overridden by the live source).
        3. Concatenate remaining *base* rows with *fresh* rows.
        """
        key = spec.dedup_key

        if key not in fresh.column_names or key not in base.column_names:
            logger.warning(
                "dedup_key '%s' missing from one of the tables — UNION fallback", key
            )
            return pa.concat_tables(self._unify_schemas([base, fresh]))

        # Keys present in the fresh (authoritative) table
        fresh_keys = fresh.column(key)
        fresh_key_set = set(fresh_keys.to_pylist())

        # Filter base: keep rows whose key is NOT in fresh
        base_key_col = base.column(key)
        mask = pc.invert(
            pc.is_in(base_key_col, value_set=pa.array(list(fresh_key_set)))
        )
        cold_base = base.filter(mask)

        # Align schemas before concat
        merged = pa.concat_tables(
            self._unify_schemas([cold_base, fresh]),
            promote_options="default",
        )

        # Optionally sort by timestamp so fresh rows appear first
        if spec.timestamp_col and spec.timestamp_col in merged.column_names:
            idx = pc.sort_indices(
                merged, sort_keys=[(spec.timestamp_col, "descending")]
            )
            merged = merged.take(idx)

        logger.debug(
            "MERGE: base=%d rows, fresh=%d rows, cold=%d rows, output=%d rows",
            base.num_rows,
            fresh.num_rows,
            cold_base.num_rows,
            merged.num_rows,
        )
        return merged

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _unify_schemas(tables: List[pa.Table]) -> List[pa.Table]:
        """Add missing columns (as null) so all tables share a common schema."""
        # Build union schema
        all_fields: Dict[str, pa.Field] = {}
        for t in tables:
            for f in t.schema:
                if f.name not in all_fields:
                    all_fields[f.name] = f

        unified_schema = pa.schema(list(all_fields.values()))
        result = []
        for t in tables:
            missing = [f for f in unified_schema if f.name not in t.column_names]
            if not missing:
                result.append(t.cast(unified_schema))
                continue
            for f in missing:
                null_col = pa.array([None] * t.num_rows, type=f.type)
                t = t.append_column(f, null_col)
            result.append(t.cast(unified_schema))
        return result

    @staticmethod
    def _disambiguate_columns(
        left: pa.Table,
        right: pa.Table,
        join_keys: List[str],
    ) -> pa.Table:
        """Rename right-table columns that conflict with left (excluding join keys)."""
        left_cols = set(left.column_names)
        renames: Dict[str, str] = {}
        for name in right.column_names:
            if name in left_cols and name not in join_keys:
                renames[name] = f"{name}_right"
        if not renames:
            return right
        new_names = [renames.get(n, n) for n in right.column_names]
        return right.rename_columns(new_names)

    @staticmethod
    def _coalesce_columns(
        joined: pa.Table,
        left: pa.Table,
        right: pa.Table,
        join_keys: List[str],
    ) -> pa.Table:
        """For each column pair (col / col_right), coalesce NULLs from left side."""
        for col in joined.column_names:
            if col.endswith("_right"):
                base_col = col[: -len("_right")]
                if base_col in joined.column_names and base_col not in join_keys:
                    coalesced = pc.if_else(
                        pc.is_null(joined.column(base_col)),
                        joined.column(col),
                        joined.column(base_col),
                    )
                    idx = joined.column_names.index(base_col)
                    joined = joined.set_column(idx, base_col, coalesced)
                    joined = joined.remove_column(joined.column_names.index(col))
        return joined

    @staticmethod
    def _project(table: pa.Table, spec: StitchSpec) -> pa.Table:
        """Apply optional column projection from *spec.output_columns*."""
        if not spec.output_columns:
            return table
        valid = [c for c in spec.output_columns if c in table.column_names]
        if not valid:
            logger.warning("output_columns projection produced no valid columns — returning all")
            return table
        return table.select(valid)
