"""PostgreSQL dialect SQL generator (F17)."""
from __future__ import annotations

import logging
import re
from typing import Optional

from metamind.core.logical.nodes import LogicalNode
from metamind.core.execution.dialects.base_gen import node_to_sql, _try_transpile

logger = logging.getLogger(__name__)


class PostgreSQLGenerator:
    """Generates PostgreSQL-dialect SQL from a logical plan or SQL string.

    Supports pg_hint_plan-style hints for index selection and parallelism.
    """

    def generate(self, node: LogicalNode, sql_hint: str = "") -> str:
        """Generate PostgreSQL SQL from a logical plan node.

        Args:
            node: Root of the logical plan tree.
            sql_hint: Optional raw SQL string to pass through transpilation
                      instead of generating from the node tree.

        Returns:
            PostgreSQL-compatible SQL string.
        """
        if sql_hint:
            return _try_transpile(sql_hint, read="postgres", write="postgres")
        return node_to_sql(node, dialect="postgres")

    def add_index_hint(self, sql: str, table: str, index: str) -> str:
        """Inject a pg_hint_plan IndexScan hint comment before the SQL.

        Requires the pg_hint_plan extension to be installed.

        Args:
            sql: Base SQL query.
            table: Table alias or name.
            index: Index name to force.

        Returns:
            SQL with pg_hint_plan comment prepended.
        """
        hint = f"/*+ IndexScan({table} {index}) */"
        return f"{hint}\n{sql}"

    def add_parallel_hint(self, sql: str, workers: int) -> str:
        """Prepend a pg_hint_plan Parallel hint to enable N workers.

        Args:
            sql: Base SQL query.
            workers: Desired number of parallel workers.

        Returns:
            SQL with parallel hint prepended.
        """
        hint = f"/*+ Parallel(* {workers} hard) */"
        return f"{hint}\n{sql}"

    def add_enable_nestloop_hint(self, sql: str, enable: bool = True) -> str:
        """Enable or disable nested loop join via pg_hint_plan.

        Args:
            sql: Base SQL query.
            enable: If False, disables nested loop joins.

        Returns:
            SQL with NestLoop hint prepended.
        """
        hint_op = "NestLoop" if enable else "NoNestLoop"
        hint = f"/*+ {hint_op} */"
        return f"{hint}\n{sql}"

    def add_hash_join_hint(self, sql: str, left_table: str, right_table: str) -> str:
        """Force a hash join between two tables via pg_hint_plan.

        Args:
            sql: Base SQL query.
            left_table: Alias of the left (build) side.
            right_table: Alias of the right (probe) side.

        Returns:
            SQL with HashJoin hint prepended.
        """
        hint = f"/*+ HashJoin({left_table} {right_table}) */"
        return f"{hint}\n{sql}"

    def set_work_mem(self, sql: str, work_mem_mb: int) -> str:
        """Prepend a SET work_mem statement to increase sort/hash memory.

        Args:
            sql: Base SQL query.
            work_mem_mb: Memory in MiB (e.g. 64 for 64 MB).

        Returns:
            Two-statement string: SET + original SQL.
        """
        return f"SET LOCAL work_mem = '{work_mem_mb}MB';\n{sql}"
