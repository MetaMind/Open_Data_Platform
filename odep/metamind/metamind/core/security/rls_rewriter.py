"""Row-Level Security (RLS) SQL Rewriter.

Loads RLS policies from mm_rls_policies and rewrites SQL so each
table reference is wrapped in a policy-filtered subquery.

Filter expressions may reference :tenant_id and :user_role bound at
rewrite time.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import sqlglot
import sqlglot.expressions as exp
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class RLSPolicy:
    """A single row-level security policy entry."""

    id: int
    tenant_id: str
    table_name: str
    filter_expr: str
    roles: list[str]
    is_active: bool = True


class RLSRewriter:
    """Rewrite SQL to enforce row-level security policies.

    For each table referenced in the query that has an active RLS policy
    matching the current tenant + user roles, the table reference is replaced
    with a filtered subquery:

        (SELECT * FROM t WHERE {filter_expr}) AS t

    Args:
        db_engine: SQLAlchemy Engine used to load policies.
    """

    def __init__(self, db_engine: Engine) -> None:
        self._engine = db_engine

    async def load_policies(self, tenant_id: str) -> list[RLSPolicy]:
        """Load active RLS policies for the given tenant from the database.

        Returns:
            List of RLSPolicy objects sorted by table_name.
        """
        query = text(
            "SELECT id, tenant_id, table_name, filter_expr, roles, is_active "
            "FROM mm_rls_policies "
            "WHERE tenant_id = :tid AND is_active = TRUE "
            "ORDER BY table_name"
        )
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(query, {"tid": tenant_id}).fetchall()
            return [
                RLSPolicy(
                    id=row.id,
                    tenant_id=row.tenant_id,
                    table_name=row.table_name,
                    filter_expr=row.filter_expr,
                    roles=list(row.roles) if row.roles else [],
                    is_active=bool(row.is_active),
                )
                for row in rows
            ]
        except Exception as exc:
            logger.error(
                "RLSRewriter.load_policies failed tenant=%s: %s", tenant_id, exc
            )
            return []

    async def rewrite(
        self,
        sql: str,
        tenant_id: str,
        user_roles: list[str],
    ) -> str:
        """Rewrite SQL to apply RLS policies.

        Tables without a matching policy are left unchanged.

        Args:
            sql: Input SQL string.
            tenant_id: Current tenant identifier.
            user_roles: Roles held by the requesting user.

        Returns:
            Rewritten SQL string (may be identical to input if no policies match).
        """
        policies = await self.load_policies(tenant_id)
        if not policies:
            return sql

        # Build lookup: table_name (lower) -> applicable policies for user roles
        applicable: dict[str, RLSPolicy] = {}
        for policy in policies:
            tname = policy.table_name.lower()
            if not policy.roles or any(r in user_roles for r in policy.roles):
                applicable[tname] = policy

        if not applicable:
            return sql

        try:
            statements = sqlglot.parse(sql)
            if not statements or statements[0] is None:
                return sql

            rewritten_parts: list[str] = []
            for stmt in statements:
                rewritten_parts.append(
                    self._rewrite_statement(stmt, applicable, tenant_id, user_roles)
                )
            return ";\n".join(rewritten_parts)

        except Exception as exc:
            logger.warning(
                "RLSRewriter parse error for tenant=%s, returning original SQL: %s",
                tenant_id,
                exc,
            )
            return sql

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_sql_literal(value: str) -> str:
        """Return a safely quoted SQL string literal using sqlglot.

        Prevents SQL injection when binding tenant_id / user_role values
        into policy filter expressions.  e.g. "acme'--" -> "'acme''--'"
        """
        return exp.Literal.string(value).sql(dialect="ansi")

    def _rewrite_statement(
        self,
        stmt: exp.Expression,
        applicable: dict[str, RLSPolicy],
        tenant_id: str,
        user_roles: list[str],
    ) -> str:
        """Walk the AST and replace table references with filtered subqueries."""
        primary_role = user_roles[0] if user_roles else ""

        # Pre-compute safely escaped literals once (fixes W-03 / SEC-01)
        tid_literal = self._safe_sql_literal(tenant_id)
        role_literal = self._safe_sql_literal(primary_role)

        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Table):
                return node

            tname = (node.name or "").lower()
            policy = applicable.get(tname)
            if policy is None:
                return node

            # Bind :tenant_id and :user_role with properly quoted literals
            filter_expr = policy.filter_expr
            filter_expr = filter_expr.replace(":tenant_id", tid_literal)
            filter_expr = filter_expr.replace(":user_role", role_literal)

            alias = node.alias or tname
            subquery_sql = (
                f"(SELECT * FROM {tname} WHERE {filter_expr})"
            )
            try:
                subquery = sqlglot.parse_one(subquery_sql)
                # Wrap as aliased subquery
                return exp.Subquery(
                    this=subquery,
                    alias=exp.TableAlias(this=exp.Identifier(this=alias)),
                )
            except Exception as parse_exc:
                logger.warning(
                    "RLSRewriter failed to build subquery for table=%s: %s",
                    tname,
                    parse_exc,
                )
                return node

        transformed = stmt.transform(_transform)
        return transformed.sql(pretty=False)
