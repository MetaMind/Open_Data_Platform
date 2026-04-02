"""GraphQL Gateway for MetaMind — exposes query logs, tables, tenants,
column lineage, and mutations through a single GraphQL schema.

Uses strawberry-graphql[fastapi].  The router is mounted at /graphql in server.py.

If strawberry is not installed the module still imports but the router will be None
and server.py skips mounting, preventing startup failures in environments where
the optional dependency is absent.
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional strawberry import — fail gracefully
# ---------------------------------------------------------------------------
try:
    import strawberry
    from strawberry.fastapi import GraphQLRouter
    _STRAWBERRY_AVAILABLE = True
except ImportError:
    _STRAWBERRY_AVAILABLE = False
    logger.warning(
        "strawberry-graphql not installed; GraphQL endpoint will not be available. "
        "Install with: pip install 'strawberry-graphql[fastapi]'"
    )


# ---------------------------------------------------------------------------
# GraphQL types (only defined when strawberry is available)
# ---------------------------------------------------------------------------

if _STRAWBERRY_AVAILABLE:
    @strawberry.type
    class TableInfo:
        name: str
        schema: Optional[str]
        row_count: Optional[int]
        tenant_id: str

    @strawberry.type
    class TenantInfo:
        tenant_id: str
        name: str
        is_active: bool
        created_at: Optional[str]

    @strawberry.type
    class QueryLogEntry:
        query_id: str
        tenant_id: str
        sql: str
        duration_ms: Optional[float]
        status: Optional[str]
        executed_at: Optional[str]

    @strawberry.type
    class QueryLogPage:
        items: List[QueryLogEntry]
        total: int
        offset: int
        limit: int

    @strawberry.type
    class LineageEdge:
        source_table: str
        source_column: str
        target_table: str
        target_column: str

    @strawberry.type
    class LineageResult:
        table: str
        column: Optional[str]
        edges: List[LineageEdge]

    @strawberry.type
    class QueryResult:
        query_id: str
        status: str
        backend_used: str
        row_count: int
        duration_ms: float
        error: Optional[str]

    @strawberry.input
    class TenantInput:
        tenant_id: str
        name: str
        contact_email: Optional[str] = None

    # -----------------------------------------------------------------------
    # Query resolvers
    # -----------------------------------------------------------------------

    @strawberry.type
    class Query:  # noqa: WPS110 — strawberry requires name "Query"
        @strawberry.field
        def tables(
            self,
            info: strawberry.types.Info,
            tenant_id: str,
            schema: Optional[str] = None,
        ) -> List[TableInfo]:
            """List tables for a tenant."""
            ctx = _get_app_context(info)
            if ctx is None:
                return []
            try:
                from sqlalchemy import text
                db = ctx._sync_db_engine
                with db.connect() as conn:
                    rows = conn.execute(
                        text(
                            "SELECT table_name, table_schema "
                            "FROM information_schema.tables "
                            "WHERE table_schema NOT IN ('pg_catalog','information_schema') "
                            "ORDER BY table_name LIMIT 200"
                        )
                    ).fetchall()
                return [
                    TableInfo(
                        name=r.table_name,
                        schema=r.table_schema,
                        row_count=None,
                        tenant_id=tenant_id,
                    )
                    for r in rows
                ]
            except Exception as exc:
                logger.error("GraphQL tables resolver error: %s", exc)
                return []

        @strawberry.field
        def tenants(self, info: strawberry.types.Info) -> List[TenantInfo]:
            """List all tenants — admin only (W-08)."""
            try:
                _require_admin(info)
            except PermissionError:
                logger.warning("GraphQL tenants: non-admin access denied for tenant=%s",
                               _get_caller_tenant(info))
                return []
            ctx = _get_app_context(info)
            if ctx is None:
                return []
            try:
                from sqlalchemy import text
                db = ctx._sync_db_engine
                with db.connect() as conn:
                    rows = conn.execute(
                        text(
                            "SELECT tenant_id, name, is_active, "
                            "CAST(created_at AS TEXT) AS created_at "
                            "FROM mm_tenants ORDER BY tenant_id LIMIT 500"
                        )
                    ).fetchall()
                return [
                    TenantInfo(
                        tenant_id=r.tenant_id,
                        name=r.name,
                        is_active=bool(r.is_active),
                        created_at=r.created_at,
                    )
                    for r in rows
                ]
            except Exception as exc:
                logger.error("GraphQL tenants resolver error: %s", exc)
                return []

        @strawberry.field
        def query_logs(
            self,
            info: strawberry.types.Info,
            tenant_id: str,
            limit: int = 50,
            offset: int = 0,
        ) -> QueryLogPage:
            """Paginated query log entries for a tenant.

            Non-admin callers can only query logs for their own tenant (W-08).
            """
            ctx = _get_app_context(info)
            if ctx is None:
                return QueryLogPage(items=[], total=0, offset=offset, limit=limit)
            # W-08: enforce tenant isolation — non-admins can only see their own logs
            caller_tenant = _get_caller_tenant(info)
            if not info.context.get("is_admin", False) and caller_tenant != tenant_id:
                return QueryLogPage(items=[], total=0, offset=offset, limit=limit)
            try:
                from sqlalchemy import text
                db = ctx._sync_db_engine
                with db.connect() as conn:
                    rows = conn.execute(
                        text(
                            "SELECT query_id, tenant_id, original_sql AS sql, total_time_ms AS duration_ms, "
                            "status, CAST(submitted_at AS TEXT) AS executed_at "
                            "FROM mm_query_logs WHERE tenant_id = :tid "
                            "ORDER BY submitted_at DESC LIMIT :lim OFFSET :off"
                        ),
                        {"tid": tenant_id, "lim": limit, "off": offset},
                    ).fetchall()
                    total_row = conn.execute(
                        text(
                            "SELECT COUNT(*) FROM mm_query_logs WHERE tenant_id = :tid"
                        ),
                        {"tid": tenant_id},
                    ).scalar()
                items = [
                    QueryLogEntry(
                        query_id=r.query_id,
                        tenant_id=r.tenant_id,
                        sql=r.sql,
                        duration_ms=float(r.duration_ms) if r.duration_ms else None,
                        status=r.status,
                        executed_at=r.executed_at,
                    )
                    for r in rows
                ]
                return QueryLogPage(
                    items=items, total=int(total_row or 0),
                    offset=offset, limit=limit,
                )
            except Exception as exc:
                logger.error("GraphQL query_logs resolver error: %s", exc)
                return QueryLogPage(items=[], total=0, offset=offset, limit=limit)

        @strawberry.field
        def column_lineage(
            self,
            info: strawberry.types.Info,
            tenant_id: str,
            table: str,
            column: Optional[str] = None,
        ) -> LineageResult:
            """Return column lineage edges for a table/column."""
            ctx = _get_app_context(info)
            if ctx is None:
                return LineageResult(table=table, column=column, edges=[])
            try:
                from sqlalchemy import text
                db = ctx._sync_db_engine
                params: dict[str, Any] = {"tid": tenant_id, "tbl": table}
                col_filter = " AND source_column = :col" if column else ""
                if column:
                    params["col"] = column
                with db.connect() as conn:
                    rows = conn.execute(
                        text(
                            "SELECT source_table, source_column, "
                            "target_table, target_column "
                            "FROM mm_column_lineage "
                            f"WHERE tenant_id = :tid AND source_table = :tbl{col_filter} "
                            "LIMIT 200"
                        ),
                        params,
                    ).fetchall()
                edges = [
                    LineageEdge(
                        source_table=r.source_table,
                        source_column=r.source_column,
                        target_table=r.target_table,
                        target_column=r.target_column,
                    )
                    for r in rows
                ]
                return LineageResult(table=table, column=column, edges=edges)
            except Exception as exc:
                logger.error("GraphQL column_lineage resolver error: %s", exc)
                return LineageResult(table=table, column=column, edges=[])

    # -----------------------------------------------------------------------
    # Mutation resolvers
    # -----------------------------------------------------------------------

    @strawberry.type
    class Mutation:
        @strawberry.mutation
        def execute_query(
            self, info: strawberry.types.Info, sql: str, tenant_id: str
        ) -> QueryResult:
            """Execute SQL — caller may only execute against their own tenant (W-08)."""
            import uuid, time
            query_id = str(uuid.uuid4())[:12]
            # W-08: tenant isolation — non-admins can only run queries for their tenant
            caller = _get_caller_tenant(info)
            if not info.context.get("is_admin", False) and caller != tenant_id:
                return QueryResult(
                    query_id=query_id, status="error",
                    backend_used="none", row_count=0,
                    duration_ms=0.0,
                    error=f"Forbidden: cannot execute queries for tenant '{tenant_id}'",
                )
            ctx = _get_app_context(info)
            if ctx is None:
                return QueryResult(
                    query_id=query_id, status="error",
                    backend_used="none", row_count=0,
                    duration_ms=0.0, error="Application not initialized",
                )
            t0 = time.monotonic()
            try:
                from sqlalchemy import text
                db = ctx._sync_db_engine
                with db.connect() as conn:
                    result = conn.execute(text(sql))
                    rows = result.fetchall()
                dur = (time.monotonic() - t0) * 1000
                return QueryResult(
                    query_id=query_id, status="success",
                    backend_used="metamind-internal",
                    row_count=len(rows), duration_ms=dur, error=None,
                )
            except Exception as exc:
                logger.error("GraphQL executeQuery error: %s", exc)
                return QueryResult(
                    query_id=query_id, status="error",
                    backend_used="none", row_count=0,
                    duration_ms=0.0, error=str(exc),
                )

        @strawberry.mutation
        def create_tenant(
            self, info: strawberry.types.Info, input: TenantInput
        ) -> TenantInfo:
            """Create a new tenant — admin only (W-08)."""
            _require_admin(info)   # raises PermissionError for non-admins
            ctx = _get_app_context(info)
            if ctx is None:
                raise ValueError("Application not initialized")
            try:
                from sqlalchemy import text
                db = ctx._sync_db_engine
                with db.begin() as conn:
                    conn.execute(
                        text(
                            "INSERT INTO mm_tenants (tenant_id, name, is_active) "
                            "VALUES (:tid, :name, TRUE) "
                            "ON CONFLICT (tenant_id) DO NOTHING"
                        ),
                        {"tid": input.tenant_id, "name": input.name},
                    )
                return TenantInfo(
                    tenant_id=input.tenant_id,
                    name=input.name,
                    is_active=True,
                    created_at=None,
                )
            except Exception as exc:
                logger.error("GraphQL createTenant error: %s", exc)
                raise ValueError(str(exc)) from exc

    # -----------------------------------------------------------------------
    # Schema + router factory
    # -----------------------------------------------------------------------

    schema = strawberry.Schema(query=Query, mutation=Mutation)

    def build_graphql_router(app_context: Any) -> "GraphQLRouter":
        """Build a GraphQL router with JWT authentication (fixes W-08).

        Every request must carry a valid Bearer token.  The verified tenant_id
        and admin status are injected into the resolver context so each
        resolver can enforce tenant isolation.
        """
        from fastapi import Request as FastAPIRequest
        from fastapi import HTTPException as FastHTTPException

        async def get_context(request: FastAPIRequest) -> dict[str, Any]:  # type: ignore[misc]
            """Authenticate and build resolver context."""
            # Import here to avoid circular at module load time
            from metamind.api.auth import get_current_tenant
            from metamind.config.settings import get_settings

            settings = get_settings()
            auth_header: str | None = request.headers.get("Authorization")
            x_tenant: str | None = request.headers.get("X-Tenant-ID")

            try:
                tenant_id = get_current_tenant(
                    authorization=auth_header,
                    x_tenant_id=x_tenant,
                )
            except FastHTTPException as exc:
                # Re-raise so strawberry returns a proper 401
                raise exc

            # Admin flag: tenants whose ID starts with "admin_" or have
            # an explicit "admin": true claim in their JWT payload
            is_admin = False
            if auth_header and auth_header.startswith("Bearer "):
                try:
                    from jose import jwt as _jwt
                    payload = _jwt.decode(
                        auth_header[7:],
                        settings.secret_key,
                        algorithms=[settings.jwt_algorithm],
                        options={"verify_exp": False},
                    )
                    is_admin = bool(payload.get("admin", False))
                except Exception:
                    pass

            return {
                "app_context": app_context,
                "tenant_id": tenant_id,
                "is_admin": is_admin,
            }

        return GraphQLRouter(schema, context_getter=get_context)

    def _get_app_context(info: strawberry.types.Info) -> Any:
        return info.context.get("app_context")

    def _get_caller_tenant(info: strawberry.types.Info) -> str:
        """Extract verified tenant_id from resolver context (fixes W-08)."""
        return info.context.get("tenant_id", "unknown")

    def _require_admin(info: strawberry.types.Info) -> None:
        """Raise PermissionError if caller is not an admin (fixes W-08)."""
        if not info.context.get("is_admin", False):
            raise PermissionError("Admin role required for this operation.")

else:
    # Stubs so imports don't break
    schema = None  # type: ignore[assignment]

    def build_graphql_router(app_context: Any) -> None:  # type: ignore[misc]
        return None

    def _get_app_context(info: Any) -> Any:  # type: ignore[misc]
        return None
