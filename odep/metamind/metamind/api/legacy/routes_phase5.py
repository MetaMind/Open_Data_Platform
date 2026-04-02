"""MetaMind API server with NL, rewrite, replay, and vector search endpoints.

Provides REST API for all Phase 5 features using a lightweight WSGI-compatible
handler pattern (no framework dependency required).
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from metamind.core.catalog.metadata import MetadataCatalog
from metamind.core.cost.model import CostModel
from metamind.core.execution.backends import BackendRegistry
from metamind.core.nl_interface.generator import (
    ConversationManager,
    NLFeedbackCollector,
    NLQueryGenerator,
)
from metamind.core.replay.recorder import (
    OptimizationSimulator,
    ReplayRecorder,
    WhatIfAPI,
)
from metamind.core.rewrite.analyzer import RewriteAnalyzer, RewriteSuggester
from metamind.core.types import Predicate
from metamind.core.vector.search import (
    VectorCostModel,
    VectorIndexManager,
    VectorSearchPlanner,
    VectorSearchRequest,
)

logger = logging.getLogger(__name__)


@dataclass
class APIResponse:
    """Standard API response wrapper."""

    status: int
    data: Any = None
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"status": self.status}
        if self.data is not None:
            result["data"] = self.data
        if self.error is not None:
            result["error"] = self.error
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)


class MetaMindAPIServer:
    """Central API server coordinating all Phase 5 features."""

    def __init__(
        self,
        catalog: MetadataCatalog,
        cost_model: Optional[CostModel] = None,
        backend_registry: Optional[BackendRegistry] = None,
        nl_generator: Optional[NLQueryGenerator] = None,
        db_engine: Any = None,
    ) -> None:
        self.catalog = catalog
        self.cost_model = cost_model or CostModel()
        self.backend_registry = backend_registry or BackendRegistry()
        self.nl_generator = nl_generator
        self.db_engine = db_engine

        self._sessions: dict[str, ConversationManager] = {}
        self._feedback = NLFeedbackCollector()

        vector_cost = VectorCostModel()
        self.vector_planner = VectorSearchPlanner(
            catalog, vector_cost, self.backend_registry
        )
        self.vector_index_mgr = VectorIndexManager(catalog, self.backend_registry)

        self.rewrite_analyzer = RewriteAnalyzer(catalog=catalog)
        self.rewrite_suggester = RewriteSuggester(self.rewrite_analyzer)

        self.simulator = OptimizationSimulator(
            catalog, self.cost_model
        )
        self.whatif = WhatIfAPI(self.simulator)

        self._routes: dict[str, Callable[..., APIResponse]] = {}
        self._register_routes()

    def _register_routes(self) -> None:
        """Register all API route handlers."""
        self._routes["POST /v1/nl/session"] = self._handle_nl_create_session
        self._routes["POST /v1/nl/session/query"] = self._handle_nl_query
        self._routes["POST /v1/nl/feedback"] = self._handle_nl_feedback
        self._routes["POST /v1/query/suggestions"] = self._handle_suggestions
        self._routes["POST /v1/replay/scenarios"] = self._handle_create_scenario
        self._routes["POST /v1/replay/scenarios/run"] = self._handle_run_scenario
        self._routes["GET /v1/replay/scenarios"] = self._handle_list_scenarios
        self._routes["GET /v1/replay/scenarios/result"] = self._handle_get_result
        self._routes["POST /v1/vector/search"] = self._handle_vector_search
        self._routes["POST /v1/vector/indexes"] = self._handle_create_vector_index

    def handle_request(
        self, method: str, path: str, body: dict[str, Any]
    ) -> APIResponse:
        """Route and handle an API request."""
        route_key = f"{method.upper()} {path}"

        for pattern, handler in self._routes.items():
            if self._match_route(pattern, route_key):
                try:
                    return handler(body)
                except Exception as exc:
                    logger.error("Handler error for %s: %s", route_key, exc)
                    return APIResponse(status=500, error=str(exc))

        return APIResponse(status=404, error=f"Route not found: {route_key}")

    @staticmethod
    def _match_route(pattern: str, route: str) -> bool:
        """Simple route matching."""
        p_parts = pattern.split("/")
        r_parts = route.split("/")
        if len(p_parts) != len(r_parts):
            return False
        for pp, rp in zip(p_parts, r_parts):
            if pp.startswith("{") and pp.endswith("}"):
                continue
            if pp != rp:
                return False
        return True

    # ────────── NL Interface Endpoints ──────────

    def _handle_nl_create_session(self, body: dict[str, Any]) -> APIResponse:
        """POST /v1/nl/session — Create a new NL conversation session."""
        tenant_id = body.get("tenant_id", "default")
        session = ConversationManager(max_turns=body.get("max_turns", 10))
        session_id = session.session_id
        self._sessions[session_id] = session

        logger.info("Created NL session %s for tenant %s", session_id, tenant_id)
        return APIResponse(status=201, data={"session_id": session_id})

    def _handle_nl_query(self, body: dict[str, Any]) -> APIResponse:
        """POST /v1/nl/session/{session_id}/query — Continue NL conversation."""
        session_id = body.get("session_id", "")
        nl_text = body.get("query", "")
        tenant_id = body.get("tenant_id", "default")

        if not nl_text:
            return APIResponse(status=400, error="query is required")

        session = self._sessions.get(session_id)
        if session is None:
            session = ConversationManager()
            self._sessions[session.session_id] = session
            session_id = session.session_id

        if self.nl_generator is None:
            return APIResponse(status=503, error="NL generator not configured")

        result = self.nl_generator.generate(
            nl_text=nl_text,
            tenant_id=tenant_id,
            conversation=session,
            table_hints=body.get("table_hints"),
        )

        return APIResponse(status=200, data={
            "session_id": session_id,
            "sql": result.sql,
            "confidence": result.confidence,
            "tables_used": result.tables_used,
            "was_validated": result.was_validated,
            "validation_error": result.validation_error,
        })

    def _handle_nl_feedback(self, body: dict[str, Any]) -> APIResponse:
        """POST /v1/nl/feedback — Record feedback on generated SQL."""
        tenant_id = body.get("tenant_id", "default")
        nl_text = body.get("nl_text", "")
        generated_sql = body.get("generated_sql", "")
        was_correct = body.get("was_correct", True)
        correction = body.get("correction")

        self._feedback.record_feedback(
            tenant_id, nl_text, generated_sql, was_correct, correction,
            self.db_engine,
        )

        return APIResponse(status=200, data={"recorded": True})

    # ────────── Rewrite Suggestions ──────────

    def _handle_suggestions(self, body: dict[str, Any]) -> APIResponse:
        """POST /v1/query/suggestions — Get rewrite suggestions for SQL."""
        sql = body.get("sql", "")
        tenant_id = body.get("tenant_id", "default")

        if not sql:
            return APIResponse(status=400, error="sql is required")

        suggestions = self.rewrite_suggester.suggest(None, sql, tenant_id)

        return APIResponse(status=200, data=[
            {
                "rule": s.rule_name,
                "description": s.description,
                "rewritten_sql": s.rewritten_sql,
                "estimated_improvement_pct": s.estimated_improvement_pct,
                "confidence": s.confidence,
                "explanation": s.explanation,
            }
            for s in suggestions
        ])

    # ────────── Replay/What-If ──────────

    def _handle_create_scenario(self, body: dict[str, Any]) -> APIResponse:
        """POST /v1/replay/scenarios — Create a what-if scenario."""
        tenant_id = body.get("tenant_id", "default")
        name = body.get("name", "Unnamed scenario")
        changes = body.get("changes", [])
        description = body.get("description", "")
        sample_size = body.get("query_sample_size", 1000)

        scenario = self.simulator.create_scenario(
            tenant_id, name, changes, description, sample_size
        )

        return APIResponse(status=201, data={
            "scenario_id": scenario.scenario_id,
            "name": scenario.name,
            "changes": scenario.hypothetical_changes,
        })

    def _handle_run_scenario(self, body: dict[str, Any]) -> APIResponse:
        """POST /v1/replay/scenarios/{scenario_id}/run — Execute simulation."""
        scenario_id = body.get("scenario_id", "")
        tenant_id = body.get("tenant_id", "default")

        scenarios = self.simulator.list_scenarios(tenant_id)
        scenario = next((s for s in scenarios if s.scenario_id == scenario_id), None)

        if scenario is None:
            return APIResponse(status=404, error=f"Scenario {scenario_id} not found")

        result = self.simulator.run_scenario(scenario, tenant_id, self.db_engine)

        return APIResponse(status=200, data={
            "scenario_id": result.scenario_id,
            "queries_replayed": result.queries_replayed,
            "original_total_cost": result.original_total_cost,
            "simulated_total_cost": result.simulated_total_cost,
            "cost_improvement_pct": result.cost_improvement_pct,
            "top_benefiting_queries": result.top_benefiting_queries,
            "recommendation": result.recommendation,
        })

    def _handle_list_scenarios(self, body: dict[str, Any]) -> APIResponse:
        """GET /v1/replay/scenarios — List all scenarios for tenant."""
        tenant_id = body.get("tenant_id", "default")
        scenarios = self.simulator.list_scenarios(tenant_id)

        return APIResponse(status=200, data=[
            {
                "scenario_id": s.scenario_id,
                "name": s.name,
                "description": s.description,
                "changes": s.hypothetical_changes,
                "created_at": s.created_at.isoformat(),
            }
            for s in scenarios
        ])

    def _handle_get_result(self, body: dict[str, Any]) -> APIResponse:
        """GET /v1/replay/scenarios/{scenario_id}/result — Get simulation results."""
        scenario_id = body.get("scenario_id", "")
        result = self.simulator.get_result(scenario_id)

        if result is None:
            return APIResponse(status=404, error="Result not found")

        return APIResponse(status=200, data={
            "scenario_id": result.scenario_id,
            "queries_replayed": result.queries_replayed,
            "cost_improvement_pct": result.cost_improvement_pct,
            "recommendation": result.recommendation,
            "per_query_results": result.per_query_results[:50],
        })

    # ────────── Vector Search ──────────

    def _handle_vector_search(self, body: dict[str, Any]) -> APIResponse:
        """POST /v1/vector/search — Execute a vector similarity search."""
        preds = [
            Predicate(column=p["column"], operator=p["operator"], value=p["value"])
            for p in body.get("filter_predicates", [])
        ]

        request = VectorSearchRequest(
            table=body.get("table", ""),
            embedding_column=body.get("embedding_column", ""),
            query_vector=body.get("query_vector", []),
            top_k=body.get("top_k", 10),
            distance_metric=body.get("distance_metric", "cosine"),
            filter_predicates=preds,
            tenant_id=body.get("tenant_id", "default"),
            backend_preference=body.get("backend_preference"),
        )

        result = self.vector_planner.execute(request)

        return APIResponse(status=200, data={
            "rows": result.rows,
            "distances": result.distances,
            "row_count": result.row_count,
            "duration_ms": result.duration_ms,
            "index_used": result.index_used,
            "backend_used": result.backend_used,
        })

    def _handle_create_vector_index(self, body: dict[str, Any]) -> APIResponse:
        """POST /v1/vector/indexes — Create a vector index."""
        tenant_id = body.get("tenant_id", "default")
        idx = self.vector_index_mgr.create_index(
            tenant_id=tenant_id,
            table=body.get("table", ""),
            column=body.get("column", ""),
            index_type=body.get("index_type", "HNSW"),
            metric=body.get("metric", "cosine"),
            params=body.get("params", {}),
            backend=body.get("backend", "pgvector"),
        )

        return APIResponse(status=201, data={
            "index_name": idx.index_name,
            "index_type": idx.index_type,
            "table": idx.table,
            "column": idx.column,
            "dimensions": idx.dimensions,
        })
