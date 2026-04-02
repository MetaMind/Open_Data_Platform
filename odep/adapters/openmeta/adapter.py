"""OpenMetaAdapter — in-memory implementation of MetadataService."""

from __future__ import annotations

import warnings
from collections import deque
from typing import Any, Dict, List, Optional, Set, Tuple

from odep.exceptions import SchemaDriftWarning
from odep.interfaces import MetadataService
from odep.models import DatasetMetadata, LineageEdge


def _compute_schema_diff(old_schema: List[Dict], new_schema: List[Dict]) -> Dict:
    old_names = {f["name"]: f for f in old_schema}
    new_names = {f["name"]: f for f in new_schema}
    added = [f for name, f in new_names.items() if name not in old_names]
    removed = [f for name, f in old_names.items() if name not in new_names]
    changed = [
        new_names[name]
        for name in old_names
        if name in new_names and old_names[name] != new_names[name]
    ]
    return {"added": added, "removed": removed, "changed": changed}


class OpenMetaAdapter:
    """In-memory MetadataService implementation simulating DataHub behavior."""

    def __init__(self, config: Any) -> None:
        self.config = config
        self._catalog: Dict[str, DatasetMetadata] = {}
        self._deleted: Set[str] = set()
        self._lineage: List[LineageEdge] = []
        self._quality_checks: Dict[str, List[Tuple[str, bool, Dict]]] = {}
        self._tags: Dict[str, Set[str]] = {}
        self._access_policies: Dict[str, Dict[str, Set[str]]] = {}

    # ------------------------------------------------------------------
    # Catalog
    # ------------------------------------------------------------------

    def register_dataset(self, dataset: DatasetMetadata) -> str:
        urn = dataset.urn
        if urn in self._catalog and urn not in self._deleted:
            existing = self._catalog[urn]
            diff = _compute_schema_diff(existing.schema_fields, dataset.schema_fields)
            if diff["added"] or diff["removed"] or diff["changed"]:
                warnings.warn(SchemaDriftWarning(urn, diff), SchemaDriftWarning, stacklevel=2)
        self._catalog[urn] = dataset
        self._deleted.discard(urn)
        return urn

    def get_dataset(self, urn: str) -> Optional[DatasetMetadata]:
        if urn in self._deleted:
            return None
        return self._catalog.get(urn)

    def search_catalog(
        self, query: str, filters: Optional[Dict[str, Any]] = None
    ) -> List[DatasetMetadata]:
        q = query.lower()
        results = []
        for urn, dataset in self._catalog.items():
            if urn in self._deleted:
                continue
            if (
                q in dataset.name.lower()
                or q in (dataset.description or "").lower()
                or any(q in tag.lower() for tag in dataset.tags)
            ):
                results.append(dataset)
        return results

    def delete_dataset(self, urn: str) -> bool:
        if urn in self._catalog and urn not in self._deleted:
            self._deleted.add(urn)
            return True
        return False

    # ------------------------------------------------------------------
    # Lineage
    # ------------------------------------------------------------------

    def create_lineage(self, edges: List[LineageEdge]) -> None:
        for edge in edges:
            if edge.source_urn == edge.target_urn:
                raise ValueError(f"Self-referential lineage edge rejected: {edge.source_urn!r}")
            self._lineage.append(edge)

    def get_upstream(self, urn: str, depth: int = 1) -> List[LineageEdge]:
        result: List[LineageEdge] = []
        seen: Set[Tuple[str, str]] = set()
        queue: deque = deque([(urn, 0)])
        visited_urns: Set[str] = set()
        while queue:
            current_urn, current_depth = queue.popleft()
            if current_urn in visited_urns:
                continue
            visited_urns.add(current_urn)
            next_depth = current_depth + 1
            for edge in self._lineage:
                if edge.target_urn == current_urn:
                    key = (edge.source_urn, edge.target_urn)
                    if key not in seen:
                        seen.add(key)
                        result.append(edge)
                    if next_depth < depth and edge.source_urn not in visited_urns:
                        queue.append((edge.source_urn, next_depth))
        return result

    def get_downstream(self, urn: str, depth: int = 1) -> List[LineageEdge]:
        result: List[LineageEdge] = []
        seen: Set[Tuple[str, str]] = set()
        queue: deque = deque([(urn, 0)])
        visited_urns: Set[str] = set()
        while queue:
            current_urn, current_depth = queue.popleft()
            if current_urn in visited_urns:
                continue
            visited_urns.add(current_urn)
            next_depth = current_depth + 1
            for edge in self._lineage:
                if edge.source_urn == current_urn:
                    key = (edge.source_urn, edge.target_urn)
                    if key not in seen:
                        seen.add(key)
                        result.append(edge)
                    if next_depth < depth and edge.target_urn not in visited_urns:
                        queue.append((edge.target_urn, next_depth))
        return result

    def get_full_upstream(self, urn: str, max_depth: int = 10) -> Dict[str, List[LineageEdge]]:
        max_depth = min(max_depth, 10)
        graph: Dict[str, List[LineageEdge]] = {}
        visited: Set[str] = set()
        queue: deque = deque([(urn, 0)])
        while queue:
            current_urn, depth = queue.popleft()
            if current_urn in visited or depth > max_depth:
                continue
            visited.add(current_urn)
            edges = self.get_upstream(current_urn, depth=1)
            graph[current_urn] = edges
            for edge in edges:
                if edge.source_urn not in visited:
                    queue.append((edge.source_urn, depth + 1))
        return graph

    # ------------------------------------------------------------------
    # Quality
    # ------------------------------------------------------------------

    def record_quality_check(
        self, urn: str, check_name: str, passed: bool, metrics: Dict[str, float]
    ) -> None:
        if urn not in self._quality_checks:
            self._quality_checks[urn] = []
        self._quality_checks[urn].append((check_name, passed, metrics))

    def get_quality_score(self, urn: str) -> float:
        checks = self._quality_checks.get(urn)
        if not checks:
            return 0.0
        passed_count = sum(1 for _, passed, _ in checks if passed)
        return (passed_count / len(checks)) * 100.0

    # ------------------------------------------------------------------
    # Governance
    # ------------------------------------------------------------------

    def apply_tag(self, urn: str, tag: str) -> None:
        if urn not in self._tags:
            self._tags[urn] = set()
        self._tags[urn].add(tag)
        if tag == "PII":
            self._apply_encryption_policy(urn)

    def check_access(self, user: str, urn: str, action: str) -> bool:
        if user == "admin":
            return True
        allowed = self._access_policies.get(urn, {}).get(action, set())
        return user in allowed

    def grant_access(self, urn: str, user: str, action: str) -> None:
        if urn not in self._access_policies:
            self._access_policies[urn] = {}
        if action not in self._access_policies[urn]:
            self._access_policies[urn][action] = set()
        self._access_policies[urn][action].add(user)

    def _apply_encryption_policy(self, urn: str) -> None:
        if urn not in self._tags:
            self._tags[urn] = set()
        self._tags[urn].add("encryption_policy_applied")
