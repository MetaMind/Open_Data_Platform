"""Backend connector registry for multi-engine query routing."""
from __future__ import annotations

import abc
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class BackendConnector(abc.ABC):
    """Abstract base class for database backend connectors."""

    @abc.abstractmethod
    def engine_name(self) -> str:
        ...

    @abc.abstractmethod
    def execute_sql(self, sql: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        ...

    @abc.abstractmethod
    def capabilities(self) -> dict[str, bool]:
        ...


class BackendRegistry:
    """Registry of available backend connectors."""

    def __init__(self) -> None:
        self._backends: dict[str, BackendConnector] = {}

    def register(self, name: str, connector: BackendConnector) -> None:
        self._backends[name] = connector
        logger.info("Registered backend: %s", name)

    def get(self, name: str) -> Optional[BackendConnector]:
        return self._backends.get(name)

    def list_backends(self) -> list[str]:
        return list(self._backends.keys())

    def has_capability(self, name: str, capability: str) -> bool:
        backend = self._backends.get(name)
        if backend is None:
            return False
        return backend.capabilities().get(capability, False)

    def available(self, name: str) -> bool:
        return name in self._backends
