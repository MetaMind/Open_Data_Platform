"""Shared pytest fixtures for MetaMind tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_redis() -> MagicMock:
    client = MagicMock()
    client.get.return_value = None
    client.setex.return_value = True
    client.ping.return_value = True
    return client


@pytest.fixture
def mock_db_engine() -> MagicMock:
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    conn.execute.return_value.fetchone.return_value = None
    engine.connect.return_value.__enter__.return_value = conn
    engine.begin.return_value.__enter__.return_value = conn
    return engine


@pytest.fixture
def test_tenant_id() -> str:
    return "default"
