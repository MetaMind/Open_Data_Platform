"""Integration test: backfill ordering.

Validates Requirement 4.7 — backfill returns run_ids in chronological order.
Uses a mock AirflowAdapter to avoid needing a real Airflow instance.
"""
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from odep.adapters.airflow.adapter import AirflowAdapter
from odep.config import OrchestrationConfig


def test_backfill_returns_chronological_run_ids():
    """Trigger 7-day backfill and assert 7 run_ids returned in chronological order."""
    config = OrchestrationConfig()
    adapter = AirflowAdapter(config)

    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 8)  # 7 days

    # Mock the httpx client to simulate Airflow responses
    call_count = [0]

    def mock_post(url, **kwargs):
        call_count[0] += 1
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.is_success = True
        # Return a dag_run_id based on the execution_date in the payload
        execution_date = kwargs.get("json", {}).get("execution_date", f"2024-01-0{call_count[0]}")
        mock_resp.json.return_value = {"dag_run_id": f"run_{execution_date}"}
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    adapter._client.post = mock_post

    run_ids = adapter.backfill("test_job", start, end)

    # Assert 7 run_ids returned
    assert len(run_ids) == 7, f"Expected 7 run_ids, got {len(run_ids)}"

    # Assert chronological order (run_ids contain ISO dates which sort lexicographically)
    assert run_ids == sorted(run_ids), f"run_ids not in chronological order: {run_ids}"
