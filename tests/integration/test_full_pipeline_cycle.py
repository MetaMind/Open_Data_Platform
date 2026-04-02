"""Integration test: full pipeline cycle.

Validates Requirements 9.3, 10.4, 2.1 using in-memory adapters (no Docker needed).
"""
from odep.adapters.duckdb.adapter import DuckDbAdapter
from odep.adapters.openmeta.adapter import OpenMetaAdapter
from odep.config import ExecutionConfig, MetadataConfig
from odep.models import DatasetMetadata, EngineType, JobConfig, LineageEdge


def test_full_pipeline_cycle():
    """Test: register datasets → create lineage → execute job → quality check."""
    metadata = OpenMetaAdapter(MetadataConfig())
    execution = DuckDbAdapter(ExecutionConfig())

    # 1. Register source dataset
    source = DatasetMetadata(
        **{
            "urn": "urn:li:dataset:(duckdb,raw.events,dev)",
            "name": "raw_events",
            "platform": "duckdb",
            "env": "dev",
            "schema": [{"name": "id", "type": "INTEGER"}, {"name": "event", "type": "VARCHAR"}],
            "owner": "test-engineer",
        }
    )
    urn = metadata.register_dataset(source)
    assert urn == "urn:li:dataset:(duckdb,raw.events,dev)"

    # 2. Register sink dataset
    sink = DatasetMetadata(
        **{
            "urn": "urn:li:dataset:(duckdb,analytics.fact_events,dev)",
            "name": "fact_events",
            "platform": "duckdb",
            "env": "dev",
            "schema": [{"name": "id", "type": "INTEGER"}],
            "owner": "test-engineer",
        }
    )
    metadata.register_dataset(sink)

    # 3. Create lineage
    edge = LineageEdge(
        source_urn="urn:li:dataset:(duckdb,raw.events,dev)",
        target_urn="urn:li:dataset:(duckdb,analytics.fact_events,dev)",
    )
    metadata.create_lineage([edge])

    # 4. Execute a SQL job
    job_config = JobConfig(engine=EngineType.SQL, code="SELECT 1 as id")
    handle = execution.submit(job_config, async_run=False)
    result = execution.wait_for_completion(handle)
    assert result.success is True

    # 5. Record quality check
    metadata.record_quality_check(
        "urn:li:dataset:(duckdb,analytics.fact_events,dev)",
        "row_count_check",
        True,
        {"row_count": result.records_processed},
    )

    # 6. Assert quality score updated
    score = metadata.get_quality_score("urn:li:dataset:(duckdb,analytics.fact_events,dev)")
    assert score == 100.0

    # 7. Assert lineage edges present
    upstream = metadata.get_upstream("urn:li:dataset:(duckdb,analytics.fact_events,dev)", depth=1)
    assert len(upstream) == 1
    assert upstream[0].source_urn == "urn:li:dataset:(duckdb,raw.events,dev)"

    # 8. Assert dataset registered in catalog
    registered = metadata.get_dataset("urn:li:dataset:(duckdb,raw.events,dev)")
    assert registered is not None
    assert registered.name == "raw_events"
