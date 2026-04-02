"""deploy_pipeline — Algorithm 2 from the ODEP design document."""

from __future__ import annotations

import re
from typing import Optional

from odep.config import OdepConfig
from odep.exceptions import OrchestratorConnectionError
from odep.factory import get_metadata_adapter, get_orchestrator_adapter
from odep.models import DatasetMetadata
from odep.sdk.pipeline import Pipeline


def deploy_pipeline(
    path: str,
    env: str,
    schedule: Optional[str] = None,
    config: Optional[OdepConfig] = None,
) -> str:
    """Deploy a pipeline definition to the configured orchestrator (Algorithm 2).

    Returns the job_id string from the orchestrator.
    Raises PipelineParseError on bad pipeline file, OrchestratorConnectionError if unreachable.
    """
    if config is None:
        config = OdepConfig()

    pipeline = Pipeline.from_file(path)
    assert pipeline.is_valid()

    if schedule is not None:
        pipeline.schedule = schedule

    job_def = pipeline.to_job_definition(env)

    orchestrator = get_orchestrator_adapter(config.orchestration.engine, config.orchestration)
    if not orchestrator.health_check():
        raise OrchestratorConnectionError(
            config.orchestration.airflow_url,
            "Run 'odep local up' to start the local stack",
        )

    job_id = orchestrator.deploy_job(job_def)

    metadata = get_metadata_adapter(config.metadata.engine, config.metadata)

    for sink in pipeline.sinks:
        urn = sink["urn"]
        m = re.match(r"urn:li:dataset:\(([^,]+),([^,]+),([^)]+)\)", urn)
        if m:
            platform, name, env_str = m.group(1), m.group(2), m.group(3)
        else:
            platform, name, env_str = "unknown", urn, env
        dataset = DatasetMetadata(
            **{"urn": urn, "name": name, "platform": platform, "env": env_str,
               "schema": [{"name": "unknown", "type": "unknown"}], "owner": "odep-deploy"}
        )
        metadata.register_dataset(dataset)

    lineage_edges = pipeline.extract_lineage_edges()
    metadata.create_lineage(lineage_edges)

    return job_id


__all__ = ["deploy_pipeline"]
