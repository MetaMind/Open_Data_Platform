"""Orchestration API routes."""
from fastapi import APIRouter, Depends

from odep.api.auth import TokenData, get_current_user
from odep.config import OdepConfig
from odep.factory import get_orchestrator_adapter

router = APIRouter(prefix="/orchestration", tags=["orchestration"])


@router.post("/deploy")
async def deploy_job(job_id: str, current_user: TokenData = Depends(get_current_user)):
    return {"status": "accepted", "job_id": job_id}


@router.post("/run/{job_id}")
async def trigger_run(job_id: str, current_user: TokenData = Depends(get_current_user)):
    config = OdepConfig()
    orchestrator = get_orchestrator_adapter(config.orchestration.engine, config.orchestration)
    run_id = orchestrator.trigger_job(job_id)
    return {"run_id": run_id}


@router.get("/status/{run_id}")
async def get_status(run_id: str, current_user: TokenData = Depends(get_current_user)):
    config = OdepConfig()
    orchestrator = get_orchestrator_adapter(config.orchestration.engine, config.orchestration)
    status = orchestrator.get_status(run_id)
    return {"run_id": run_id, "status": status.value}
