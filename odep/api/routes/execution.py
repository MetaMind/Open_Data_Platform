"""Execution API routes."""
from fastapi import APIRouter, Depends

from odep.api.auth import TokenData, get_current_user
from odep.config import OdepConfig
from odep.factory import get_execution_adapter
from odep.models import JobConfig

router = APIRouter(prefix="/execution", tags=["execution"])


@router.post("/submit")
async def submit_job(job_config: JobConfig, current_user: TokenData = Depends(get_current_user)):
    config = OdepConfig()
    engine = get_execution_adapter(config.execution.default_engine, config.execution)
    handle = engine.submit(job_config)
    return {"job_handle": handle}


@router.get("/status/{job_handle}")
async def get_job_status(job_handle: str, current_user: TokenData = Depends(get_current_user)):
    config = OdepConfig()
    engine = get_execution_adapter(config.execution.default_engine, config.execution)
    return engine.get_status(job_handle)
