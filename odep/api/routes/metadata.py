"""Metadata API routes."""
from fastapi import APIRouter, Depends, HTTPException

from odep.api.auth import TokenData, get_current_user
from odep.config import OdepConfig
from odep.factory import get_metadata_adapter

router = APIRouter(prefix="/metadata", tags=["metadata"])


@router.get("/dataset/{urn}")
async def get_dataset(urn: str, current_user: TokenData = Depends(get_current_user)):
    config = OdepConfig()
    adapter = get_metadata_adapter(config.metadata.engine, config.metadata)
    dataset = adapter.get_dataset(urn)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset {urn!r} not found")
    return dataset.model_dump()


@router.get("/search")
async def search_catalog(q: str, current_user: TokenData = Depends(get_current_user)):
    config = OdepConfig()
    adapter = get_metadata_adapter(config.metadata.engine, config.metadata)
    results = adapter.search_catalog(q)
    return [r.model_dump() for r in results]


@router.delete("/dataset/{urn}")
async def delete_dataset(urn: str, current_user: TokenData = Depends(get_current_user)):
    config = OdepConfig()
    adapter = get_metadata_adapter(config.metadata.engine, config.metadata)
    deleted = adapter.delete_dataset(urn)
    return {"deleted": deleted, "urn": urn}
