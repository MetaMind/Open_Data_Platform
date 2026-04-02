"""Legacy federation router placeholder.

This module has been quarantined because it depended on legacy bootstrap
contracts that are not part of the current AppContext-based runtime.

Original implementation archived at:
  metamind/api/legacy/federation_router.py
"""
from __future__ import annotations

import logging

from fastapi import APIRouter

logger = logging.getLogger(__name__)
router = APIRouter()

logger.warning(
    "metamind.api.federation_router is deprecated and quarantined; "
    "see metamind/api/legacy/federation_router.py"
)
