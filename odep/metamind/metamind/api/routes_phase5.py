"""Legacy Phase 5 routes placeholder.

This module is intentionally not mounted by the active FastAPI server.
The original implementation is archived at:
  metamind/api/legacy/routes_phase5.py
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)
logger.warning(
    "metamind.api.routes_phase5 is deprecated and quarantined; "
    "see metamind/api/legacy/routes_phase5.py"
)
